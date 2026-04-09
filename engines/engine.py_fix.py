import sys

path = '/home/ubuntu/vision2030_project/engines/engine.py'
with open(path, 'r') as f:
    lines = f.readlines()

# البحث عن السطر التالف
start_idx = -1
end_idx = -1
for i, line in enumerate(lines):
    if 'cands.appe            cands.append({' in line:
        start_idx = i
    if '})": img_u,' in line:
        end_idx = i

if start_idx != -1 and end_idx != -1:
    new_lines = [
        '            cands.append({\n',
        '                "name": name, "score": score,\n',
        '                "price": self.prices[idx], "product_id": self.ids[idx],\n',
        '                "brand": c_br, "size": c_sz, "type": c_tp, "gender": c_gd,\n',
        '                "image_url": img_u, "product_url": url_u,\n',
        '                "thumb": img_u,\n',
        '                "raw_description": self.raw_descriptions[idx] if idx < len(self.raw_descriptions) else "",\n',
        '            })\n'
    ]
    lines[start_idx:end_idx+2] = new_lines
    with open(path, 'w') as f:
        f.writelines(lines)
    print("Fixed successfully")
else:
    print(f"Could not find markers: start={start_idx}, end={end_idx}")
