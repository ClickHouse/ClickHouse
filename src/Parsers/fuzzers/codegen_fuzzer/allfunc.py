import re

keywords = set()
functions = set()

# Use regex to identify keywords and functions
keyword_pattern = re.compile(r'\b(SELECT|ORDER BY|GROUP BY|WHERE|LIMIT|JOIN|ARRAY JOIN|ON|USING|INTO OUTFILE)\b', re.IGNORECASE)
function_pattern = re.compile(r'\b[A-Z][a-zA-Z0-9_]*\b')

# Read the all.dict file
with open("all.dict", "r") as f:
    content = f.read()
    keywords.update(keyword_pattern.findall(content))
    functions.update(function_pattern.findall(content))

# Save keywords and functions
with open('keywords.txt', 'w') as f:
    for keyword in sorted(keywords):
        f.write(f"{keyword}\n")

with open('functions.txt', 'w') as f:
    for function in sorted(functions):
        f.write(f"{function}\n")

# Combine every keyword with every function and up to 3 arguments
combinations = []
for keyword in keywords:
    for function in functions:
        combinations.append(f"\"{keyword} \" $1 \" {function} \" $2 \"{function} \" $3 ;")  # No arguments


# Save combinations to file
with open('combinations.txt', 'w') as f:
    for combination in combinations:
        f.write(f"{combination}\n")
