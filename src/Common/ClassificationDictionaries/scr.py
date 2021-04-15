file = open("total.txt")
new_f = open("programming_freq2.txt", "w")
for i in file:
    a = i.split()
    if len(a) == 1:
        new_f.write("// " + i)
    elif len(a) > 0 and len(a[0]) >= 2 and a[0][0] == '/' and a[0][1] == '/':
        continue
    else:
        new_f.write(i)
file.close()
new_f.close()
