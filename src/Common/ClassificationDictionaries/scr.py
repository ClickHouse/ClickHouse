f = open("programming_freq.txt")
for i in f:
    a = i.split()
    if len(a) == 2 and a[0] == "//":
        print(a[1])
f.close()
