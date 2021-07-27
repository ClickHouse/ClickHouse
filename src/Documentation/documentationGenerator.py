import os

names = ['dictionary',
'postgresql',
'mysql',
'numbers',
'view',
'numbers',
'hdfs',
'cosn',
'odbc',
'values',
's3Cluster',
'generateRandom',
'remote',
'input',
's3',
'url',
'remoteSecure',
'sqlite',
'zeros-mt',
'jdbc',
'zeros',
'clusterAllReplicas',
'cluster',
'file',
'merge',
'null']

text = ""
directory = "../TableFunctions"
files = filter(lambda x: x.endswith(".h"), os.listdir(directory))
for name in names:
  text = ""
  try:
    with open("../../docs/en/sql-reference/table-functions/" + name + ".md") as fl:
      i = 0;
      for line in fl.readlines():
        if (i < 7):
          i += 1
          continue
        text += line

    for file in files:
      if (file.lower() == "tablefunction" + name + ".h"):
        all_text = []
        with open(directory + '/' + file, 'r') as fl:
          all_text = fl.readlines();

        with open(directory + '/' + file, "w") as fl:
          j = 0
          while (j < len(all_text) - 1):
            print(all_text[j])
            fl.write(all_text[j])
            
            j += 1
            if (j == 2):
              fl.write("#include <Documentation/SimpleDocumentation.h>\n")
            
          fl.write('namespace ' +  name.title() + "Doc" + '\n')
          fl.write('{\n')
          fl.write('const char * doc = R"(\n')
          fl.write(text)
          fl.write('\n)";\n')
          fl.write('\n')
          fl.write('}\n')
          fl.write('\n')
          fl.write('}\n')
        print("Success", name)
        break
  except Exception:
    print("Not", name)