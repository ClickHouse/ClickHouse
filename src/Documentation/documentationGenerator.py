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
'merge']

text = ""

for name in names:
  text = ""
  try:
    with open("../../docs/en/sql-reference/table-functions/" + name + ".md") as fl:
      for line in fl.readlines():
        text += line
    with open("SimpleDocumentation" + name.title() + ".h", "w") as fl:
      fl.write("#pragma once\n")
      fl.write('\n')
      fl.write('#include <Documentation/IDocumentation.h>\n')
      fl.write('\n')
      fl.write('namespace DB\n')
      fl.write('{\n')
      fl.write('\n')
      fl.write('const char * doc = R"(\n')
      fl.write(text)
      fl.write('\n)";\n')
      fl.write('\n')
      fl.write('}\n')
  except Exception:
    print(name)
