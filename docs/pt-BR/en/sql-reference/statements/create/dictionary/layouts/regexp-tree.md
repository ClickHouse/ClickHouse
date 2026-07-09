---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: 'Layout de dicionário de árvore de expressões regulares'
sidebar_label: 'Regexp Tree'
sidebar_position: 12
description: 'Configure um dicionário de árvore de expressões regulares para consultas baseadas em padrões.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## Visão geral
</div>

O dicionário `regexp_tree` permite mapear chaves para valores com base em padrões hierárquicos de expressões regulares.
Ele é otimizado para buscas por correspondência de padrões (por exemplo, para classificar strings, como strings de user agent, com base na correspondência com padrões regex), em vez de correspondência exata de chaves.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="Uma introdução aos dicionários regexp_tree do ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## Use o dicionário de árvore de expressões regulares com a fonte YAMLRegExpTree
</div>

<CloudNotSupportedBadge />

No ClickHouse de código aberto, os dicionários de árvore de expressões regulares são definidos usando a fonte [`YAMLRegExpTree`](../sources/yamlregexptree.md), informando o caminho para um arquivo YAML que contém a árvore de expressões regulares.

```sql title="Query"
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

A fonte do dicionário [`YAMLRegExpTree`](../sources/yamlregexptree.md) representa a estrutura de uma árvore de expressões regulares. Por exemplo:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

Esta configuração consiste em uma lista de nós de uma árvore de expressões regulares. Cada nó tem a seguinte estrutura:

* **regexp**: a expressão regular do nó.
* **attributes**: uma lista de atributos de dicionário definidos pelo usuário. Neste exemplo, há dois atributos: `name` e `version`. O primeiro nó define ambos os atributos. O segundo nó define apenas o atributo `name`. O atributo `version` é fornecido pelos nós filhos do segundo nó.
  * O valor de um atributo pode conter **back references**, que se referem a grupos de captura da expressão regular correspondente. No exemplo, o valor do atributo `version` no primeiro nó consiste em uma back reference `\1` para o grupo de captura `(\d+[\.\d]*)` na expressão regular. Os números de back reference variam de 1 a 9 e são escritos como `$1` ou `\1` (para o número 1). A back reference é substituída pelo grupo de captura correspondente durante a execução da consulta.
* **child nodes**: uma lista de nós filhos de um nó da árvore de expressões regulares, cada um com seus próprios atributos e, potencialmente, nós filhos. A correspondência de strings é feita em profundidade primeiro. Se uma string corresponder a um nó regexp, o dicionário verifica se ela também corresponde aos nós filhos desse nó. Se isso acontecer, os atributos do nó correspondente mais profundo serão atribuídos. Os atributos de um nó filho substituem atributos de mesmo nome dos nós pai. O nome dos nós filhos em arquivos YAML pode ser arbitrário, por exemplo, `versions` no exemplo acima.

Dicionários de árvore de expressões regulares permitem acesso apenas por meio das funções `dictGet`, `dictGetOrDefault` e `dictGetAll`. Por exemplo:

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

Nesse caso, primeiro encontramos a correspondência da expressão regular `\d+/tclwebkit(?:\d+[\.\d]*)` no segundo nó da camada superior.
Em seguida, o dicionário continua procurando nos nós filhos e descobre que a string também corresponde a `3[12]/tclwebkit`.
Como resultado, o valor do atributo `name` é `Android` (definido na primeira camada) e o valor do atributo `version` é `12` (definido no nó filho).

Com um arquivo de configuração YAML sofisticado, você pode usar dicionários de árvore de expressões regulares como analisador de strings de user agent.
O ClickHouse oferece suporte ao [uap-core](https://github.com/ua-parser/uap-core) e você pode ver como usá-lo no teste funcional [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)

<div id="collecting-attribute-values">
  ### Coletando valores de atributo
</div>

Às vezes, é útil retornar valores de várias expressões regulares que tiveram correspondência, em vez de apenas o valor de um nó folha. Nesses casos, pode-se usar a função especializada [`dictGetAll`](/pt-BR/sql-reference/functions/ext-dict-functions.md#dictGetAll). Se um nó tiver um valor de atributo do tipo `T`, `dictGetAll` retornará um `Array(T)` contendo zero ou mais valores.

Por padrão, o número de correspondências retornadas por chave não tem limite. É possível passar um limite como quarto argumento opcional para `dictGetAll`. O array é preenchido em *ordem topológica*, o que significa que os nós filho vêm antes dos nós pai, e os nós irmãos seguem a ordem da fonte.

Exemplo:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql title="Query"
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

```text title="Response"
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="matching-modes">
  ### Modos de correspondência
</div>

O comportamento da correspondência de padrões pode ser modificado com determinadas configurações do Dicionário:

* `regexp_dict_flag_case_insensitive`: Usa correspondência sem diferenciar maiúsculas de minúsculas (o padrão é `false`). Pode ser sobrescrita em expressões individuais com `(?i)` e `(?-i)`.
* `regexp_dict_flag_dotall`: Permite que `.` corresponda a caracteres de nova linha (o padrão é `false`).

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## Use o dicionário de árvore de expressões regulares no ClickHouse Cloud
</div>

A fonte [`YAMLRegExpTree`](../sources/yamlregexptree.md) funciona no ClickHouse de código aberto, mas não no ClickHouse Cloud.
Para usar dicionários de árvore de expressões regulares no ClickHouse Cloud, primeiro crie localmente, no ClickHouse de código aberto, um dicionário de árvore de expressões regulares a partir de um arquivo YAML e, em seguida, exporte esse dicionário para um arquivo CSV usando a função de tabela `dictionary` e a cláusula [INTO OUTFILE](/pt-BR/sql-reference/statements/select/into-outfile.md).

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

O conteúdo do arquivo CSV é:

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

O esquema do arquivo de dump é:

* `id UInt64`: o id do nó RegexpTree.
* `parent_id UInt64`: o id do nó pai.
* `regexp String`: a string da expressão regular.
* `keys Array(String)`: os nomes dos atributos definidos pelo usuário.
* `values Array(String)`: os valores dos atributos definidos pelo usuário.

Para criar o dicionário no ClickHouse Cloud, primeiro crie uma tabela `regexp_dictionary_source_table` com a estrutura da tabela abaixo:

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

Em seguida, atualize o CSV local com

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

Consulte [Inserir arquivos locais](/pt-BR/integrations/data-ingestion/insert-local-files) para mais detalhes. Depois de inicializarmos a tabela de origem, podemos criar uma RegexpTree a partir da tabela de origem:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```