---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: 'Структура словаря на основе дерева регулярных выражений'
sidebar_label: 'Regexp Tree'
sidebar_position: 12
description: 'Настройте словарь на основе дерева регулярных выражений для поиска по шаблонам.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## Обзор
</div>

Словарь `regexp_tree` позволяет сопоставлять ключи со значениями на основе иерархических шаблонов регулярных выражений.
Он оптимизирован для поиска по совпадению с шаблоном (например, для классификации строк, таких как строки user agent, по совпадению с шаблонами регулярных выражений), а не для точного сопоставления ключей.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="Введение в словари деревьев regex в ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## Использование словаря на основе дерева регулярных выражений с источником YAMLRegExpTree
</div>

<CloudNotSupportedBadge />

Словари на основе дерева регулярных выражений в ClickHouse с открытым исходным кодом определяются с помощью источника [`YAMLRegExpTree`](../sources/yamlregexptree.md), которому передаётся путь к YAML-файлу, содержащему дерево регулярных выражений.

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

Источник словаря [`YAMLRegExpTree`](../sources/yamlregexptree.md) задаёт структуру дерева регулярных выражений. Например:

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

Эта конфигурация состоит из списка узлов дерева регулярных выражений. Каждый узел имеет следующую структуру:

* **regexp**: регулярное выражение узла.
* **attributes**: список определяемых пользователем атрибутов словаря. В этом примере есть два атрибута: `name` и `version`. Первый узел определяет оба атрибута. Второй узел определяет только атрибут `name`. Атрибут `version` задаётся дочерними узлами второго узла.
  * Значение атрибута может содержать **обратные ссылки**, указывающие на группы захвата в совпавшем регулярном выражении. В примере значение атрибута `version` в первом узле состоит из обратной ссылки `\1` на группу захвата `(\d+[\.\d]*)` в регулярном выражении. Номера обратных ссылок находятся в диапазоне от 1 до 9 и записываются как `$1` или `\1` (для номера 1). Во время выполнения запроса обратная ссылка заменяется соответствующей совпавшей группой захвата.
* **child nodes**: список дочерних узлов узла дерева регулярных выражений, каждый из которых имеет собственные атрибуты и (потенциально) дочерние узлы. Сопоставление строк выполняется обходом в глубину. Если строка соответствует узлу regexp, словарь проверяет, соответствует ли она также дочерним узлам этого узла. Если да, присваиваются атрибуты самого глубокого совпавшего узла. Атрибуты дочернего узла перезаписывают одноимённые атрибуты родительских узлов. Имена дочерних узлов в YAML-файлах могут быть произвольными, например `versions` в приведённом выше примере.

Словарь дерева регулярных выражений допускает доступ только с помощью функций `dictGet`, `dictGetOrDefault` и `dictGetAll`. Например:

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

В этом случае мы сначала сопоставляем строку с регулярным выражением `\d+/tclwebkit(?:\d+[\.\d]*)` во втором узле верхнего слоя.
Затем словарь продолжает поиск в дочерних узлах и обнаруживает, что строка также соответствует `3[12]/tclwebkit`.
В результате значение атрибута `name` — `Android` (определено в первом слое), а значение атрибута `version` — `12` (определено в дочернем узле).

Используя продвинутый файл конфигурации YAML, вы можете применять словарь дерева регулярных выражений как парсер строки user agent.
ClickHouse поддерживает [uap-core](https://github.com/ua-parser/uap-core), а посмотреть, как его использовать, можно в функциональном тесте [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)

<div id="collecting-attribute-values">
  ### Получение значений атрибутов
</div>

Иногда полезно возвращать значения из нескольких совпавших регулярных выражений, а не только значение листового узла. В таких случаях можно использовать специализированную функцию [`dictGetAll`](/ru/sql-reference/functions/ext-dict-functions.md#dictGetAll). Если у узла есть значение атрибута типа `T`, `dictGetAll` вернёт `Array(T)`, содержащий ноль или более значений.

По умолчанию число совпадений, возвращаемых для каждого ключа, не ограничено. Ограничение можно передать в `dictGetAll` в виде необязательного четвёртого аргумента. Массив заполняется в *топологическом порядке*: дочерние узлы идут перед родительскими, а одноуровневые узлы следуют порядку в источнике.

Пример:

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
  ### Режимы сопоставления
</div>

Поведение сопоставления с шаблоном можно изменять с помощью определённых настроек словаря:

* `regexp_dict_flag_case_insensitive`: использовать регистронезависимое сопоставление (по умолчанию — `false`). Эту настройку можно переопределить в отдельных выражениях с помощью `(?i)` и `(?-i)`.
* `regexp_dict_flag_dotall`: разрешить символу `.` сопоставляться с символами новой строки (по умолчанию — `false`).

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## Использование словаря на основе дерева регулярных выражений в ClickHouse Cloud
</div>

Источник [`YAMLRegExpTree`](../sources/yamlregexptree.md) работает в ClickHouse с открытым исходным кодом, но не поддерживается в ClickHouse Cloud.
Чтобы использовать словари на основе дерева регулярных выражений в ClickHouse Cloud, сначала создайте локально в ClickHouse с открытым исходным кодом словарь на основе дерева регулярных выражений из YAML-файла, а затем выгрузите его в CSV-файл с помощью табличной функции `dictionary` и [предложения INTO OUTFILE](/ru/sql-reference/statements/select/into-outfile.md).

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

Содержимое CSV-файла:

```text
1,0,"Linux/(\\d+[\\.\\d]*).+tlinux","['version','name']","['\\\\1','TencentOS']"
2,0,"(\\d+)/tclwebkit(\\d+[\\.\\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

Схема файла дампа:

* `id UInt64`: идентификатор узла RegexpTree.
* `parent_id UInt64`: идентификатор родительского узла.
* `regexp String`: строка регулярного выражения.
* `keys Array(String)`: имена пользовательских атрибутов.
* `values Array(String)`: значения пользовательских атрибутов.

Чтобы создать словарь в ClickHouse Cloud, сначала создайте таблицу `regexp_dictionary_source_table` со следующей структурой:

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

Затем обновите локальный CSV следующим образом

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

Подробнее см. в разделе [Вставка локальных файлов](/ru/integrations/data-ingestion/insert-local-files). После инициализации исходной таблицы мы можем создать RegexpTree, используя таблицу в качестве источника:

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