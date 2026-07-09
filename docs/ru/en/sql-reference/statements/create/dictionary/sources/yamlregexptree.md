---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'Источник словаря YAMLRegExpTree'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'Настройте YAML-файл в качестве источника для словарей на основе дерева регулярных выражений.'
doc_type: 'справочник'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

Источник `YAMLRegExpTree` загружает дерево регулярных выражений из YAML-файла на локальной файловой системе.
Он предназначен исключительно для использования со [структурой словаря `regexp_tree`](../layouts/regexp-tree.md)
и предоставляет иерархические сопоставления регулярных выражений с атрибутами для поиска по шаблонам, например при разборе user agent.

:::note
Источник `YAMLRegExpTree` доступен только в ClickHouse Open Source.
В ClickHouse Cloud вместо этого экспортируйте словарь в CSV и загрузите его через [источник на основе таблицы ClickHouse](./clickhouse.md).
Подробности см. в разделе [Использование словарей regexp&#95;tree в ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud).
:::

<div id="configuration">
  ## Конфигурация
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

Поля настройки:

| Setting | Description                                                                                                                                   |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `PATH`  | Абсолютный путь к YAML-файлу, содержащему дерево регулярных выражений. При создании через DDL файл должен находиться в каталоге `user_files`. |

<div id="yaml-file-structure">
  ## Структура YAML-файла
</div>

YAML-файл содержит список узлов дерева регулярных выражений. Каждый узел может иметь атрибуты и дочерние узлы, образуя иерархию:

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

Каждый узел имеет следующую структуру:

* **`regexp`**: Регулярное выражение для этого узла.
* **attributes**: Пользовательские атрибуты словаря (например, `name`, `version`). Значения атрибутов могут содержать **обратные ссылки** на группы захвата в регулярном выражении, записанные как `\1` или `$1` (числа от 1 до 9). Во время выполнения запроса они заменяются соответствующей совпавшей группой захвата.
* **child nodes**: Список дочерних узлов, у каждого из которых есть собственные атрибуты и, при необходимости, дополнительные дочерние узлы. Имя списка дочерних узлов произвольно (например, `versions` выше). Сопоставление строк выполняется в глубину: если строка соответствует узлу, проверяются и его дочерние узлы. Атрибуты самого глубокого совпавшего узла имеют приоритет и переопределяют одноимённые атрибуты родительского узла.

<div id="related-pages">
  ## Связанные страницы
</div>

* [структура словаря regexp&#95;tree](../layouts/regexp-tree.md) — настройка структуры, примеры запросов и режимы сопоставления
* [dictGet](/ru/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/ru/sql-reference/functions/ext-dict-functions#dictGetAll) — функции для запросов к словарям дерева регулярных выражений