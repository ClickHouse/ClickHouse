---
description: 'Эта таблица содержит предупреждения о работе сервера ClickHouse.'
keywords: [ 'системная таблица', 'предупреждения' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud />

<div id="description">
  ## Описание
</div>

Эта таблица содержит предупреждения о сервере ClickHouse.
Предупреждения одного и того же типа объединяются в одно.
Например, если число N присоединённых баз данных превышает настраиваемый порог T, вместо N отдельных записей отображается одна запись с текущим значением N.
Если текущее значение становится ниже порога, запись удаляется из таблицы.

Таблицу можно настроить с помощью следующих параметров:

* [max&#95;table&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
* [max&#95;database&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
* [max&#95;dictionary&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
* [max&#95;view&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
* [max&#95;part&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
* [max&#95;pending&#95;mutations&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)
* [max&#95;pending&#95;mutations&#95;execution&#95;time&#95;to&#95;warn](/ru/operations/server-configuration-parameters/settings#max_pending_mutations_execution_time_to_warn)
* [max&#95;named&#95;collection&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_named_collection_num_to_warn)
* [resource&#95;overload&#95;warnings](/ru/operations/settings/server-overload#resource-overload-warnings)

<div id="columns">
  ## Столбцы
</div>

* `message` ([String](../../sql-reference/data-types/string.md)) — Сообщение предупреждения.
* `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Строка формата, используемая для форматирования сообщения.

<div id="example">
  ## Пример
</div>

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```