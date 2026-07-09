---
description: 'Создает временную таблицу указанной структуры с движком таблицы Null.
  Функция используется для удобства написания тестов и демонстраций.'
sidebar_label: 'функция null'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'reference'
---

Создает временную таблицу указанной структуры с движком таблицы [Null](../../engines/table-engines/special/null.md). Согласно свойствам движка `Null`, данные таблицы игнорируются, а сама таблица удаляется сразу после выполнения запроса. Функция используется для удобства написания тестов и демонстраций.

<div id="syntax">
  ## Синтаксис
</div>

```sql
null('structure')
```

<div id="argument">
  ## Аргумент
</div>

* `structure` — Список столбцов и их типов. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Возвращаемое значение
</div>

Временная таблица с движком `Null` и указанной структурой.

<div id="example">
  ## Пример
</div>

Запрос с функцией `null`:

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

можно заменить тремя запросами:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## См. также
</div>

* [Движок таблицы Null](../../engines/table-engines/special/null.md)