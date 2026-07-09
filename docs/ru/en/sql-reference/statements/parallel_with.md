---
description: 'Документация по конструкции PARALLEL WITH'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'Конструкция PARALLEL WITH'
doc_type: 'reference'
---

Позволяет выполнять несколько команд параллельно.

<div id="syntax">
  ## Синтаксис
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

Выполняет команды `statement1`, `statement2`, `statement3`, ... параллельно. Вывод этих команд отбрасывается.

Во многих случаях параллельное выполнение команд может быть быстрее, чем их обычный последовательный запуск. Например, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` скорее всего выполнится быстрее, чем `statement1; statement2; statement3`.

<div id="examples">
  ## Примеры
</div>

Создает две таблицы одновременно:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

Удаляет две таблицы одновременно:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## Настройки
</div>

Параметр [max&#95;threads](../../operations/settings/settings.md#max_threads) определяет, сколько потоков создаётся.

<div id="comparison-with-union">
  ## Сравнение с UNION
</div>

Конструкция `PARALLEL WITH` немного похожа на [UNION](select/union.md), который тоже выполняет свои операнды параллельно. Однако есть несколько отличий:

* `PARALLEL WITH` не возвращает результаты выполнения своих операндов и может лишь повторно сгенерировать исключение, если оно в них возникло;
* `PARALLEL WITH` не требует, чтобы его операнды имели одинаковый набор результирующих столбцов;
* `PARALLEL WITH` может выполнять любые команды (а не только `SELECT`).