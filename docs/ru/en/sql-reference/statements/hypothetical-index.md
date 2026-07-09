---
description: 'Документация по гипотетическим индексам («что, если»)'
sidebar_label: 'ГИПОТЕТИЧЕСКИЙ ИНДЕКС'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: 'Гипотетические индексы'
doc_type: 'reference'
---

<div id="hypothetical-indexes">
  # Гипотетические индексы
</div>

Гипотетические индексы — это виртуальные индексы пропуска данных, ограниченные рамками сеанса, которые можно подключить к таблице семейства `MergeTree`, не создавая и не сохраняя их физически. Они существуют только в пределах текущего сеанса и используются [`EXPLAIN WHATIF`](/ru/sql-reference/statements/explain#explain-whatif) для оценки того, как реальный индекс пропуска данных повлияет на запрос — обычно это коэффициент пропуска (доля меток, которые можно было бы пропустить) и приблизительная стоимость в метках и байтах.

Используйте гипотетические индексы, чтобы оценить потенциальные индексы до того, как нести затраты на их материализацию на диске.

<div id="create-hypothetical-index">
  ## CREATE HYPOTHETICAL INDEX
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

Синтаксис повторяет `ALTER TABLE ... ADD INDEX`, но индекс не создаётся и не записывается — в текущем сеансе сохраняется только описание индекса.

* `name` — имя индекса; в этом сеансе оно должно быть уникальным в пределах `(database, table)`.
* `expression` — столбец или выражение, по которому строится индекс.
* `TYPE type` — `minmax`, `set(N)`, `bloom_filter(p)`, `ngrambf_v1(...)`, `tokenbf_v1(...)`. `text` и `vector_similarity` не поддерживаются и отклоняются во время `CREATE`, потому что проверка для их реального `ALTER TABLE ... ADD INDEX` зависит от настроек на уровне таблицы, которые хранилище, доступное только в рамках сеанса, не может воспроизвести.
* `GRANULARITY value` — число гранул данных на одну гранулу индекса. По умолчанию — 1.

Целевая таблица должна быть таблицей семейства `MergeTree` в базе данных `Atomic` (то есть иметь UUID). Таблицы без UUID — например, в устаревшей базе данных `Ordinary` или `MergeTree` со старым синтаксисом — отклоняются, потому что хранилище сеанса привязывает гипотетические индексы к UUID таблицы.

**Пример**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## Оценка гипотетического индекса с помощью EXPLAIN WHATIF
</div>

Само по себе определение гипотетического индекса ничего не даёт — чтобы понять, как он повлияет на запрос, выполните [`EXPLAIN WHATIF`](/ru/sql-reference/statements/explain#explain-whatif) для типичного `SELECT`. Оценщик показывает применимость каждого индекса-кандидата, количество меток, которые он будет читать, итоговый коэффициент пропуска и то, как была получена оценка (`empirical`, `statistical` или `applicability_only`).

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

Результат:

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` — это оценка, основанная на среднем размере строки таблицы, поэтому точное значение зависит от способа хранения и сжатия.

Чтобы пропустить эмпирическое сканирование в памяти и вместо этого выполнять оценку на основе [статистики столбцов](/ru/engines/table-engines/mergetree-family/mergetree#column-statistics), сначала задайте её для соответствующих столбцов (по умолчанию она отключена), дождитесь завершения мутации materialize, а затем отключите эмпирический способ оценки:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

Полную схему вывода и настройки см. на справочной странице [`EXPLAIN WHATIF`](/ru/sql-reference/statements/explain#explain-whatif).

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

Удаляет гипотетический индекс из текущего сеанса.

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

Удаляет все гипотетические индексы, определённые в текущем сеансе, независимо от таблицы.

<div id="scope-and-lifetime">
  ## Область действия и время жизни
</div>

* Гипотетические индексы существуют только в **текущем сеансе** — они не видны другим сеансам и удаляются после его завершения.
* Создание или удаление такого индекса не приводит к построению реального индекса и никак не влияет на обычные запросы к таблице. Однако эмпирический `EXPLAIN WHATIF` читает данные таблицы, чтобы построить в памяти предполагаемый индекс, и это сканирование учитывается в лимитах чтения и квотах сеанса.
* Просмотреть гипотетические индексы текущего сеанса можно через [`system.hypothetical_indexes`](/ru/operations/system-tables/hypothetical_indexes).

<div id="limitations">
  ## Ограничения
</div>

Кандидаты `text` и `vector_similarity` отклоняются на этапе `CREATE HYPOTHETICAL INDEX`, поскольку их фактическая проверка зависит от настроек на уровне таблицы, которые хранилище, доступное только в рамках сеанса, не может воспроизвести.

`EXPLAIN WHATIF` возвращает `status: not_applicable` для запросов с `FINAL` (отсев индексом пропуска данных взаимодействует с `PrimaryKeyExpand`) и ошибку `NOT_IMPLEMENTED`, если запрос обслуживается из проекции (индекс родительской таблицы не материализуется в частях проекции).

Эмпирический `skip_ratio` — это **верхняя граница**: он учитывает каждую оставшуюся гранулу независимо и не моделирует объединение промежутков seek-gap (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`), а также сочетание кандидата с существующим индексом пропуска данных при дизъюнктивном предикате (`OR`). Поэтому реальный материализованный индекс может читать немного больше данных или, наоборот, выполнять отсев в случаях, которые эта оценка не отражает.

<div id="required-privileges">
  ## Необходимые привилегии
</div>

`CREATE HYPOTHETICAL INDEX` требует `SELECT` на столбцы, на которые ссылается выражение индекса; `SELECT` на уровне столбца (например, `GRANT SELECT(b)`) достаточно, поскольку при эмпирическом `EXPLAIN WHATIF` считываются эти столбцы.

`DROP HYPOTHETICAL INDEX` и `DROP ALL HYPOTHETICAL INDEXES` не требуют дополнительных привилегий; они лишь удаляют записи из локального для сеанса хранилища.

<div id="see-also">
  ## См. также
</div>

* [`EXPLAIN WHATIF`](/ru/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/ru/operations/system-tables/hypothetical_indexes)
* [Индексы пропуска данных](/ru/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)