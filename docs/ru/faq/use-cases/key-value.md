---
title: Можно ли использовать ClickHouse как хранилище вида "ключ-значение"?
toc_hidden: true
toc_priority: 101
---

# Можно ли использовать ClickHouse как хранилище вида "ключ-значение"? {#can-i-use-clickhouse-as-a-key-value-storage}

Если отвечать коротко, то **"нет"**. Причем рабочий поток, относящийся к виду "ключ-значение", занимает одну из верхних позиций в списке, когда категорически **не стоит**{.text-danger} пользоваться ClickHouse. К тому же, это ведь [OLAP](../../faq/general/olap.md)-система, в то время как есть много систем хранения вида "ключ-значение".

Тем не менее, могут быть такие ситуауии, когда все еще стоит пользоваться ClickHouse для запросов "ключ-значение". Обычно

However, there might be situations where it still makes sense to use ClickHouse for key-value-like queries. Usually, it’s some low-budget products where the main workload is analytical in nature and fits ClickHouse well, but there’s also some secondary process that needs a key-value pattern with not so high request throughput and without strict latency requirements. If you had an unlimited budget, you would have installed a secondary key-value database for thus secondary workload, but in reality, there’s an additional cost of maintaining one more storage system (monitoring, backups, etc.) which might be desirable to avoid.

If you decide to go against recommendations and run some key-value-like queries against ClickHouse, here’re some tips:

-   The key reason why point queries are expensive in ClickHouse is its sparse primary index of main [MergeTree table engine family](../../engines/table-engines/mergetree-family/mergetree.md). This index can’t point to each specific row of data, instead, it points to each N-th and the system has to scan from the neighboring N-th row to the desired one, reading excessive data along the way. In a key-value scenario, it might be useful to reduce the value of N with the `index_granularity` setting.
-   ClickHouse keeps each column in a separate set of files, so to assemble one complete row it needs to go through each of those files. Their count increases linearly with the number of columns, so in the key-value scenario, it might be worth to avoid using many columns and put all your payload in a single `String` column encoded in some serialization format like JSON, Protobuf or whatever makes sense.
-   There’s an alternative approach that uses [Join](../../engines/table-engines/special/join.md) table engine instead of normal `MergeTree` tables and [joinGet](../../sql-reference/functions/other-functions.md#joinget) function to retrieve the data. It can provide better query performance but might have some usability and reliability issues. Here’s an [usage example](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00800_versatile_storage_join.sql#L49-L51).
