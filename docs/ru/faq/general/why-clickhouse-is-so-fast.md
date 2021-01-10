---
title: Why ClickHouse is so fast?
toc_hidden: true
toc_priority: 8
---

#  Почему ClickHouse так быстро работает? {#why-clickhouse-is-so-fast}

Изначально он был разработан так, чтобы быть быстрым. Производительность выполнения запросов всегда была самой важной в процессе разработки, но и другие важные характеристики, как например, комфорт пользователя, масштабируемость и защищенность также принимались во внимание, так чтобы ClickHouse стал системой с серьезной производительностью. 

Сначала ClickHouse был построен как прототип для выполнения одной вещи на отлично: фильтровать и агрегировать данные так быстро, насколько это возможно. Это именно то, что необходимо, чтобы создать типичный аналитический ответ и то, что происходит по стандартном запросу [GROUP BY](../../sql-reference/statements/select/group-by.md). Команда ClickHouse приняла несколько высокоуровневых решения, которые все вместе сделали достижимой следующую цель: 

Колоночое хранилище
:   Исходные данные часто содержат сотни или даже тысячи столбцов, в то время как для отчета нужно только несколько из них. Система не должна читать ненужные столбцы, иначе самые дорогостоящие операции прочтения с диска будут производиться зря. 

Индексы
:   ClickHouse хранит в памяти структуры данных, а это позволяет читать не только используемые столбцы, но только необходимые диапазоны строк для этих столбцов.

Сжатие данных
:   Хранение различных значений одной и той же колонки вместе часто ведет к лучшим пропорциям сжатия (по сравнению со стандартными ориентированными на строки системами). Причина тому — в реальности столбец данных часто имеет такие же или не слишком различные значения для соседних строк. А в дополнение к универсальному сжатию ClickHouse поддерживает [специализированные кодеки](../../sql-reference/statements/create/table.md#create-query-specialized-codecs), которые могут ужать данные еще больше. 

Vectorized query execution
:   ClickHouse не только хранит данные в столбцах, но также обрабатывает их. А это приводит к улучшению использования кеша процессора и позволяет использовать инструкции [SIMD](https://en.wikipedia.org/wiki/SIMD).

Масштабируемость
:   ClickHouse can leverage all available CPU cores and disks to execute even a single query. Not only on a single server but all CPU cores and disks of a cluster as well.

But many other database management systems use similar techniques. What really makes ClickHouse stand out is **attention to low-level details**. Most programming languages provide implementations for most common algorithms and data structures, but they tend to be too generic to be effective. Every task can be considered as a landscape with various characteristics, instead of just throwing in random implementation. For example, if you need a hash table, here are some key questions to consider:

-   Which hash function to choose?
-   Collision resolution algorithm: [open addressing](https://en.wikipedia.org/wiki/Open_addressing) vs [chaining](https://en.wikipedia.org/wiki/Hash_table#Separate_chaining)?
-   Memory layout: one array for keys and values or separate arrays? Will it store small or large values?
-   Fill factor: when and how to resize? How to move values around on resize?
-   Will values be removed and which algorithm will work better if they will?
-   Will we need fast probing with bitmaps, inline placement of string keys, support for non-movable values, prefetch, and batching?

Hash table is a key data structure for `GROUP BY` implementation and ClickHouse automatically chooses one of [30+ variations](https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Aggregator.h) for each specific query.

The same goes for algorithms, for example, in sorting you might consider:

-   What will be sorted: an array of numbers, tuples, strings, or structures?
-   Is all data available completely in RAM?
-   Do we need a stable sort?
-   Do we need a full sort? Maybe partial sort or n-th element will suffice?
-   How to implement comparisons?
-   Are we sorting data that has already been partially sorted?

Algorithms that they rely on characteristics of data they are working with can often do better than their generic counterparts. If it is not really known in advance, the system can try various implementations and choose the one that works best in runtime. For example, see an [article on how LZ4 decompression is implemented in ClickHouse](https://habr.com/en/company/yandex/blog/457612/).

Last but not least, the ClickHouse team always monitors the Internet on people claiming that they came up with the best implementation, algorithm, or data structure to do something and tries it out. Those claims mostly appear to be false, but from time to time you’ll indeed find a gem.

!!! info "Tips for building your own high-performance software"


    -   Keep in mind low-level details when designing your system.
    -   Design based on hardware capabilities.
    -   Choose data structures and abstractions based on the needs of the task.
    -   Provide specializations for special cases.
    -   Try new, “best” algorithms, that you read about yesterday.
    -   Choose an algorithm in runtime based on statistics.
    -   Benchmark on real datasets.
    -   Test for performance regressions in CI.
    -   Measure and observe everything.
