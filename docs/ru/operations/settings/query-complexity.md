# Ограничения на сложность запроса {#restrictions-on-query-complexity}

Ограничения на сложность запроса - часть настроек.
Используются, чтобы обеспечить более безопасное исполнение запросов из пользовательского интерфейса.
Почти все ограничения действуют только на SELECT-ы.
При распределённой обработке запроса, ограничения действуют на каждом сервере по отдельности.

Ограничения проверяются на каждый блок обработанных данных, а не на каждую строку. В связи с этим, ограничения могут быть превышены на размер блока.

Ограничения вида «максимальное количество чего-нибудь» могут принимать значение 0, которое обозначает «не ограничено».
Для большинства ограничений также присутствует настройка вида overflow\_mode - что делать, когда ограничение превышено.
Оно может принимать одно из двух значений: `throw` или `break`; а для ограничения на агрегацию (group\_by\_overflow\_mode) есть ещё значение `any`.

`throw` - кинуть исключение (по умолчанию).

`break` - прервать выполнение запроса и вернуть неполный результат, как будто исходные данные закончились.

`any (только для group_by_overflow_mode)` - продолжить агрегацию по ключам, которые успели войти в набор, но не добавлять новые ключи в набор.

## max\_memory\_usage {#settings_max_memory_usage}

Максимальный возможный объём оперативной памяти для выполнения запроса на одном сервере.

В конфигурационном файле по умолчанию, ограничение равно 10 ГБ.

Настройка не учитывает объём свободной памяти или общий объём памяти на машине.
Ограничение действует на один запрос, в пределах одного сервера.
Текущее потребление памяти для каждого запроса можно посмотреть с помощью `SHOW PROCESSLIST`.
Также отслеживается и выводится в лог пиковое потребление памяти для каждого запроса.

Потребление памяти не отслеживается для состояний некоторых агрегатных функций.

Потребление памяти не полностью учитывается для состояний агрегатных функций `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` от аргументов `String` и `Array`.

Потребление памяти ограничивается также параметрами `max_memory_usage_for_user` и [max_server_memory_usage](../server-configuration-parameters/settings.md#max_server_memory_usage).

## max\_memory\_usage\_for\_user {#max-memory-usage-for-user}

Максимальный возможный объём оперативной памяти для запросов пользователя на одном сервере.

Значения по умолчанию определены в файле [Settings.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L288). По умолчанию размер не ограничен (`max_memory_usage_for_user = 0`).

Смотрите также описание настройки [max\_memory\_usage](#settings_max_memory_usage).

## max\_rows\_to\_read {#max-rows-to-read}

Следующие ограничения могут проверяться на каждый блок (а не на каждую строку). То есть, ограничения могут быть немного нарушены.

Максимальное количество строчек, которое можно прочитать из таблицы при выполнении запроса.

## max\_bytes\_to\_read {#max-bytes-to-read}

Максимальное количество байт (несжатых данных), которое можно прочитать из таблицы при выполнении запроса.

## read\_overflow\_mode {#read-overflow-mode}

Что делать, когда количество прочитанных данных превысило одно из ограничений: throw или break. По умолчанию: throw.

## max\_rows\_to\_group\_by {#settings-max-rows-to-group-by}

Максимальное количество уникальных ключей, получаемых в процессе агрегации. Позволяет ограничить потребление оперативки при агрегации.

## group\_by\_overflow\_mode {#group-by-overflow-mode}

Что делать, когда количество уникальных ключей при агрегации превысило ограничение: throw, break или any. По умолчанию: throw.
Использование значения any позволяет выполнить GROUP BY приближённо. Качество такого приближённого вычисления сильно зависит от статистических свойств данных.

## max\_bytes\_before\_external\_group\_by {#settings-max_bytes_before_external_group_by}

Включает или отключает выполнение секций `GROUP BY` во внешней памяти. Смотрите [GROUP BY во внешней памяти](../../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory).

Возможные значения:

-   Максимальный объём RAM (в байтах), который может использовать отдельная операция [GROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-clause).
-   0 — `GROUP BY` во внешней памяти отключен.

Значение по умолчанию — 0.

## max\_rows\_to\_sort {#max-rows-to-sort}

Максимальное количество строк до сортировки. Позволяет ограничить потребление оперативки при сортировке.

## max\_bytes\_to\_sort {#max-bytes-to-sort}

Максимальное количество байт до сортировки.

## sort\_overflow\_mode {#sort-overflow-mode}

Что делать, если количество строк, полученное перед сортировкой, превысило одно из ограничений: throw или break. По умолчанию: throw.

## max\_result\_rows {#setting-max_result_rows}

Ограничение на количество строк результата. Проверяются также для подзапросов и на удалённых серверах при выполнении части распределённого запроса.

## max\_result\_bytes {#max-result-bytes}

Ограничение на количество байт результата. Аналогично.

## result\_overflow\_mode {#result-overflow-mode}

Что делать, если объём результата превысил одно из ограничений: throw или break. По умолчанию: throw.

Использование break по смыслу похоже на LIMIT. Break прерывает выполнение только на уровне блока. Т.е. число строк которые вернет запрос будет больше чем ограничение [max\_result\_rows](#setting-max_result_rows), кратно [max\_block\_size](../settings/settings.md#setting-max_block_size) и зависит от [max\_threads](../settings/settings.md#settings-max_threads).

Пример:

``` sql
SET max_threads = 3, max_block_size = 3333;
SET max_result_rows = 3334, result_overflow_mode = 'break';

SELECT *
FROM numbers_mt(100000)
FORMAT Null;
```

Результат:

``` text
6666 rows in set. ...
```

## max\_execution\_time {#max-execution-time}

Максимальное время выполнения запроса в секундах.
На данный момент не проверяется при одной из стадий сортировки а также при слиянии и финализации агрегатных функций.

## timeout\_overflow\_mode {#timeout-overflow-mode}

Что делать, если запрос выполняется дольше max\_execution\_time: throw или break. По умолчанию: throw.

## min\_execution\_speed {#min-execution-speed}

Минимальная скорость выполнения запроса в строчках в секунду. Проверяется на каждый блок данных по истечении timeout\_before\_checking\_execution\_speed. Если скорость выполнения запроса оказывается меньше, то кидается исключение.

## min\_execution\_speed\_bytes {#min-execution-speed-bytes}

Минимальная скорость выполнения запроса в строках на байт. Он проверяется для каждого блока данных после timeout\_before\_checking\_execution\_speed. Если скорость выполнения запроса меньше, исключение.

## max\_execution\_speed {#max-execution-speed}

Максимальная скорость выполнения запроса в строках в секунду. Он проверяется для каждого блока данных после timeout\_before\_checking\_execution\_speed. Если скорость выполнения запроса выше, скорость будет снижена.

## max\_execution\_speed\_bytes {#max-execution-speed-bytes}

Максимальная скорость выполнения запроса в байтах в секунду. Он проверяется для каждого блока данных после timeout\_before\_checking\_execution\_speed. Если скорость выполнения запроса выше, скорость будет снижена.

## timeout\_before\_checking\_execution\_speed {#timeout-before-checking-execution-speed}

Проверять, что скорость выполнения запроса не слишком низкая (не меньше min\_execution\_speed), после прошествия указанного времени в секундах.

## max\_columns\_to\_read {#max-columns-to-read}

Максимальное количество столбцов, которых можно читать из таблицы в одном запросе. Если запрос требует чтения большего количества столбцов - кинуть исключение.

## max\_temporary\_columns {#max-temporary-columns}

Максимальное количество временных столбцов, которых необходимо одновременно держать в оперативке, в процессе выполнения запроса, включая константные столбцы. Если временных столбцов оказалось больше - кидается исключение.

## max\_temporary\_non\_const\_columns {#max-temporary-non-const-columns}

То же самое, что и max\_temporary\_columns, но без учёта столбцов-констант.
Стоит заметить, что столбцы-константы довольно часто образуются в процессе выполнения запроса, но расходуют примерно нулевое количество вычислительных ресурсов.

## max\_subquery\_depth {#max-subquery-depth}

Максимальная вложенность подзапросов. Если подзапросы более глубокие - кидается исключение. По умолчанию: 100.

## max\_pipeline\_depth {#max-pipeline-depth}

Максимальная глубина конвейера выполнения запроса. Соответствует количеству преобразований, которое проходит каждый блок данных в процессе выполнения запроса. Считается в пределах одного сервера. Если глубина конвейера больше - кидается исключение. По умолчанию: 1000.

## max\_ast\_depth {#max-ast-depth}

Максимальная вложенность синтаксического дерева запроса. Если превышена - кидается исключение.
На данный момент, проверяются не во время парсинга а уже после парсинга запроса. То есть, во время парсинга может быть создано слишком глубокое синтаксическое дерево, но запрос не будет выполнен. По умолчанию: 1000.

## max\_ast\_elements {#max-ast-elements}

Максимальное количество элементов синтаксического дерева запроса. Если превышено - кидается исключение.
Аналогично, проверяется уже после парсинга запроса. По умолчанию: 50 000.

## max\_rows\_in\_set {#max-rows-in-set}

Максимальное количество строчек для множества в секции IN, создаваемого из подзапроса.

## max\_bytes\_in\_set {#max-bytes-in-set}

Максимальное количество байт (несжатых данных), занимаемое множеством в секции IN, создаваемым из подзапроса.

## set\_overflow\_mode {#set-overflow-mode}

Что делать, когда количество данных превысило одно из ограничений: throw или break. По умолчанию: throw.

## max\_rows\_in\_distinct {#max-rows-in-distinct}

Максимальное количество различных строчек при использовании DISTINCT.

## max\_bytes\_in\_distinct {#max-bytes-in-distinct}

Максимальное количество байт, занимаемых хэш-таблицей, при использовании DISTINCT.

## distinct\_overflow\_mode {#distinct-overflow-mode}

Что делать, когда количество данных превысило одно из ограничений: throw или break. По умолчанию: throw.

## max\_rows\_to\_transfer {#max-rows-to-transfer}

Максимальное количество строчек, которых можно передать на удалённый сервер или сохранить во временную таблицу, при использовании GLOBAL IN.

## max\_bytes\_to\_transfer {#max-bytes-to-transfer}

Максимальное количество байт (несжатых данных), которых можно передать на удалённый сервер или сохранить во временную таблицу, при использовании GLOBAL IN.

## transfer\_overflow\_mode {#transfer-overflow-mode}

Что делать, когда количество данных превысило одно из ограничений: throw или break. По умолчанию: throw.

## max\_rows\_in\_join {#settings-max_rows_in_join}

Ограничивает количество строк в хэш-таблице, используемой при соединении таблиц.

Параметр применяется к операциям [SELECT… JOIN](../../sql-reference/statements/select/join.md#select-join) и к движку таблиц [Join](../../engines/table-engines/special/join.md).

Если запрос содержит несколько `JOIN`, то ClickHouse проверяет значение настройки для каждого промежуточного результата.

При достижении предела ClickHouse может выполнять различные действия. Используйте настройку [join\_overflow\_mode](#settings-join_overflow_mode) для выбора действия.

Возможные значения:

-   Положительное целое число.
-   0 — неограниченное количество строк.

Значение по умолчанию — 0.

## max\_bytes\_in\_join {#settings-max_bytes_in_join}

Ограничивает размер (в байтах) хэш-таблицы, используемой при объединении таблиц.

Параметр применяется к операциям [SELECT… JOIN](../../sql-reference/statements/select/join.md#select-join) и к движку таблиц [Join](../../engines/table-engines/special/join.md).

Если запрос содержит несколько `JOIN`, то ClickHouse проверяет значение настройки для каждого промежуточного результата.

При достижении предела ClickHouse может выполнять различные действия. Используйте настройку [join\_overflow\_mode](#settings-join_overflow_mode) для выбора действия.

Возможные значения:

-   Положительное целое число.
-   0 — контроль памяти отключен.

Значение по умолчанию — 0.

## join\_overflow\_mode {#settings-join_overflow_mode}

Определяет, какое действие ClickHouse выполняет при достижении любого из следующих ограничений для `JOIN`:

-   [max\_bytes\_in\_join](#settings-max_bytes_in_join)
-   [max\_rows\_in\_join](#settings-max_rows_in_join)

Возможные значения:

-   `THROW` — ClickHouse генерирует исключение и прерывает операцию.
-   `BREAK` — ClickHouse прерывает операцию, но не генерирует исключение.

Значение по умолчанию — `THROW`.

**Смотрите также**

-   [Секция JOIN](../../sql-reference/statements/select/join.md#select-join)
-   [Движоy таблиц Join](../../engines/table-engines/special/join.md)

## max\_partitions\_per\_insert\_block {#max-partitions-per-insert-block}

Ограничивает максимальное количество партиций в одном вставленном блоке.

-   Положительное целое число.
-   0 — неограниченное количество разделов.

Значение по умолчанию: 100.

**Подробности**

При вставке данных, ClickHouse вычисляет количество партиций во вставленном блоке. Если число партиций больше, чем `max_partitions_per_insert_block`, ClickHouse генерирует исключение со следующим текстом:

> «Too many partitions for single INSERT block (more than» + toString(max\_parts) + «). The limit is controlled by ‘max\_partitions\_per\_insert\_block’ setting. Large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc).»

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/settings/query_complexity/) <!--hide-->
