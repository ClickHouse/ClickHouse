# system.zookeeper_log {#system-zookeeper_log}

Эта таблица содержит информацию о параметрах запроса к серверу ZooKeeper и ответа от него.

Для запросов заполняются только столбцы с параметрами запроса, а остальные столбцы заполняются значениями по умолчанию (`0` или `NULL`). Когда поступает ответ, данные добавляются в столбцы с параметрами ответа на запрос.

Столбцы с параметрами запроса:

-   `type` ([Enum](../../sql-reference/data-types/enum.md)) — тип события в клиенте ZooKeeper. Может иметь одно из следующих значений:
    -   `Request` — запрос отправлен.
    -   `Response` — ответ получен.
    -   `Finalize` — соединение разорвано, ответ не получен.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата, когда произошло событие.
-   `event_time` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — дата и время, когда произошло событие.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес сервера ZooKeeper, с которого был сделан запрос.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт сервера ZooKeeper, с которого был сделан запрос.
-   `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — идентификатор сессии, который сервер ZooKeeper создает для каждого соединения.
-   `xid` ([Int32](../../sql-reference/data-types/int-uint.md)) — идентификатор запроса внутри сессии. Обычно это последовательный номер запроса, одинаковый у строки запроса и у парной строки `response`/`finalize`.
-   `has_watch` ([UInt8](../../sql-reference/data-types/int-uint.md)) — установлен ли запрос [watch](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches).
-   `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — тип запроса или ответа на запрос.
-   `path` ([String](../../sql-reference/data-types/string.md)) — путь к узлу ZooKeeper, указанный в запросе. Пустая строка, если запрос не требует указания пути.
-   `data` ([String](../../sql-reference/data-types/string.md)) — данные, записанные на узле ZooKeeper (для запросов `SET` и `CREATE` — что запрос хотел записать, для ответа на запрос `GET` — что было прочитано), или пустая строка.
-   `is_ephemeral` ([UInt8](../../sql-reference/data-types/int-uint.md)) — создается ли узел ZooKeeper как [ephemeral](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Ephemeral+Nodes).
-   `is_sequential` ([UInt8](../../sql-reference/data-types/int-uint.md)) — создается ли узел ZooKeeper как [sequential](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#Sequence+Nodes+--+Unique+Naming).
-   `version` ([Nullable(Int32)](../../sql-reference/data-types/nullable.md)) — версия узла ZooKeeper, которую запрос ожидает увидеть при выполнении. Поддерживается для запросов `CHECK`, `SET`, `REMOVE` (`-1` — запрос не проверяет версию, `NULL` — для других запросов, которые не поддерживают проверку версии).
-   `requests_size` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество запросов, включенных в мультизапрос (это специальный запрос, который состоит из нескольких последовательных обычных запросов, выполняющихся атомарно). Все запросы, включенные в мультизапрос, имеют одинаковый `xid`.
-   `request_idx` ([UInt32](../../sql-reference/data-types/int-uint.md)) — номер запроса, включенного в мультизапрос (`0` — для мультизапроса, далее по порядку с `1`).

Столбцы с параметрами ответа на запрос:

-   `zxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — идентификатор транзакции в ZooKeeper. Последовательный номер, выданный сервером ZooKeeper в ответе на успешно выполненный запрос (`0` — запрос не был выполнен, возвращена ошибка или клиент ZooKeeper не знает, был ли выполнен запрос).
-   `error` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — код ошибки. Может иметь много значений, здесь приведены только некоторые из них:
    -   `ZOK` — запрос успешно выполнен.
    -   `ZCONNECTIONLOSS` — соединение разорвано.
    -   `ZOPERATIONTIMEOUT` — истекло время ожидания выполнения запроса.
	-   `ZSESSIONEXPIRED` — истекло время сессии.
    -   `NULL` — выполнен запрос.
-   `watch_type` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — тип события `watch` (для ответов на запрос при `op_num` = `Watch`), для остальных ответов: `NULL`.
-   `watch_state` ([Nullable(Enum)](../../sql-reference/data-types/nullable.md)) — статус события `watch` (для ответов на запрос при `op_num` = `Watch`), для остальных ответов: `NULL`.
-   `path_created` ([String](../../sql-reference/data-types/string.md)) — путь к созданному узлу ZooKeeper (для ответов на запрос `CREATE`). Может отличаться от `path`, если узел создается как `sequential`.
-   `stat_czxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — идентификатор транзакции, в результате которой был создан узел ZooKeeper.
-   `stat_mzxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — идентификатор транзакции, которая последней модифицировала узел ZooKeeper.
-   `stat_pzxid` ([Int64](../../sql-reference/data-types/int-uint.md)) — идентификатор транзакции, которая последней модифицировала дочерние узлы ZooKeeper.
-   `stat_version` ([Int32](../../sql-reference/data-types/int-uint.md)) — количество изменений в данных узла ZooKeeper.
-   `stat_cversion` ([Int32](../../sql-reference/data-types/int-uint.md)) — количество изменений в дочерних узлах ZooKeeper.
-   `stat_dataLength` ([Int32](../../sql-reference/data-types/int-uint.md)) — длина поля данных узла ZooKeeper.
-   `stat_numChildren` ([Int32](../../sql-reference/data-types/int-uint.md)) — количество дочерних узлов ZooKeeper.
-   `children` ([Array(String)](../../sql-reference/data-types/array.md)) — список дочерних узлов ZooKeeper (для ответов на запрос `LIST`).

**Пример**

Запрос:

``` sql
SELECT * FROM system.zookeeper_log WHERE (session_id = '106662742089334927') AND (xid = '10858') FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
type:             Request
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.291792
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:             
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             0
error:            ᴺᵁᴸᴸ
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:     
stat_czxid:       0
stat_mzxid:       0
stat_pzxid:       0
stat_version:     0
stat_cversion:    0
stat_dataLength:  0
stat_numChildren: 0
children:         []

Row 2:
──────
type:             Response
event_date:       2021-08-09
event_time:       2021-08-09 21:38:30.292086
address:          ::
port:             2181
session_id:       106662742089334927
xid:              10858
has_watch:        1
op_num:           List
path:             /clickhouse/task_queue/ddl
data:             
is_ephemeral:     0
is_sequential:    0
version:          ᴺᵁᴸᴸ
requests_size:    0
request_idx:      0
zxid:             16926267
error:            ZOK
watch_type:       ᴺᵁᴸᴸ
watch_state:      ᴺᵁᴸᴸ
path_created:     
stat_czxid:       16925469
stat_mzxid:       16925469
stat_pzxid:       16926179
stat_version:     0
stat_cversion:    7
stat_dataLength:  0
stat_numChildren: 7
children:         ['query-0000000006','query-0000000005','query-0000000004','query-0000000003','query-0000000002','query-0000000001','query-0000000000']
```

**См. также**

-   [ZooKeeper](../../operations/tips.md#zookeeper)
-   [Руководство по ZooKeeper](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)
