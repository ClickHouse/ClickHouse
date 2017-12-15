# system.zookeeper

Позволяет читать данные из ZooKeeper кластера, описанного в конфигурации.
В запросе обязательно в секции WHERE должно присутствовать условие на равенство path - путь в ZooKeeper, для детей которого вы хотите получить данные.

Запрос `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` выведет данные по всем детям узла `/clickhouse`.
Чтобы вывести данные по всем узлам в корне, напишите path = '/'.
Если узла, указанного в path не существует, то будет брошено исключение.

Столбцы:

```text
name String          - имя узла
path String          - путь к узлу
value String         - значение узла
dataLength Int32     - размер значения
numChildren Int32    - количество детей
czxid Int64          - идентификатор транзакции, в которой узел был создан
mzxid Int64          - идентификатор транзакции, в которой узел был последний раз изменён
pzxid Int64          - идентификатор транзакции, последний раз удаливший или добавивший детей
ctime DateTime       - время создания узла
mtime DateTime       - время последней модификации узла
version Int32        - версия узла - количество раз, когда узел был изменён
cversion Int32       - количество добавлений или удалений детей
aversion Int32       - количество изменений ACL
ephemeralOwner Int64 - для эфемерных узлов - идентификатор сессии, которая владеет этим узлом
```

Пример:

```sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

```text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```
