---
slug: /ru/sql-reference/table-functions/fileCluster
sidebar_position: 38
sidebar_label: fileCluster
---

# fileCluster

Позволяет одновременно обрабатывать файлы, находящиеся по указанному пути, на нескольких узлах внутри кластера. Узел-инициатор устанавливает соединения с рабочими узлами (worker nodes), раскрывает шаблоны в пути к файлам и отдаёт задачи по чтению файлов рабочим узлам. Рабочий узел запрашивает у инициатора путь к следующему файлу для обработки, повторяя до тех пор, пока не завершатся все задачи (то есть пока не будут обработаны все файлы).

:::note    
Эта табличная функция будет работать _корректно_ только в случае, если набор файлов, соответствующих изначально указанному пути, одинаков на всех узлах и содержание этих файлов идентично на различных узлах. В случае, если эти файлы различаются между узлами, результат не предопределён и зависит от очерёдности, с которой рабочие узлы будут запрашивать задачи у инициатора.
:::

**Синтаксис**

``` sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

**Аргументы**

- `cluster_name` — имя кластера, используемое для создания набора адресов и параметров подключения к удаленным и локальным серверам.
- `path` — относительный путь до файла от [user_files_path](../../sql-reference/table-functions/file.md#server_configuration_parameters-user_files_path). Путь к файлу поддерживает [шаблоны поискаglobs](#globs_in_path). 
- `format` — [формат](../../interfaces/formats.md#formats) файла.
- `structure` — структура таблицы. Формат: `'colunmn1_name column1_ype, column2_name column2_type, ...'`.
- `compression_method` — Используемый тип сжатия. Поддерживаемые типы: `gz`, `br`, `xz`, `zst`, `lz4` и `bz2`.

**Возвращаемое значение**

Таблица с указанным форматом и структурой, содержащая данные из файлов, соответствующих указанному пути.

**Пример**
Пусть есть кластер с именем `my_cluster`, а также установлено нижеследующее значение параметра `user_files_path`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

Пусть также на каждом узле кластера в директории `user_files_path` находятся файлы `test1.csv` и `test2.csv`, и их содержимое идентично на разных узлах:
```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

Например, эти файлы можно создать, выполнив на каждом узле два запроса:
```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

Прочитаем содержимое файлов `test1.csv` и `test2.csv` с помощью табличной функции `fileCluster`:

```sql
SELECT * from fileCluster(
    'my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY (i, s)"""
)
```

```
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```


## Шаблоны поиска в компонентах пути {#globs_in_path}

Поддерживаются все шаблоны поиска, что поддерживаются табличной функцией [File](../../sql-reference/table-functions/file.md#globs-in-path).

**Смотрите также**

- [File (табличная функция)](../../sql-reference/table-functions/file.md)
