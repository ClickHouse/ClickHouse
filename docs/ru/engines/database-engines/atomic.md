---
toc_priority: 32
toc_title: Atomic
---

# Atomic {#atomic}

Поддерживает неблокирующие запросы `DROP` и `RENAME TABLE` и запросы `EXCHANGE TABLES t1 AND t2`. Движок `Atomic` используется по умолчанию.

## Создание БД {#creating-a-database}

``` sql
    CREATE DATABASE test ENGINE = Atomic;
```

**Параметры движка**

## Поддержка типов данных {#data_types-support} 

|  EngineName           | ClickHouse                         |
|-----------------------|------------------------------------|
| NativeDataTypeName    | [ClickHouseDataTypeName](link#)    |


## Особенности и рекомендации {#specifics-and-recommendations}

Каждая таблица в базе данных `Atomic` имеет уникальный UUID и хранит данные в папке `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`, где `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` - это UUID таблицы.
Запросы `RENAME` выполняются без изменения UUID и перемещения табличных данных.
Можно получить доступ к любой таблице базы данных `Atomic` по ее UUID через DatabaseCatalog.
При выполнении запроса `DROP TABLE` никакие данные не удаляются. Таблица помечается как удаленная, метаданные перемещаются в папку `/clickhouse_path/metadata_dropped/` и база данных уведомляет DatabaseCatalog.
Запущенные запросы все еще могут использовать удаленную таблицу. Таблица будет фактически удалена, когда она не будет использоваться.
Позволяет выполнять `RENAME` и `DROP` без RWLocks на уровне хранилища.

## Примеры использования {#usage-example}

Создадим базу данных:

``` text
```

Запрос:

``` sql
```

Результат:

``` text
```

Follow up with any text to clarify the example.
