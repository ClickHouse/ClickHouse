---
toc_priority: 32
toc_title: Atomic
---

# Atomic {#atomic}

Поддерживает неблокирующие запросы `DROP` и `RENAME TABLE` и атомарные запросы `EXCHANGE TABLES t1 AND t2`. Движок `Atomic` используется по умолчанию.

## Создание БД {#creating-a-database}

``` sql
    CREATE DATABASE test[ ENGINE = Atomic];
```

## Особенности и рекомендации {#specifics-and-recommendations}

Каждая таблица в базе данных `Atomic` имеет уникальный [UUID](../../sql-reference/data-types/uuid.md) и хранит данные в папке `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`, где `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` - это UUID таблицы. Можно получить доступ к любой таблице базы данных `Atomic` по ее UUID через DatabaseCatalog.

### DROP TABLE

При выполнении запроса `DROP TABLE` никакие данные не удаляются. Таблица помечается как удаленная, метаданные перемещаются в папку `/clickhouse_path/metadata_dropped/` и база данных уведомляет DatabaseCatalog. Запущенные запросы все еще могут использовать удаленную таблицу. Таблица будет фактически удалена, когда она не будет использоваться.

### RENAME TABLE

Запросы `RENAME` выполняются без изменения UUID и перемещения табличных данных. `RENAME` и `DROP` не требуют RWLocks на уровне хранилища.

### DELETE/DETACH

Запросы `DELETE` и `DETACH` выполняются асинхронно — база данных ожидает завершения запущенных запросов `SELECT`, но невидима для новых запросов.

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

## Смотрите также

-   Системная таблица [system.databases](../../operations/system-tables/databases.md).
