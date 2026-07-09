---
title: Движок Template
---

<div id="enginename">
  # EngineName
</div>

* Назначение движка базы данных/таблицы.
* Взаимосвязи с другими движками, если они есть.

<div id="creating-a-database">
  ## Создание базы данных
</div>

```sql
    CREATE DATABASE ...
```

или

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
    CREATE TABLE ...
```

**Параметры движка**

**Секции запроса** (только для движков таблиц)

<div id="virtual-columns-virtual-columns-for-table-engines-only">
  ## Виртуальные столбцы                    (только для движков таблиц)
</div>

Список виртуальных столбцов с описанием, если они есть.

<div id="data-types-support-data_types-support-for-database-engines-only">
  ## Поддерживаемые типы данных                       (только для движков баз данных)
</div>

| EngineName         | ClickHouse                      |
| ------------------ | ------------------------------- |
| NativeDataTypeName | [ClickHouseDataTypeName](link#) |

<div id="specifics-and-recommendations">
  ## Особенности и рекомендации
</div>

Алгоритмы
Особенности процессов чтения и записи
Примеры задач
Рекомендации по использованию
Особенности хранения данных

<div id="usage-example">
  ## Пример использования
</div>

В примере должны быть показаны варианты использования и сценарии применения. Ниже приведён текст с рекомендуемыми частями этого раздела.

Исходная таблица:

```text
```

```sql title="Query"
```

```text title="Response"
```

При необходимости добавьте текст, поясняющий пример.

**См. также**

* [ссылка](#)