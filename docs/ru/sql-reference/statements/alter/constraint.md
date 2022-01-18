---
toc_priority: 43
toc_title: "\u041c\u0430\u043d\u0438\u043f\u0443\u043b\u044f\u0446\u0438\u0438\u0020\u0441\u0020\u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u0438\u044f\u043c\u0438"
---

# Манипуляции с ограничениями (constraints) {#manipuliatsii-s-ogranicheniiami-constraints}

Про ограничения подробнее написано [тут](../create/table.md#constraints).

Добавить или удалить ограничение можно с помощью запросов

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

Запросы выполняют добавление или удаление метаданных об ограничениях таблицы `[db].name`, поэтому выполняются мгновенно.

Если ограничение появилось для непустой таблицы, то *проверка ограничения для имеющихся данных не производится*.

Запрос на изменение ограничений для Replicated таблиц реплицируется, сохраняя новые метаданные в ZooKeeper и применяя изменения на всех репликах.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/constraint/) <!--hide-->