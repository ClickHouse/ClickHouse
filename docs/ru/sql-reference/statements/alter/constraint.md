---
toc_priority: 43
toc_title: "Манипуляции с ограничениями"
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

