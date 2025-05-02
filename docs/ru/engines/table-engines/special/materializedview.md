---
slug: /ru/engines/table-engines/special/materializedview
sidebar_position: 43
sidebar_label: MaterializedView
---

# MaterializedView {#materializedview}

Используется для реализации материализованных представлений (подробнее см. запрос [CREATE VIEW](/sql-reference/statements/create/view#materialized-view)). Для хранения данных, использует другой движок, который был указан при создании представления. При чтении из таблицы, просто использует этот движок.
