---
slug: /ru/engines/table-engines/special/view
sidebar_position: 42
sidebar_label: View
---

# View {#table_engines-view}

Используется для реализации представлений (подробнее см. запрос `CREATE VIEW`). Не хранит данные, а хранит только указанный запрос `SELECT`. При чтении из таблицы, выполняет его (с удалением из запроса всех ненужных столбцов).
