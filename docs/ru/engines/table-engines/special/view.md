---
toc_priority: 42
toc_title: View
---

# View {#table_engines-view}

Используется для реализации представлений (подробнее см. запрос `CREATE VIEW`). Не хранит данные, а хранит только указанный запрос `SELECT`. При чтении из таблицы, выполняет его (с удалением из запроса всех ненужных столбцов).

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/view/) <!--hide-->
