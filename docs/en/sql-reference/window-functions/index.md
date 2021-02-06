---
toc_priority: 62
toc_title: Window Functions
---

# [experimental] Window Functions

!!! warning "Warning"
This is an experimental feature that is currently in development and is not ready
for general use. It will change in unpredictable backwards-incompatible ways in
the future releases. Set `allow_experimental_window_functions = 1` to enable it.

ClickHouse currently supports calculation of aggregate functions over a window.
Pure window functions such as `rank`, `lag`, `lead` and so on are not yet supported.

The window can be specified either with an `OVER` clause or with a separate
`WINDOW` clause.

Only two variants of frame are supported, `ROWS` and `RANGE`. Offsets for the `RANGE` frame are not yet supported.

## References

### GitHub Issues
The roadmap for the initial support of window functions is [in this issue](https://github.com/ClickHouse/ClickHouse/issues/18097).

All GitHub issues related to window funtions have the [comp-window-functions](https://github.com/ClickHouse/ClickHouse/labels/comp-window-functions) tag.

### Tests
These tests contain the examples of the currently supported grammar:
https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/window_functions.xml
https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/01591_window_functions.sql

### Postgres Docs
https://www.postgresql.org/docs/current/sql-select.html#SQL-WINDOW
https://www.postgresql.org/docs/devel/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
https://www.postgresql.org/docs/devel/functions-window.html
https://www.postgresql.org/docs/devel/tutorial-window.html

### MySQL Docs
https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
