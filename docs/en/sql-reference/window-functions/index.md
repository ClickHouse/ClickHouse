---
toc_priority: 62
toc_title: Window Functions
---

# [experimental] Window Functions

!!! warning "Warning"
    This is an experimental feature that is currently in development and is not ready for general use. It will change in unpredictable backwards-incompatible ways in the future releases. Set `allow_experimental_window_functions = 1` to enable it.

ClickHouse supports the standard grammar for defining windows and window functions. The following features are currently supported:

| Feature | Support or workaround |
| --------| ----------|
| ad hoc window specification (`count(*) over (partition by id order by time desc)`) | supported |
| expressions involving window functions, e.g. `(count(*) over ()) / 2)` | not supported, wrap in a subquery ([feature request](https://github.com/ClickHouse/ClickHouse/issues/19857)) |
| `WINDOW` clause (`select ... from table window w as (partiton by id)`) | supported |
| `ROWS` frame | supported |
| `RANGE` frame | supported, the default |
| `INTERVAL` syntax for `DateTime` `RANGE OFFSET` frame | not supported, specify the number of seconds instead |
| `GROUPS` frame | not supported |
| Calculating aggregate functions over a frame (`sum(value) over (order by time)`) | all aggregate functions are supported |
| `rank()`, `dense_rank()`, `row_number()` | supported |
| `lag/lead(value, offset)` | Not supported. Workarounds: |
|  | 1) replace with `any(value) over (.... rows between <offset> preceding and <offset> preceding)`, or `following` for `lead`|
|  | 2) use `lagInFrame/leadInFrame`, which are analogous, but respect the window frame. To get behavior identical to `lag/lead`, use `rows between unbounded preceding and unbounded following` |

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
