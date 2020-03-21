# numero {#numbers}

`numbers(N)` – Devuelve una tabla con el único ‘number’ columna (UInt64) que contiene enteros de 0 a N-1.
`numbers(N, M)` - Devuelve una tabla con el único ‘number’ columna (UInt64) que contiene enteros de N a (N + M - 1).

Similar a la `system.numbers` tabla, puede ser utilizado para probar y generar valores sucesivos, `numbers(N, M)` más eficiente que `system.numbers`.

Las siguientes consultas son equivalentes:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

Ejemplos:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[Artículo Original](https://clickhouse.tech/docs/es/query_language/table_functions/numbers/) <!--hide-->
