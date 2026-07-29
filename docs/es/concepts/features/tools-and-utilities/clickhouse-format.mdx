---
description: 'Guía para usar la utilidad de formato para trabajar con los formatos de datos de ClickHouse'
slug: /operations/utilities/clickhouse-format
title: 'clickhouse-format'
doc_type: 'reference'
---

Permite formatear consultas de entrada.

Opciones:

* `--help` o `-h` — Muestra el mensaje de ayuda.
* `--query` — Formatea consultas de cualquier longitud y complejidad.
* `--hilite` o `--highlight` — Añade resaltado de sintaxis con secuencias de escape ANSI del terminal.
* `--oneline` — Formatea en una sola línea.
* `--max_line_length` — Formatea en una sola línea las consultas cuya longitud sea inferior a la especificada.
* `--comments` — Conserva los comentarios en la salida.
* `--quiet` o `-q` — Solo comprueba la sintaxis; no produce salida si se ejecuta correctamente.
* `--multiquery` o `-n` — Permite varias consultas en el mismo archivo.
* `--obfuscate` — Ofusca en lugar de formatear.
* `--seed <string>` — Cadena semilla arbitraria que determina el resultado de la ofuscación.
* `--backslash` — Añade una barra invertida al final de cada línea de la consulta formateada. Puede ser útil cuando copias una consulta desde la web o desde otro lugar en varias líneas y quieres ejecutarla en la línea de comandos.
* `--semicolons_inline` — En el modo multiquery, escribe los puntos y coma en la última línea de la consulta en lugar de en una línea nueva.

<div id="examples">
  ## Ejemplos
</div>

1. Formatear una consulta:

```bash title="Query"
$ clickhouse-format --query "select number from numbers(10) where number%2 order by number desc;"
```

```bash title="Response"
SELECT number
FROM numbers(10)
WHERE number % 2
ORDER BY number DESC
```

2. Resaltado y una sola línea:

```bash title="Query"
$ clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

```sql title="Response"
SELECT sum(number) FROM numbers(5)
```

3. Múltiples consultas:

```bash title="Query"
$ clickhouse-format -n <<< "SELECT min(number) FROM numbers(5); SELECT max(number) FROM numbers(5);"
```

```sql title="Response"
SELECT min(number)
FROM numbers(5)
;

SELECT max(number)
FROM numbers(5)
;

```

4. Ofuscación:

```bash title="Query"
$ clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```

La misma consulta y otra cadena usada como semilla:

```bash title="Query"
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

5. Añadir la barra invertida:

```bash title="Query"
$ clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

```sql title="Response"
SELECT * \
FROM  \
( \
    SELECT 1 AS x \
    UNION ALL \
    SELECT 1 \
    UNION DISTINCT \
    SELECT 3 \
)
```