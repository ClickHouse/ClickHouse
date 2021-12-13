---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# PREWHERE Cláusula {#prewhere-clause}

Prewhere es una optimización para aplicar el filtrado de manera más eficiente. Está habilitado de forma predeterminada incluso si `PREWHERE` cláusula no se especifica explícitamente. Funciona moviendo automáticamente parte de [WHERE](where.md) condición a la etapa prewhere. El papel de `PREWHERE` cláusula es sólo para controlar esta optimización si usted piensa que usted sabe cómo hacerlo mejor de lo que sucede por defecto.

Con la optimización prewhere, al principio solo se leen las columnas necesarias para ejecutar la expresión prewhere. Luego se leen las otras columnas que son necesarias para ejecutar el resto de la consulta, pero solo aquellos bloques donde está la expresión prewhere “true” al menos para algunas filas. Si hay muchos bloques donde la expresión prewhere es “false” para todas las filas y prewhere necesita menos columnas que otras partes de la consulta, esto a menudo permite leer muchos menos datos del disco para la ejecución de la consulta.

## Control de Prewhere manualmente {#controlling-prewhere-manually}

La cláusula tiene el mismo significado que la `WHERE` clausula. La diferencia radica en qué datos se leen de la tabla. Al controlar manualmente `PREWHERE` para las condiciones de filtración utilizadas por una minoría de las columnas de la consulta, pero que proporcionan una filtración de datos segura. Esto reduce el volumen de datos a leer.

Una consulta puede especificar simultáneamente `PREWHERE` y `WHERE`. En este caso, `PREWHERE` preceder `WHERE`.

Si el `optimize_move_to_prewhere` se establece en 0, heurística para mover automáticamente partes de expresiones de `WHERE` a `PREWHERE` están deshabilitados.

## Limitacion {#limitations}

`PREWHERE` sólo es compatible con tablas de la `*MergeTree` familia.
