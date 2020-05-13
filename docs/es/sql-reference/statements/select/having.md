---
machine_translated: true
machine_translated_rev: b29e72533c161967b8b0b5a3b0391347dadd5679
---

# Cláusula HAVING {#having-clause}

Permite filtrar los resultados de agregación producidos por [GROUP BY](group-by.md). Es similar a la [WHERE](where.md) cláusula, pero la diferencia es que `WHERE` se realiza antes de la agregación, mientras que `HAVING` se realiza después de eso.

Es posible hacer referencia a los resultados de la agregación de `SELECT` cláusula en `HAVING` cláusula por su alias. Alternativamente, `HAVING` cláusula puede filtrar los resultados de agregados adicionales que no se devuelven en los resultados de la consulta.

## Limitacion {#limitations}

`HAVING` no se puede usar si no se realiza la agregación. Utilizar `WHERE` en su lugar.
