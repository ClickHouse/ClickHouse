---
machine_translated: true
machine_translated_rev: b29e72533c161967b8b0b5a3b0391347dadd5679
---

# Cláusula LIMIT {#limit-clause}

`LIMIT m` permite seleccionar la primera `m` filas del resultado.

`LIMIT n, m` permite seleccionar el `m` el resultado después de omitir la primera `n` filas. El `LIMIT m OFFSET n` sintaxis es equivalente.

`n` y `m` deben ser enteros no negativos.

Si no hay [ORDER BY](order-by.md) cláusula que ordena explícitamente los resultados, la elección de las filas para el resultado puede ser arbitraria y no determinista.
