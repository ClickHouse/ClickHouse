---
machine_translated: true
machine_translated_rev: b29e72533c161967b8b0b5a3b0391347dadd5679
---

# DONDE Cláusula {#select-where}

`WHERE` cláusula permite filtrar los datos que provienen de [FROM](from.md) cláusula de `SELECT`.

Si hay un `WHERE` cláusula, debe contener una expresión con el `UInt8` tipo. Esta suele ser una expresión con comparación y operadores lógicos. Las filas en las que esta expresión se evalúa como 0 se explican a partir de otras transformaciones o resultados.

`WHERE` expresión se evalúa en la capacidad de utilizar índices y poda de partición, si el motor de tabla subyacente lo admite.

!!! note "Nota"
    Hay una optimización de filtrado llamada [preliminar](prewhere.md).
