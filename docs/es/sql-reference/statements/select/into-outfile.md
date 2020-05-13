---
machine_translated: true
machine_translated_rev: b29e72533c161967b8b0b5a3b0391347dadd5679
---

# INTO OUTFILE Cláusula {#into-outfile-clause}

Añadir el `INTO OUTFILE filename` cláusula (donde filename es un literal de cadena) para `SELECT query` para redirigir su salida al archivo especificado en el lado del cliente.

## Detalles de implementación {#implementation-details}

-   Esta funcionalidad está disponible en el [cliente de línea de comandos](../../../interfaces/cli.md) y [Sistema abierto.](../../../operations/utilities/clickhouse-local.md). Por lo tanto, una consulta enviada a través de [Interfaz HTTP](../../../interfaces/http.md) fallará.
-   La consulta fallará si ya existe un archivo con el mismo nombre de archivo.
-   Predeterminado [formato de salida](../../../interfaces/formats.md) ser `TabSeparated` (como en el modo por lotes de cliente de línea de comandos).
