---
machine_translated: true
---

# TinyLog {#tinylog}

El motor pertenece a la familia de motores de registro. Ver [Familia del motor de registro](log_family.md) para las propiedades comunes de los motores de registro y sus diferencias.

Este motor de tablas se usa normalmente con el método write-once: escribir datos una vez, luego leerlos tantas veces como sea necesario. Por ejemplo, puede usar `TinyLog`-type tablas para datos intermedios que se procesan en pequeños lotes. Tenga en cuenta que el almacenamiento de datos en un gran número de tablas pequeñas es ineficiente.

Las consultas se ejecutan en una sola secuencia. En otras palabras, este motor está diseñado para tablas relativamente pequeñas (hasta aproximadamente 1,000,000 filas). Tiene sentido utilizar este motor de tablas si tiene muchas tablas pequeñas, ya que es más simple que el [Registro](log.md) motor (menos archivos necesitan ser abiertos.

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/tinylog/) <!--hide-->
