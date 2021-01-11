---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: Registro
---

# Registro {#log}

El motor pertenece a la familia de motores de registro. Consulte las propiedades comunes de los motores de registro y sus diferencias en [Familia del motor de registro](index.md) artículo.

El registro difiere de [TinyLog](tinylog.md) en que un pequeño archivo de “marks” reside con los archivos de columna. Estas marcas se escriben en cada bloque de datos y contienen compensaciones que indican dónde comenzar a leer el archivo para omitir el número especificado de filas. Esto hace posible leer datos de tabla en múltiples hilos.
Para el acceso a datos simultáneos, las operaciones de lectura se pueden realizar simultáneamente, mientras que las operaciones de escritura bloquean las lecturas entre sí.
El motor de registro no admite índices. Del mismo modo, si la escritura en una tabla falla, la tabla se rompe y la lectura de ella devuelve un error. El motor de registro es adecuado para datos temporales, tablas de escritura única y para fines de prueba o demostración.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/log/) <!--hide-->
