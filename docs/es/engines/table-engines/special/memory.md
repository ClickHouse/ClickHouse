---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Memoria
---

# Memoria {#memory}

El motor de memoria almacena datos en RAM, en forma sin comprimir. Los datos se almacenan exactamente en la misma forma en que se reciben cuando se leen. En otras palabras, la lectura de esta tabla es completamente gratuita.
El acceso a los datos simultáneos está sincronizado. Los bloqueos son cortos: las operaciones de lectura y escritura no se bloquean entre sí.
Los índices no son compatibles. La lectura está paralelizada.
La productividad máxima (más de 10 GB/s) se alcanza en consultas simples, porque no hay lectura del disco, descomprimir o deserializar datos. (Cabe señalar que, en muchos casos, la productividad del motor MergeTree es casi tan alta.)
Al reiniciar un servidor, los datos desaparecen de la tabla y la tabla queda vacía.
Normalmente, el uso de este motor de tabla no está justificado. Sin embargo, se puede usar para pruebas y para tareas donde se requiere la velocidad máxima en un número relativamente pequeño de filas (hasta aproximadamente 100,000,000).

El sistema utiliza el motor de memoria para tablas temporales con datos de consulta externos (consulte la sección “External data for processing a query”), y para la implementación de GLOBAL IN (véase la sección “IN operators”).

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/memory/) <!--hide-->
