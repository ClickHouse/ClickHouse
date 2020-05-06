---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 39
toc_title: Establecer
---

# Establecer {#set}

Un conjunto de datos que siempre está en la memoria RAM. Está diseñado para su uso en el lado derecho del operador IN (consulte la sección “IN operators”).

Puede usar INSERT para insertar datos en la tabla. Se agregarán nuevos elementos al conjunto de datos, mientras que los duplicados se ignorarán.
Pero no puede realizar SELECT desde la tabla. La única forma de recuperar datos es usándolos en la mitad derecha del operador IN.

Los datos siempre se encuentran en la memoria RAM. Para INSERT, los bloques de datos insertados también se escriben en el directorio de tablas en el disco. Al iniciar el servidor, estos datos se cargan en la RAM. En otras palabras, después de reiniciar, los datos permanecen en su lugar.

Para un reinicio aproximado del servidor, el bloque de datos en el disco puede perderse o dañarse. En este último caso, es posible que deba eliminar manualmente el archivo con datos dañados.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/set/) <!--hide-->
