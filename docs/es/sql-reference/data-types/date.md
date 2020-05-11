---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 47
toc_title: Fecha
---

# Fecha {#date}

Fecha. Almacenado en dos bytes como el número de días desde 1970-01-01 (sin signo). Permite almacenar valores desde justo después del comienzo de la Época Unix hasta el umbral superior definido por una constante en la etapa de compilación (actualmente, esto es hasta el año 2106, pero el último año totalmente soportado es 2105).
El valor mínimo se emite como 0000-00-00.

El valor de fecha se almacena sin la zona horaria.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/date/) <!--hide-->
