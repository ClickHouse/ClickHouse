---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 47
toc_title: "Actualizaci\xF3n de ClickHouse"
---

# Actualización De ClickHouse {#clickhouse-update}

Si se instaló ClickHouse desde paquetes deb, ejecute los siguientes comandos en el servidor:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Si ha instalado ClickHouse utilizando algo distinto de los paquetes deb recomendados, utilice el método de actualización adecuado.

ClickHouse no admite una actualización distribuida. La operación debe realizarse consecutivamente en cada servidor separado. No actualice todos los servidores de un clúster simultáneamente, o el clúster no estará disponible durante algún tiempo.
