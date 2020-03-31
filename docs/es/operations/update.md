---
machine_translated: true
---

# Actualización de ClickHouse {#clickhouse-update}

Si se instaló ClickHouse desde paquetes deb, ejecute los siguientes comandos en el servidor:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Si ha instalado ClickHouse utilizando algo distinto de los paquetes deb recomendados, utilice el método de actualización adecuado.

ClickHouse no admite una actualización distribuida. La operación debe realizarse consecutivamente en cada servidor separado. No actualice todos los servidores de un clúster simultáneamente, o el clúster no estará disponible durante algún tiempo.
