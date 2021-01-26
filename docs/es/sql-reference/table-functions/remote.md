---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: remoto
---

# remoto, remoteSecure {#remote-remotesecure}

Le permite acceder a servidores remotos sin crear un `Distributed` tabla.

Firma:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port` o simplemente `host`. El host se puede especificar como nombre de servidor o como dirección IPv4 o IPv6. Una dirección IPv6 se especifica entre corchetes. El puerto es el puerto TCP del servidor remoto. Si se omite el puerto, utiliza `tcp_port` del archivo de configuración del servidor (por defecto, 9000).

!!! important "Importante"
    El puerto es necesario para una dirección IPv6.

Ejemplos:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Se pueden separar varias direcciones por comas. En este caso, ClickHouse usará procesamiento distribuido, por lo que enviará la consulta a todas las direcciones especificadas (como a fragmentos con datos diferentes).

Ejemplo:

``` text
example01-01-1,example01-02-1
```

Parte de la expresión se puede especificar entre llaves. El ejemplo anterior se puede escribir de la siguiente manera:

``` text
example01-0{1,2}-1
```

Los corchetes rizados pueden contener un rango de números separados por dos puntos (enteros no negativos). En este caso, el rango se expande a un conjunto de valores que generan direcciones de fragmentos. Si el primer número comienza con cero, los valores se forman con la misma alineación cero. El ejemplo anterior se puede escribir de la siguiente manera:

``` text
example01-{01..02}-1
```

Si tiene varios pares de llaves, genera el producto directo de los conjuntos correspondientes.

Las direcciones y partes de las direcciones entre llaves se pueden separar mediante el símbolo de tubería (\|). En este caso, los conjuntos de direcciones correspondientes se interpretan como réplicas y la consulta se enviará a la primera réplica en buen estado. Sin embargo, las réplicas se iteran en el orden establecido actualmente en el [load\_balancing](../../operations/settings/settings.md) configuración.

Ejemplo:

``` text
example01-{01..02}-{1|2}
```

En este ejemplo se especifican dos fragmentos que tienen dos réplicas cada uno.

El número de direcciones generadas está limitado por una constante. En este momento esto es 1000 direcciones.

Uso de la `remote` función de la tabla es menos óptima que la creación de un `Distributed` mesa, porque en este caso, la conexión del servidor se restablece para cada solicitud. Además, si se establecen nombres de host, los nombres se resuelven y los errores no se cuentan cuando se trabaja con varias réplicas. Cuando procese un gran número de consultas, cree siempre el `Distributed` mesa antes de tiempo, y no utilice el `remote` función de la tabla.

El `remote` puede ser útil en los siguientes casos:

-   Acceder a un servidor específico para la comparación de datos, la depuración y las pruebas.
-   Consultas entre varios clústeres de ClickHouse con fines de investigación.
-   Solicitudes distribuidas poco frecuentes que se realizan manualmente.
-   Solicitudes distribuidas donde el conjunto de servidores se redefine cada vez.

Si el usuario no está especificado, `default` se utiliza.
Si no se especifica la contraseña, se utiliza una contraseña vacía.

`remoteSecure` - igual que `remote` but with secured connection. Default port — [Tcp\_port\_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) de config o 9440.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
