---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "Configuraci\xF3n del usuario"
---

# Configuración del usuario {#user-settings}

El `users` sección de la `user.xml` el archivo de configuración contiene la configuración del usuario.

!!! note "Información"
    ClickHouse también es compatible [Flujo de trabajo controlado por SQL](../access-rights.md#access-control) para la gestión de usuarios. Recomendamos usarlo.

Estructura del `users` apartado:

``` xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>
        <!-- Or -->
        <password_sha256_hex></password_sha256_hex>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>

        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                <table_name>
            </database_name>
        </databases>
    </user_name>
    <!-- Other users settings -->
</users>
```

### user\_name/contraseña {#user-namepassword}

La contraseña se puede especificar en texto sin formato o en SHA256 (formato hexagonal).

-   Para asignar una contraseña en texto sin formato (**no se recomienda**), colóquelo en un `password` elemento.

    Por ejemplo, `<password>qwerty</password>`. La contraseña se puede dejar en blanco.

<a id="password_sha256_hex"></a>

-   Para asignar una contraseña utilizando su hash SHA256, colóquela en un `password_sha256_hex` elemento.

    Por ejemplo, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    Ejemplo de cómo generar una contraseña desde el shell:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    La primera línea del resultado es la contraseña. La segunda línea es el hash SHA256 correspondiente.

<a id="password_double_sha1_hex"></a>

-   Para la compatibilidad con los clientes MySQL, la contraseña se puede especificar en doble hash SHA1. Colóquelo en `password_double_sha1_hex` elemento.

    Por ejemplo, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    Ejemplo de cómo generar una contraseña desde el shell:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    La primera línea del resultado es la contraseña. La segunda línea es el hash SHA1 doble correspondiente.

### access\_management {#access_management-user-setting}

Esta configuración habilita deshabilita el uso de [control de acceso y gestión de cuentas](../access-rights.md#access-control) para el usuario.

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

### user\_name/redes {#user-namenetworks}

Lista de redes desde las que el usuario puede conectarse al servidor ClickHouse.

Cada elemento de la lista puede tener una de las siguientes formas:

-   `<ip>` — IP address or network mask.

    Ejemplos: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Hostname.

    Ejemplo: `example01.host.ru`.

    Para comprobar el acceso, se realiza una consulta DNS y todas las direcciones IP devueltas se comparan con la dirección del mismo nivel.

-   `<host_regexp>` — Regular expression for hostnames.

    Ejemplo, `^example\d\d-\d\d-\d\.host\.ru$`

    Para comprobar el acceso, un [Consulta de DNS PTR](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) se realiza para la dirección del mismo nivel y luego se aplica la expresión regular especificada. A continuación, se realiza otra consulta DNS para los resultados de la consulta PTR y todas las direcciones recibidas se comparan con la dirección del mismo nivel. Recomendamos encarecidamente que regexp termine con $ .

Todos los resultados de las solicitudes DNS se almacenan en caché hasta que el servidor se reinicia.

**Ejemplos**

Para abrir el acceso del usuario desde cualquier red, especifique:

``` xml
<ip>::/0</ip>
```

!!! warning "Advertencia"
    No es seguro abrir el acceso desde cualquier red a menos que tenga un firewall configurado correctamente o el servidor no esté conectado directamente a Internet.

Para abrir el acceso solo desde localhost, especifique:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user\_name/perfil {#user-nameprofile}

Puede asignar un perfil de configuración para el usuario. Los perfiles de configuración se configuran en una sección separada del `users.xml` file. Para obtener más información, consulte [Perfiles de configuración](settings-profiles.md).

### user\_name/cuota {#user-namequota}

Las cuotas le permiten realizar un seguimiento o limitar el uso de recursos durante un período de tiempo. Las cuotas se configuran en el `quotas`
sección de la `users.xml` archivo de configuración.

Puede asignar un conjunto de cuotas para el usuario. Para obtener una descripción detallada de la configuración de las cuotas, consulte [Cuota](../quotas.md#quotas).

### nombre\_usuario/bases de datos {#user-namedatabases}

En esta sección, puede limitar las filas devueltas por ClickHouse para `SELECT` consultas realizadas por el usuario actual, implementando así la seguridad básica a nivel de fila.

**Ejemplo**

La siguiente configuración obliga a que el usuario `user1` sólo puede ver las filas de `table1` como resultado de `SELECT` consultas, donde el valor de la `id` campo es 1000.

``` xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

El `filter` puede ser cualquier expresión que resulte en un [UInt8](../../sql-reference/data-types/int-uint.md)-tipo de valor. Por lo general, contiene comparaciones y operadores lógicos. Filas de `database_name.table1` donde los resultados del filtro a 0 no se devuelven para este usuario. El filtrado es incompatible con `PREWHERE` operaciones y desactiva `WHERE→PREWHERE` optimización.

[Artículo Original](https://clickhouse.tech/docs/en/operations/settings/settings_users/) <!--hide-->
