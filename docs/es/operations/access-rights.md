---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 48
toc_title: Derechos de acceso
---

# Derechos De Acceso {#access-rights}

Los usuarios y los derechos de acceso se configuran en la configuración del usuario. Esto suele ser `users.xml`.

Los usuarios se registran en el `users` apartado. Aquí hay un fragmento de la `users.xml` file:

``` xml
<!-- Users and ACL. -->
<users>
    <!-- If the user name is not specified, the 'default' user is used. -->
    <default>
        <!-- Password could be specified in plaintext or in SHA256 (in hex format).

             If you want to specify password in plaintext (not recommended), place it in 'password' element.
             Example: <password>qwerty</password>.
             Password could be empty.

             If you want to specify SHA256, place it in 'password_sha256_hex' element.
             Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>

             How to generate decent password:
             Execute: PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
             In first line will be password and in second - corresponding SHA256.
        -->
        <password></password>

        <!-- A list of networks that access is allowed from.
            Each list item has one of the following forms:
            <ip> The IP address or subnet mask. For example: 198.51.100.0/24 or 2001:DB8::/32.
            <host> Host name. For example: example01. A DNS query is made for verification, and all addresses obtained are compared with the address of the customer.
            <host_regexp> Regular expression for host names. For example, ^example\d\d-\d\d-\d\.host\.ru$
                To check it, a DNS PTR request is made for the client's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.host\.ru$.

            If you are installing ClickHouse yourself, specify here:
                <networks>
                        <ip>::/0</ip>
                </networks>
        -->
        <networks incl="networks" />

        <!-- Settings profile for the user. -->
        <profile>default</profile>

        <!-- Quota for the user. -->
        <quota>default</quota>
    </default>

    <!-- For requests from the Yandex.Metrica user interface via the API for data on specific counters. -->
    <web>
        <password></password>
        <networks incl="networks" />
        <profile>web</profile>
        <quota>default</quota>
        <allow_databases>
           <database>test</database>
        </allow_databases>
        <allow_dictionaries>
           <dictionary>test</dictionary>
        </allow_dictionaries>
    </web>
</users>
```

Puede ver una declaración de dos usuarios: `default`y`web`. Hemos añadido el `web` usuario por separado.

El `default` usuario se elige en los casos en que no se pasa el nombre de usuario. El `default` usuario también se utiliza para el procesamiento de consultas distribuidas, si la configuración del servidor o clúster no `user` y `password` (véase la sección sobre el [Distribuido](../engines/table-engines/special/distributed.md) motor).

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

La contraseña se especifica en texto sin cifrar (no recomendado) o en SHA-256. El hash no está salado. En este sentido, no debe considerar estas contraseñas como proporcionar seguridad contra posibles ataques maliciosos. Más bien, son necesarios para la protección de los empleados.

Se especifica una lista de redes desde las que se permite el acceso. En este ejemplo, la lista de redes para ambos usuarios se carga desde un archivo independiente (`/etc/metrika.xml`) que contiene el `networks` sustitución. Aquí hay un fragmento de eso:

``` xml
<yandex>
    ...
    <networks>
        <ip>::/64</ip>
        <ip>203.0.113.0/24</ip>
        <ip>2001:DB8::/32</ip>
        ...
    </networks>
</yandex>
```

Puede definir esta lista de redes directamente en `users.xml` o en un archivo en el `users.d` (para obtener más información, consulte la sección “[Archivos de configuración](configuration-files.md#configuration_files)”).

La configuración incluye comentarios que explican cómo abrir el acceso desde todas partes.

Para su uso en producción, sólo especifique `ip` elementos (direcciones IP y sus máscaras), ya que usan `host` y `hoost_regexp` podría causar latencia adicional.

A continuación se especifica el perfil de configuración de usuario (consulte la sección “[Perfiles de configuración](settings/settings-profiles.md)”. Puede especificar el perfil predeterminado, `default'`. El perfil puede tener cualquier nombre. Puede especificar el mismo perfil para diferentes usuarios. Lo más importante que puede escribir en el perfil de configuración es `readonly=1`, que asegura el acceso de sólo lectura. A continuación, especifique la cuota que se utilizará (consulte la sección “[Cuota](quotas.md#quotas)”). Puede especificar la cuota predeterminada: `default`. It is set in the config by default to only count resource usage, without restricting it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

En el opcional `<allow_databases>` sección, también puede especificar una lista de bases de datos a las que el usuario puede acceder. De forma predeterminada, todas las bases de datos están disponibles para el usuario. Puede especificar el `default` base. En este caso, el usuario recibirá acceso a la base de datos de forma predeterminada.

En el opcional `<allow_dictionaries>` sección, también puede especificar una lista de diccionarios a los que el usuario puede acceder. De forma predeterminada, todos los diccionarios están disponibles para el usuario.

Acceso a la `system` base de datos siempre está permitida (ya que esta base de datos se utiliza para procesar consultas).

El usuario puede obtener una lista de todas las bases de datos y tablas en ellos mediante el uso de `SHOW` consultas o tablas del sistema, incluso si no se permite el acceso a bases de datos individuales.

El acceso a la base de datos no está [sólo lectura](settings/permissions-for-queries.md#settings_readonly) configuración. No puede conceder acceso completo a una base de datos y `readonly` acceso a otro.

[Artículo Original](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
