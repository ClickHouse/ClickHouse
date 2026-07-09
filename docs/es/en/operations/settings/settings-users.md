---
description: 'Configuración para usuarios y roles.'
sidebar_label: 'Configuración de usuario'
sidebar_position: 63
slug: /operations/settings/settings-users
title: 'Configuración de usuarios y roles'
doc_type: 'reference'
---

La sección `users` del archivo de configuración `users.xml` contiene la configuración del usuario.

:::note
ClickHouse también admite un [flujo de trabajo basado en SQL](/es/operations/access-rights#access-control-usage) para la gestión de usuarios. Se recomienda su uso.
:::

Estructura de la sección `users`:

```xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <!-- Exactly one authentication method may be specified at the users.user_name level. For example: -->
        <password></password>
        <!-- Or (exclusive) -->
        <password_sha256_hex></password_sha256_hex>
 
        <!-- Or (exclusive) (N.B. multiple SSH keys are allowed for backwards compatibility) -->
        <ssh_keys>
            <ssh_key>
                <type>ssh-ed25519</type>
                <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ecdsa-sha2-nistp256</type>
                <base64_key>AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBNxeV2uN5UY6CUbCzTA1rXfYimKQA5ivNIqxdax4bcMXz4D0nSk2l5E1TkR5mG8EBWtmExSPbcEPJ8V7lyWWbA8=</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ssh-rsa</type>
                <base64_key>AAAAB3NzaC1yc2EAAAADAQABAAABgQCpgqL1SHhPVBOTFlOm0pu+cYBbADzC2jL41sPMawYCJHDyHuq7t+htaVVh2fRgpAPmSEnLEC2d4BEIKMtPK3bfR8plJqVXlLt6Q8t4b1oUlnjb3VPA9P6iGcW7CV1FBkZQEVx8ckOfJ3F+kI5VsrRlEDgiecm/C1VPl0/9M2llW/mPUMaD65cM9nlZgM/hUeBrfxOEqM11gDYxEZm1aRSbZoY4dfdm3vzvpSQ6lrCrkjn3X2aSmaCLcOWJhfBWMovNDB8uiPuw54g3ioZ++qEQMlfxVsqXDGYhXCrsArOVuW/5RbReO79BvXqdssiYShfwo+GhQ0+aLWMIW/jgBkkqx/n7uKLzCMX7b2F+aebRYFh+/QXEj7SnihdVfr9ud6NN3MWzZ1ltfIczlEcFLrLJ1Yq57wW6wXtviWh59WvTWFiPejGjeSjjJyqqB49tKdFVFuBnIU5u/bch2DXVgiAEdQwUrIp1ACoYPq22HFFAYUJrL32y7RxX3PGzuAv3LOc=</base64_key>
            </ssh_key>
        </ssh_keys>

        <!-- Or (exclusive) for multiple authentication methods: -->
        <auth_methods>
            <method1>
                <password></password>
            </method1>
            <method2>
                <password_sha256_hex></password_sha256_hex>
            </method2>
            <!-- ... -->
            <methodN>
                <!-- ... -->
            </methodN>
        </auth_methods>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>
        <default_database>default</default_database>
        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                </table_name>
            </database_name>
        </databases>

        <grants>
            <query>GRANT SELECT ON system.*</query>
        </grants>
    </user_name>
    <!-- Other users settings -->
</users>
```

<div id="user-namepassword">
  ### user_name/password
</div>

La contraseña puede especificarse en texto sin cifrar o en SHA256 (formato hexadecimal).

* Para asignar una contraseña en texto sin cifrar (**no se recomienda**), colóquela en un elemento `password`.

  Por ejemplo, `<password>qwerty</password>`. La contraseña puede dejarse en blanco.

<a id="password_sha256_hex" />

* Para asignar una contraseña usando su hash SHA256, colóquela en un elemento `password_sha256_hex`.

  Por ejemplo, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

  Ejemplo de cómo generar una contraseña desde la shell:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
  ```

  La primera línea del resultado es la contraseña. La segunda línea es el hash SHA256 correspondiente.

<a id="password_double_sha1_hex" />

* Para mantener la compatibilidad con los clientes de MySQL, la contraseña puede especificarse como un hash doble SHA1. Colóquela en el elemento `password_double_sha1_hex`.

  Por ejemplo, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

  Ejemplo de cómo generar una contraseña desde la shell:

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
  ```

  La primera línea del resultado es la contraseña. La segunda línea es el hash doble SHA1 correspondiente.

<div id="totp-authentication-configuration">
  ### Configuración de la autenticación TOTP
</div>

La contraseña de un solo uso basada en tiempo (TOTP) puede utilizarse para autenticar a los usuarios de ClickHouse mediante la generación de códigos de acceso temporales válidos durante un tiempo limitado.
Este método de autenticación TOTP se ajusta al estándar [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238), lo que lo hace compatible con aplicaciones TOTP populares como Google Authenticator, 1Password y herramientas similares.
Puede configurarse mediante el archivo de configuración `users.xml`, además de la autenticación basada en contraseña.
Todavía no es compatible con el control de acceso basado en SQL.

Para autenticarse mediante TOTP, los usuarios deben proporcionar una contraseña principal junto con una contraseña de un solo uso generada por su aplicación TOTP mediante la opción de línea de comandos `--one-time-password` o concatenándola a la contraseña principal con el carácter &#39;+&#39;.
Por ejemplo, si la contraseña principal es `some_password` y el código TOTP generado es `345123`, el usuario puede especificar `--password some_password+345123` o `--password some_password --one-time-password 345123` al conectarse a ClickHouse. Si no se especifica ninguna contraseña, `clickhouse-client` la solicitará de forma interactiva.

Para habilitar la autenticación TOTP para un usuario, configure la sección `time_based_one_time_password` en `users.xml`. Esta sección define la configuración de TOTP, como el secreto, el período de validez, el número de dígitos y el algoritmo de hash.

**Ejemplo**

````xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- Primary password-based authentication: -->
            <password>some_password</password>
            <password_sha256_hex>1464acd6765f91fccd3f5bf4f14ebb7ca69f53af91b0a5790c2bba9d8819417b</password_sha256_hex>
            <!-- ... or any other supported authentication method ... -->

            <!-- TOTP authentication configuration -->
            <time_based_one_time_password>
                <secret>JBSWY3DPEHPK3PXP</secret>      <!-- Base32-encoded TOTP secret -->
                <period>30</period>                    <!-- Optional: OTP validity period in seconds -->
                <digits>6</digits>                     <!-- Optional: Number of digits in the OTP -->
                <algorithm>SHA1</algorithm>            <!-- Optional: Hash algorithm: SHA1, SHA256, SHA512 -->
            </time_based_one_time_password>
        </my_user>
    </users>
</clickhouse>

Parameters:

- secret - (Required) The base32-encoded secret key used to generate TOTP codes.
- period - Optional. Sets the validity period of each OTP in seconds. Must be a positive number not exceeding 120. Default is 30.
- digits - Optional. Specifies the number of digits in each OTP. Must be between 4 and 10. Default is 6.
- algorithm - Optional. Defines the hash algorithm for generating OTPs. Supported values are SHA1, SHA256, and SHA512. Default is SHA1.

Generating a TOTP Secret

To generate a TOTP-compatible secret for use with ClickHouse, run the following command in the terminal:

```bash
$ base32 -w32 < /dev/urandom | head -1
````

Este comando generará un secreto codificado en base32 que puede añadirse al campo `secret` de users.xml.

Para habilitar TOTP para un usuario específico, añada otra sección `time_based_one_time_password` a cualquier campo existente basado en contraseña (como `password` o `password_sha256_hex`).

La herramienta [qrencode](https://linux.die.net/man/1/qrencode) puede utilizarse para generar un código QR para el secreto de TOTP.

```bash
$ qrencode -t ansiutf8 'otpauth://totp/ClickHouse?issuer=ClickHouse&secret=JBSWY3DPEHPK3PXP'
```

Después de configurar TOTP para un usuario, se puede usar una contraseña de un solo uso como parte del proceso de autenticación, tal como se describió anteriormente.

### username/ssh-key

Esta configuración permite la autenticación con claves SSH.

Dada una clave SSH (como la que genera `ssh-keygen`), como

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```

Se espera que el elemento `ssh_key` sea

```xml
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

Sustituya `ssh-ed25519` por `ssh-rsa` o `ecdsa-sha2-nistp256` en los demás algoritmos compatibles.

### Múltiples métodos de autenticación

Se puede configurar un mismo usuario con varios métodos de autenticación mediante el elemento `<auth_methods>`. Esto permite que un usuario se autentique con cualquiera de los métodos indicados; por ejemplo, un usuario podría tener tanto una contraseña como una credencial LDAP, y el inicio de sesión funcionaría con cualquiera de las dos.

Cada elemento hijo de `<auth_methods>` es un envoltorio con un nombre arbitrario que contiene exactamente un tipo de autenticación. El nombre del envoltorio (p. ej., `<method1>`, `<primary>`, `<a1>`) no importa; solo se utiliza el elemento interno de autenticación.

**Ejemplo: varias contraseñas**

```xml
<users>
    <my_user>
        <auth_methods>
            <primary>
                <password>password_one</password>
            </primary>
            <secondary>
                <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
            </secondary>
        </auth_methods>
    </my_user>
</users>
```

**Ejemplo: tipos de autenticación mixtos**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>plaintext_pass</password>
            </a1>
            <a2>
                <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
            </a2>
            <a3>
                <ldap>
                    <server>my_ldap_server</server>
                </ldap>
            </a3>
        </auth_methods>
    </my_user>
</users>
```

Se admiten los siguientes tipos de autenticación dentro de `<auth_methods>`:

* **`password`** — contraseña en texto sin cifrar
* **`password_sha256_hex`** — hash SHA256 de la contraseña
* **`password_scram_sha256_hex`** — hash SCRAM-SHA-256 de la contraseña
* **`password_double_sha1_hex`** — hash de doble SHA1 de la contraseña
* **`ldap`** — autenticación del servidor LDAP
* **`kerberos`** — autenticación Kerberos
* **`ssl_certificates`** — autenticación mediante certificado SSL
* **`ssh_keys`** — autenticación mediante clave SSH
* **`http_authentication`** — autenticación HTTP

**Reglas y restricciones:**

* `<auth_methods>` **no puede** usarse junto con métodos de autenticación especificados a nivel de usuario. Use uno u otro estilo, no ambos.
* `<auth_methods>` debe contener al menos un método de autenticación.
* Cada elemento envoltorio dentro de `<auth_methods>` debe contener exactamente un tipo de autenticación (con la excepción de `<ssh_keys>`, que puede contener varios por compatibilidad con versiones anteriores).
* TOTP (`<time_based_one_time_password>`) se especifica a nivel de usuario (fuera de `<auth_methods>`) y se aplica a todos los métodos basados en contraseña de la lista. Se requiere al menos un método basado en contraseña cuando TOTP está habilitado.

**Ejemplo: `auth_methods` con TOTP**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>my_password</password>
            </a1>
            <a2>
                <ldap>
                    <server>ldap_server_1</server>
                </ldap>
            </a2>
        </auth_methods>
        <time_based_one_time_password>
            <secret>JBSWY3DPEHPK3PXP</secret>
        </time_based_one_time_password>
    </my_user>
</users>
```

En este ejemplo, la verificación TOTP se aplica al método basado en contraseña (`<password>`), mientras que el método LDAP se autentica de forma independiente en el servidor externo.

### access_management

Esta configuración habilita o deshabilita el uso del [control de acceso y la gestión de cuentas](/es/operations/access-rights#access-control-usage) basados en SQL para el usuario.

Valores posibles:

* 0 — Deshabilitado.
* 1 — Habilitado.

Valor predeterminado: 0.

### grants

Esta configuración permite otorgar cualquier privilegio al usuario seleccionado.
Cada elemento de la lista debe ser una consulta `GRANT` sin especificar ningún destinatario.

Ejemplo:

```xml
<user1>
    <grants>
        <query>GRANT SHOW ON *.*</query>
        <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        <query>GRANT SELECT ON system.*</query>
    </grants>
</user1>
```

Esta configuración no puede especificarse al mismo tiempo que las opciones
`dictionaries`, `access_management`, `named_collection_control`, `show_named_collections_secrets`
y `allow_databases`.

### user_name/networks

Lista de redes desde las que el usuario puede conectarse al servidor de ClickHouse.

Cada elemento de la lista puede tener una de las siguientes formas:

* `<ip>` — Dirección IP o máscara de red.

  Ejemplos: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

* `<host>` — Nombre de host.

  Ejemplo: `example01.host.ru`.

  Para comprobar el acceso, se realiza una consulta DNS y todas las direcciones IP devueltas se comparan con la dirección remota.

* `<host_regexp>` — Expresión regular para nombres de host.

  Ejemplo: `^example\d\d-\d\d-\d\.host\.ru$`

  Para comprobar el acceso, se realiza una [consulta DNS PTR](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) para la dirección remota y, a continuación, se aplica la expresión regular especificada. Después, se realiza otra consulta DNS para los resultados de la consulta PTR y todas las direcciones recibidas se comparan con la dirección remota. Recomendamos encarecidamente que la expresión regular termine con $.

Todos los resultados de las consultas DNS se almacenan en caché hasta que el servidor se reinicie.

**Ejemplos**

Para permitir el acceso del usuario desde cualquier red, especifique:

```xml
<ip>::/0</ip>
```

:::note
No es seguro abrir el acceso desde cualquier red, a menos que tenga un firewall correctamente configurado o que el servidor no esté conectado directamente a Internet.
:::

Para abrir el acceso solo desde localhost, especifique:

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

Puede asignar un perfil de configuración al usuario. Los perfiles de configuración se definen en una sección independiente del archivo `users.xml`. Para obtener más información, consulte [Perfiles de configuración](../../operations/settings/settings-profiles.md).

### user_name/quota

Las cuotas permiten rastrear o limitar el uso de recursos durante un período de tiempo. Las cuotas se configuran en la sección `quotas`
del archivo de configuración `users.xml`.

Puede asignar un conjunto de cuotas al usuario. Para obtener una descripción detallada de la configuración de las cuotas, consulte [Quotas](/es/operations/quotas).

### user_name/databases

En esta sección, puede limitar las filas que ClickHouse devuelve para las consultas `SELECT` realizadas por el usuario actual, implementando así una seguridad básica a nivel de fila.

**Ejemplo**

La siguiente configuración hace que el usuario `user1` solo pueda ver las filas de `table1` como resultado de las consultas `SELECT` en las que el valor del campo `id` es 1000.

```xml
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

El `filter` puede ser cualquier expresión que dé como resultado un valor de tipo [UInt8](../../sql-reference/data-types/int-uint.md). Suele incluir comparaciones y operadores lógicos. Las filas de `database_name.table1` para las que `filter` da como resultado 0 no se devuelven a este usuario. El filtrado es incompatible con las operaciones `PREWHERE` y desactiva la optimización `WHERE→PREWHERE`.

## Roles

Puede crear cualquiera de los roles predefinidos mediante la sección `roles` del archivo de configuración `user.xml`.

Estructura de la sección `roles`:

```xml
<roles>
    <test_role>
        <grants>
            <query>GRANT SHOW ON *.*</query>
            <query>REVOKE SHOW ON system.*</query>
            <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        </grants>
    </test_role>
</roles>
```

Estos roles también pueden asignarse a los usuarios desde la sección `users`:

```xml
<users>
    <user_name>
        ...
        <grants>
            <query>GRANT test_role</query>
        </grants>
    </user_name>
<users>
```