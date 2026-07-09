---
description: 'Guía para configurar la autenticación mediante LDAP en ClickHouse'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

El servidor LDAP puede utilizarse para autenticar a los usuarios de ClickHouse. Hay dos enfoques distintos para hacerlo:

* Usar LDAP como autenticador externo para usuarios existentes, definidos en `users.xml` o en rutas locales de control de acceso.
* Usar LDAP como directorio externo de usuarios y permitir que se autentiquen usuarios no definidos localmente si existen en el servidor LDAP.

Para ambos enfoques, debe definirse en la configuración de ClickHouse un servidor LDAP con un nombre interno, de modo que otras partes de la configuración puedan hacer referencia a él.

<div id="ldap-server-definition">
  ## Definición del servidor LDAP
</div>

Para definir el servidor LDAP, debe añadir la sección `ldap_servers` al archivo `config.xml`.

**Ejemplo**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

Tenga en cuenta que puede definir varios servidores LDAP en la sección `ldap_servers` con nombres distintos.

**Parámetros**

| Parámetro                      | Predeterminado | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------ | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                         | —              | Nombre de host o IP del servidor LDAP. Este parámetro es obligatorio y no puede estar vacío.                                                                                                                                                                                                                                                                                                                                                                                                  |
| `port`                         | `636` / `389`  | Puerto del servidor LDAP. De forma predeterminada, es `636` si `enable_tls` está establecido en `yes`; en caso contrario, `389`.                                                                                                                                                                                                                                                                                                                                                              |
| `bind_dn`                      | —              | Plantilla utilizada para construir el DN con el que hacer bind. El DN resultante se construirá reemplazando todas las subcadenas `{user_name}` de la plantilla por el nombre de usuario real durante cada intento de autenticación.                                                                                                                                                                                                                                                           |
| `auth_dn_prefix`               | —              | **Obsoleto.** Una alternativa a `bind_dn`. No puede usarse junto con `bind_dn`. Cuando se especifica, el bind DN se construye como `auth_dn_prefix + {user_name} + auth_dn_suffix`. Por ejemplo, establecer `auth_dn_prefix` en `uid=` y `auth_dn_suffix` en `,ou=users,dc=example,dc=com` equivale a establecer `bind_dn` en `uid={user_name},ou=users,dc=example,dc=com`.                                                                                                                   |
| `auth_dn_suffix`               | —              | **Obsoleto.** Consulte `auth_dn_prefix`.                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `verification_cooldown`        | `0`            | Período de tiempo, en segundos, tras un intento de bind correcto, durante el cual se asumirá que el usuario está autenticado correctamente para todas las solicitudes consecutivas sin contactar con el servidor LDAP. Especifique `0` para deshabilitar el almacenamiento en caché y forzar el contacto con el servidor LDAP en cada solicitud de autenticación.                                                                                                                             |
| `follow_referrals`             | `false`        | Un indicador para permitir que la biblioteca client de LDAP siga automáticamente las referencias LDAP devueltas por el servidor. Es especialmente relevante en entornos de Microsoft Active Directory, donde las búsquedas de subárbol en un base DN de nivel superior (por ejemplo, `DC=example,DC=com`) pueden devolver referencias/referencias de búsqueda (por ejemplo, `DC=DomainDnsZones,...`). Establézcalo en `true` solo cuando necesite explícitamente búsquedas entre particiones. |
| `enable_tls`                   | `yes`          | Un indicador para activar el uso de una secure connection con el servidor LDAP. Especifique `no` para el protocolo `ldap://` en texto plano (no recomendado), `yes` para el protocolo LDAP sobre SSL/TLS `ldaps://` (recomendado), o `starttls` para el protocolo heredado StartTLS (protocolo `ldap://` en texto plano, actualizado a TLS).                                                                                                                                                  |
| `tls_minimum_protocol_version` | `tls1.2`       | La versión mínima del protocolo SSL/TLS. Valores aceptados: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`.                                                                                                                                                                                                                                                                                                                                                                                     |
| `tls_require_cert`             | `demand`       | Comportamiento de la verificación del certificado del par SSL/TLS. Valores aceptados: `never`, `allow`, `try`, `demand`.                                                                                                                                                                                                                                                                                                                                                                      |
| `tls_cert_file`                | —              | Ruta al archivo del certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `tls_key_file`                 | —              | Ruta al archivo de la clave del certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `tls_ca_cert_file`             | —              | Ruta al archivo del certificado de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `tls_ca_cert_dir`              | —              | Ruta al directorio que contiene los certificados de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `tls_cipher_suite`             | —              | Conjunto de cifrado permitido (en notación OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `search_limit`                 | `256`          | Número máximo de entradas que pueden devolver las consultas de búsqueda LDAP realizadas por esta definición de servidor (para la detección del user DN y la asignación de roles).                                                                                                                                                                                                                                                                                                               |

**Subparámetros de `user_dn_detection`**

Sección con parámetros de búsqueda LDAP para detectar el user DN real del usuario autenticado con bind. Esto se utiliza principalmente en filtros de búsqueda para una asignación de roles posterior cuando el servidor es Active Directory. El user DN resultante se usará al reemplazar las subcadenas `{user_dn}` dondequiera que estén permitidas. De forma predeterminada, el user DN se establece igual al bind DN, pero, una vez realizada la búsqueda, se actualizará con el valor real del user DN detectado.

| Parámetro       | Predeterminado | Descripción                                                                                                                                                                                                                                                                                                                                                               |
| --------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —              | Plantilla utilizada para construir el base DN para la búsqueda LDAP. El DN resultante se construirá reemplazando todas las subcadenas `{user_name}` y `{bind_dn}` de la plantilla por el nombre de usuario real y el bind DN durante la búsqueda LDAP.                                                                                                                        |
| `scope`         | `subtree`      | Alcance de la búsqueda LDAP. Valores aceptados: `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                                                                                                                                 |
| `search_filter` | —              | Plantilla utilizada para construir el search filter para la búsqueda LDAP. El filtro resultante se construirá reemplazando todas las subcadenas `{user_name}`, `{bind_dn}` y `{base_dn}` de la plantilla por el nombre de usuario real, el bind DN y el base DN durante la búsqueda LDAP. Tenga en cuenta que los caracteres especiales deben escaparse correctamente en XML. |

<div id="ldap-external-authenticator">
  ## Autenticador externo LDAP
</div>

Se puede usar un servidor LDAP remoto como método para verificar las contraseñas de usuarios definidos localmente (usuarios definidos en `users.xml` o en las rutas locales de control de acceso). Para ello, especifique el nombre de un servidor LDAP definido previamente en lugar de `password` o secciones similares en la definición del usuario.

En cada intento de inicio de sesión, ClickHouse intenta hacer &quot;bind&quot; con el DN especificado por el parámetro `bind_dn` en la [definición del servidor LDAP](#ldap-server-definition) usando las credenciales proporcionadas y, si lo consigue, el usuario se considera autenticado. Esto suele denominarse método de &quot;simple bind&quot;.

**Ejemplo**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

Tenga en cuenta que el usuario `my_user` hace referencia a `my_ldap_server`. Este servidor LDAP debe configurarse en el archivo principal `config.xml`, como se describió anteriormente.

Cuando está habilitado el [Control de acceso y gestión de cuentas](/es/operations/access-rights#access-control-usage) basado en SQL, los usuarios autenticados mediante servidores LDAP también pueden crearse con la sentencia [CREATE USER](/es/sql-reference/statements/create/user).

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## Directorio externo de usuarios LDAP
</div>

Además de los usuarios definidos localmente, se puede usar un servidor LDAP remoto como fuente de definiciones de usuarios. Para ello, especifique el nombre del servidor LDAP definido previamente (consulte [Definición del servidor LDAP](#ldap-server-definition)) en la sección `ldap`, dentro de la sección `users_directories` del archivo `config.xml`.

En cada intento de inicio de sesión, ClickHouse intenta encontrar la definición del usuario localmente y autenticarlo de la manera habitual. Si el usuario no está definido, ClickHouse asumirá que la definición existe en el directorio LDAP externo e intentará hacer &quot;bind&quot; con el DN especificado en el servidor LDAP usando las credenciales proporcionadas. Si lo consigue, se considerará que el usuario existe y está autenticado. Al usuario se le asignarán los roles de la lista especificada en la sección `roles`. Además, se puede realizar una &quot;búsqueda&quot; LDAP, y los resultados pueden transformarse y tratarse como nombres de roles para luego asignárselos al usuario si la sección `role_mapping` también está configurada. Todo esto implica que el [Control de acceso y gestión de cuentas](/es/operations/access-rights#access-control-usage) basado en SQL está habilitado y que los roles se crean mediante la sentencia [CREATE ROLE](/es/sql-reference/statements/create/role).

**Ejemplo**

Va en `config.xml`.

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

Tenga en cuenta que `my_ldap_server`, al que se hace referencia en la sección `ldap` dentro de la sección `user_directories`, debe ser un servidor LDAP definido previamente y configurado en `config.xml` (consulte [Definición del servidor LDAP](#ldap-server-definition)).

**Parámetros**

| Parámetro | Predeterminado | Descripción                                                                                                                                                                                                                                                                                   |
| --------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server`  | —              | Uno de los nombres de servidores LDAP definidos anteriormente en la sección de configuración `ldap_servers`. Este parámetro es obligatorio y no puede estar vacío.                                                                                                                            |
| `roles`   | —              | Sección con una lista de roles definidos localmente que se asignarán a cada usuario obtenido del servidor LDAP. Si aquí no se especifica ningún rol ni se asigna ninguno durante la asignación de roles (más abajo), el usuario no podrá realizar ninguna acción después de la autenticación. |

**Subparámetros de `role_mapping`**

Sección con parámetros de búsqueda LDAP y reglas de asignación. Cuando un usuario se autentica, mientras sigue vinculado a LDAP, se realiza una búsqueda LDAP mediante `search_filter` y el nombre del usuario autenticado. Para cada entrada encontrada durante esa búsqueda, se extrae el valor del atributo especificado. Para cada valor de atributo que tenga el prefijo especificado, se elimina ese prefijo y el resto del valor pasa a ser el nombre de un rol local definido en ClickHouse, que se espera que se haya creado previamente mediante la sentencia [CREATE ROLE](/es/sql-reference/statements/create/role). Puede haber varias secciones `role_mapping` definidas dentro de la misma sección `ldap`. Todas se aplicarán.

| Parámetro       | Predeterminado | Descripción                                                                                                                                                                                                                                                                                                                                                                                     |
| --------------- | -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —              | Template utilizado para construir el base DN de la búsqueda LDAP. El DN resultante se construirá reemplazando todas las subcadenas `{user_name}`, `{bind_dn}` y `{user_dn}` de la Template por el nombre de usuario real, el bind DN y el user DN en cada búsqueda LDAP.                                                                                                                        |
| `scope`         | `subtree`      | Alcance de la búsqueda LDAP. Valores aceptados: `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                                                                                                                                                     |
| `search_filter` | —              | Template utilizado para construir el search filter de la búsqueda LDAP. El filtro resultante se construirá reemplazando todas las subcadenas `{user_name}`, `{bind_dn}`, `{user_dn}` y `{base_dn}` de la Template por el nombre de usuario real, el bind DN, el user DN y el base DN en cada búsqueda LDAP. Tenga en cuenta que los caracteres especiales deben escaparse correctamente en XML. |
| `attribute`     | `cn`           | Nombre del atributo cuyos valores devolverá la búsqueda LDAP.                                                                                                                                                                                                                                                                                                                                   |
| `prefix`        | vacío          | Prefijo esperado al inicio de cada cadena de la lista original de cadenas devueltas por la búsqueda LDAP. El prefijo se eliminará de las cadenas originales y las cadenas resultantes se tratarán como nombres de roles locales.                                                                                                                                                                |