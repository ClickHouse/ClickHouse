---
description: 'Guía de autenticación mediante JWT y usuarios efímeros en ClickHouse Cloud'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'Autenticación JWT'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

ClickHouse puede autenticar usuarios mediante JSON Web Tokens (JWT). A diferencia de otros autenticadores externos, como [LDAP](/es/operations/external-authenticators/ldap) o [Kerberos](/es/operations/external-authenticators/kerberos), la autenticación con JWT no verifica la identidad de usuarios ya existentes. En su lugar, crea dinámicamente **usuarios efímeros** a partir de las claims incluidas en cada token. Estos usuarios existen solo en memoria, reciben derechos de acceso derivados de las claims del token y se eliminan automáticamente cuando el token caduca.

Esto hace que la autenticación con JWT sea fundamentalmente distinta de los métodos basados en contraseña o en certificados: no existe ninguna instrucción `CREATE USER ... IDENTIFIED WITH jwt`, e intentarlo genera una excepción. Los usuarios JWT se gestionan por completo mediante el ciclo de vida del token.

<div id="overview">
  ## Descripción general
</div>

El flujo de autenticación funciona de la siguiente manera:

1. Un `client` presenta un JWT firmado mediante uno de los mecanismos de transporte compatibles (el encabezado HTTP `Authorization: Bearer`, el protocolo nativo TCP o el campo `jwt` de gRPC).
2. ClickHouse valida la firma del token.
3. Se verifican los claims obligatorios (`exp`, `iat`, `iss`, `sub`, `aud`).
4. Se crea en memoria un usuario efímero con derechos de acceso derivados de los claims `clickhouse:grants` y `clickhouse:roles` del token, tras intersectarlos con un límite de permisos.
5. Cuando el token expira, una tarea de recolección de basura en segundo plano elimina al usuario.

<div id="token-claims">
  ## Claims del token
</div>

<div id="required-claims">
  ### Claims obligatorios
</div>

Todos los JWT presentados a ClickHouse deben contener los siguientes claims:

| Claim | Descripción                                                                                      |
| ----- | ------------------------------------------------------------------------------------------------ |
| `alg` | Algoritmo de firma (claim del header). Valores admitidos: `HS256`, `RS256`, `ES256`.             |
| `exp` | Hora de expiración. Establece el `valid_until` del usuario efímero.                              |
| `iat` | Hora de emisión. Se usa para evitar la reutilización de tokens antiguos para la misma identidad. |
| `iss` | Emisor. Se compara con el emisor esperado del proveedor.                                         |
| `sub` | Subject. Pasa a formar parte del username generado.                                              |
| `aud` | Audiencia. Se compara con la audiencia esperada del proveedor.                                   |

El claim `kid` (ID de clave) del header también es obligatorio cuando se usa la resolución de claves basada en JWKS.

:::note El modo JWKS solo admite claves RSA
Mientras que los proveedores con clave estática aceptan `HS256`, `RS256` o `ES256`, los proveedores basados en JWKS solo aceptan JWK cuyo `kty` sea `RSA` (es decir, tokens firmados con `RS256`). Los tokens firmados con claves HMAC (`HS256`) o EC (`ES256`) no pueden verificarse con un endpoint JWKS y se rechazarán.
:::

<div id="other-recognized-claims">
  ### Otras claims reconocidas
</div>

| Claim | Descripción                                                                                                                             |
| ----- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `nbf` | Momento a partir del cual es válido. Este claim no es obligatorio, pero, si está presente, los tokens se rechazan antes de ese momento. |
| `jti` | Reservado. Se acepta en los tokens, pero actualmente no se valida ni se utiliza.                                                        |

<div id="optional-claims">
  ### Claims opcionales
</div>

| Claim                                                                                                                                                                | Nombre predeterminado | Descripción                                                                                                                                                         |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Grants                                                                                                                                                               | `clickhouse:grants`   | Un array JSON de fragmentos SQL `GRANT`, por ejemplo `["SELECT ON db.*", "INSERT ON db.table1"]`. Cada elemento se analiza como el cuerpo de una sentencia `GRANT`. |
| Roles                                                                                                                                                                | `clickhouse:roles`    | Un array JSON de nombres de roles para asignar, por ejemplo `["analyst", "reader"]`.                                                                                |
| Los nombres predeterminados de los claims se pueden remapear a nombres de claims personalizados si tu proveedor de identidad usa una convención de nombres distinta. |                       |                                                                                                                                                                     |

<div id="example-token-header-and-payload">
  ### Ejemplo del encabezado y la carga útil de un token
</div>

```json
{
  "alg": "RS256",
  "kid": "my-key-id"
}
```

```json
{
  "iss": "https://idp.example.com",
  "sub": "jane.doe",
  "aud": "my-clickhouse-cluster",
  "exp": 1719504000,
  "iat": 1719500400,
  "clickhouse:grants": ["SELECT ON analytics.*", "INSERT ON analytics.events"],
  "clickhouse:roles": ["analyst"]
}
```

<div id="ephemeral-user-behavior">
  ## Comportamiento del usuario efímero
</div>

Los usuarios JWT se diferencian de los usuarios normales de ClickHouse en varios aspectos importantes.

<div id="identity-and-naming">
  ### Identidad y nombres
</div>

Cada usuario de JWT recibe un UUID determinístico calculado a partir de las claims `iss`, `sub` y `aud`. Este UUID es **estable** entre sesiones de inicio de sesión. Un usuario que inicia sesión varias veces con distintos tokens (pero con el mismo emisor, subject y audiencia) siempre obtiene el mismo UUID.

Sin embargo, el nombre de usuario es **volátil**. Se construye de la siguiente manera:

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

La parte `<claims_hash>` cambia cada vez que cambian las claims `clickhouse:roles` o `clickhouse:grants`. Esto significa que los tokens con distintos conjuntos de roles o de grants generan nombres de usuario distintos incluso para la misma identidad.

<div id="access-rights">
  ### Derechos de acceso
</div>

Los derechos de acceso efectivos se calculan de la siguiente manera:

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

Donde `permission_limit` es el conjunto de derechos de acceso que posee un rol o un usuario de referencia configurado como límite superior. Los derechos solicitados por el token que superen ese límite se descartan sin notificación.

<div id="token-freshness">
  ### Vigencia del token
</div>

ClickHouse registra el claim `iat` (issued-at) del token autenticado más recientemente para cada identidad estable. Si se presenta un token con un `iat` igual o anterior al valor almacenado, el servidor reutiliza el usuario efímero existente sin volver a evaluar los claims. Esto evita que los tokens más antiguos reduzcan los permisos de un usuario.

<div id="lifetime-and-garbage-collection">
  ### Ciclo de vida y recolección de basura
</div>

Los usuarios efímeros se crean cuando se autentica un token por primera vez, y una tarea de recolección de basura en segundo plano los elimina después de que venza `valid_until` (derivado de `exp`). El intervalo de GC se controla mediante el parámetro `gc_interval` (predeterminado: 5 minutos).

Entre ejecuciones de GC, los usuarios expirados pueden seguir siendo visibles en `system.users`, pero ya no pueden autenticarse.

<div id="persistent-access-assignments">
  ### Asignaciones de acceso persistentes
</div>

Dado que el UUID es estable, puede asignar perfiles de configuración, cuotas, políticas de filas y políticas de enmascaramiento de columnas a un usuario JWT mediante sentencias SQL. Estas asignaciones persisten en el almacenamiento de control de acceso (en disco o en ZooKeeper) y se conservan tras el vencimiento del token y la nueva autenticación.

Haga referencia al usuario por su nombre de usuario actual:

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
El nombre de usuario y el UUID de una identidad concreta pueden encontrarse en las columnas `name` e `id` de `system.users` mientras ese usuario esté activo.
:::

Ten en cuenta que `ALTER USER` no funciona directamente con usuarios JWT, ya que son de solo lectura. Para asignar perfiles de configuración, cuotas o políticas, usa las sentencias `ALTER SETTINGS PROFILE`, `ALTER QUOTA` o `ALTER ROW POLICY`, como se muestra anteriormente.

<div id="differences-from-regular-users">
  ## Diferencias con los usuarios convencionales
</div>

| Funcionalidad                         | Usuarios JWT                                                           | Usuarios convencionales                           |
| ------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------- |
| Creación                              | Automática a partir de los claims del token                            | Sentencia `CREATE USER`                           |
| Almacenamiento                        | Solo en memoria (efímero)                                              | Disco, ZooKeeper o archivo de configuración       |
| `CREATE USER ... IDENTIFIED WITH jwt` | No admitido (genera una excepción)                                     | Se admiten todos los demás tipos de autenticación |
| `ALTER USER` / `DROP USER`            | No admitido                                                            | Admitido                                          |
| Copia de seguridad y restauración     | No incluido                                                            | Incluido                                          |
| Nombre de usuario                     | Generado automáticamente, volátil                                      | Elegido por el administrador, fijo                |
| UUID                                  | Determinista a partir de `iss`+`sub`+`aud`                             | Aleatorio en el momento de la creación            |
| Tiempo de vida                        | Limitado por `exp` del token                                           | Hasta que se elimine explícitamente               |
| Derechos de acceso                    | Derivados de los claims del token, limitados por el límite de permisos | Otorgados explícitamente mediante `GRANT`         |
| Restricciones de host                 | Configuración de red por proveedor                                     | Cláusula `HOST` por usuario                       |
| Perfiles de configuración             | Asignables por UUID (persistentes)                                     | Configurables directamente                        |
| Cuotas y políticas de fila            | Asignables por UUID (persistentes)                                     | Configurables directamente                        |
| Roles predeterminados                 | No configurables                                                       | Configurables                                     |

<div id="sql-security-definer-views">
  ## Vistas con SQL SECURITY DEFINER
</div>

Cuando un usuario efímero de JWT crea una vista con `SQL SECURITY DEFINER`, el servidor crea automáticamente una copia persistente oculta del usuario para que actúe como definidor de la vista. Este usuario oculto:

* Tiene el nombre `<original_jwt_username>:definer`
* Tiene `NO_AUTHENTICATION` (no se puede usar para iniciar sesión)
* Conserva los mismos derechos de acceso que el usuario JWT original en el momento en que se creó la vista

Esto garantiza que la vista siga funcionando después de que caduque el token del usuario efímero y de que el usuario original se elimine mediante recolección de basura.

<div id="client-usage">
  ## Uso de Client
</div>

<div id="passing-token-directly">
  ### Pasar un token directamente
</div>

Usa el indicador `--jwt` con `clickhouse-client` para autenticarte con un token obtenido de antemano:

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
El indicador `--jwt` es mutuamente excluyente con `--user`. Cuando se especifica `--jwt`, el nombre de usuario se obtiene del token.
:::

<div id="http-interface">
  ### Interfaz HTTP
</div>

Envía el token como token Bearer en el encabezado `Authorization`:

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
Envía siempre los JWT a través de HTTPS. Un token Bearer enviado por HTTP sin cifrar queda expuesto a cualquiera en la ruta de la red y equivale a filtrar la credencial.
:::

<div id="oauth2-device-code-login">
  ### Inicio de sesión con código de dispositivo de OAuth2
</div>

El `clickhouse-client` admite un flujo interactivo de código de dispositivo de OAuth2 mediante el indicador `--login`. Para los endpoints de ClickHouse Cloud, el client realiza automáticamente el intercambio de tokens para obtener un JWT específico de ClickHouse. Los tokens se renuevan de forma transparente durante la sesión. Cuando se obtiene un token nuevo, el client se reconecta automáticamente.

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

<div id="clickhouse-cloud-built-in">
  ## Autenticador JWT integrado de ClickHouse Cloud
</div>

Cada servicio de ClickHouse Cloud incluye un autenticador JWT predefinido que usan SQL Console y el flujo `--login` de `clickhouse-client`. Este autenticador está configurado con:

| Parámetro        | Valor                                                                      |
| ---------------- | -------------------------------------------------------------------------- |
| `iss` (emisor)   | `ClickHouse`                                                               |
| `aud` (audiencia) | El UUID del servicio (visible en la URL de la consola de ClickHouse Cloud) |
| `sub` (subject)  | La dirección de correo electrónico de su cuenta de ClickHouse Cloud        |

El autenticador integrado tiene un límite de permisos establecido en el rol `default_role` y el usuario `default`. Esto significa que los derechos efectivos de cualquier usuario JWT resultan de intersectar los grants de esas dos entidades, por lo que un token nunca puede escalar privilegios más allá de lo que `default_role` y `default` tienen permitido hacer.

No necesita configurar nada para usar este autenticador. Se aprovisiona automáticamente cuando se crea el servicio.

<div id="interserver-communication">
  ## Comunicación entre servidores
</div>

Cuando una consulta se redirige a otro segmento o réplica, el token JWT se incluye en el protocolo de comunicación entre servidores. El nodo remoto vuelve a autenticar el token de forma independiente y crea su propio usuario efímero.

<div id="troubleshooting">
  ## Solución de problemas
</div>

* **No se han concedido derechos de acceso:** Es posible que el rol o usuario al que se hace referencia no tenga los grants necesarios. Asegúrese de que los roles indicados en `clickhouse:roles` existan e incluyan los grants adecuados.
* **Token rechazado:** Verifique que `iss`, `aud` y el algoritmo de firma de su token coincidan con lo que espera el proveedor de JWT. Si se usa JWKS, asegúrese de que el `kid` del token coincida con una clave del conjunto de claves del proveedor.
* **El usuario desaparece entre consultas:** Los usuarios efímeros se eliminan cuando el token vence. Use un client que admita la actualización del token (por ejemplo, el modo `--login`) para sesiones de larga duración.
* **`CREATE USER ... IDENTIFIED WITH jwt` falla:** Esto es normal. Los usuarios JWT no pueden crearse mediante DDL. Su gestión depende por completo del ciclo de vida del token.