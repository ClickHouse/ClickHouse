---
slug: /en/operations/external-authenticators/jwt
---
# JWT
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Existing and properly configured ClickHouse users can be authenticated via JWT.

Currently, JWT can only be used as an external authenticator for existing users, which are defined in `users.xml` or in local access control paths. 

The username will be extracted from the JWT after validating the token expiration and against the signature. Signature can be validated by:
- static public key
- static JWKS
- received from the JWKS servers
Additionally, each user can be verified using a JWT payload. In this case, after running general checks, the occurrence of CLAIMS from the user settings in the JWT payload is checked.

For this approach, JWT validators must be configured in the system and must be enabled in ClickHouse config.


## Enabling JWT validators in ClickHouse {#enabling-jwt-validators-in-clickhouse}

To enable JWT validators, one should include `jwt_verifiers` section in `config.xml`. This section may contain several JWT verifiers, minimum is 1.

### Verifying JWT signature using static key {$verifying-jwt-signature-using-static-key}

**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwt_verifiers>
        <basic_jwt_validator>
          <algo>HS256</algo>
          <single_key>c3VwZXIKa2V5</single_key>
          <single_key_in_base64>true</single_key_in_base64>
        </basic_jwt_validator>
    </jwt_verifiers>
</clickhouse>
```

#### Parameters:

- `algo` - Algorithm for validate signature. Supported:

  | HMSC  | RSA   | ECDSA  | PSS   | EdDSA   |
  | ----- | ----- | ------ | ----- | ------- |
  | HS256 | RS256 | ES256  | PS256 | Ed25519 |
  | HS384 | RS384 | ES384  | PS384 | Ed448   |
  | HS512 | RS512 | ES512  | PS512 |         |
  |       |       | ES256K |       |         |
  Also support None.
- `single_key` - key for HS* algorithms. Required in these algorithms.
- `single_key_in_base64` - a sign that the `single_key` key is encoded in base64
- `public_key` - public key for validate in all algorithms except HS* family and None. Required in these algorithms.
- `private_key` - private key for validate in all algorithms except HS* family and None. Optional in these algorithms.
- `public_key_password` - public key password for verification in all algorithms except the HS* family and None. Optional in these algorithms.
- `private_key_password` - private key password for verification in all algorithms except the HS* family and None. Optional in these algorithms.

### Verifying JWT signature using static JWKS {$verifying-jwt-signature-using-static-jwks}

**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwt_verifiers>
        <basic_jwt_validator>
          <static_jwks>CONTENT_OF_JWKS</static_jwks>
        </basic_jwt_validator>
    </jwt_verifiers>
</clickhouse>
```

#### Parameters:
- `static_jwks` - content of JWKS in json
- `static_jwks_file` - path to file with JWKS

:::note
Supported only RS* family algorithms
:::

:::note
Only one of `static_jwks` or `static_jwks_file` keys must be present in one verifier
:::

### Verifying JWT signature using JWKS servers {$verifying-jwt-signature-using-static-jwks}

**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwt_verifiers>
        <basic_auth_server>
          <uri>http://localhost:8000/jwks.json</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <refresh_ms>300000</refresh_ms>
        </basic_auth_server>
    </jwt_verifiers>
</clickhouse>
```

#### Parameters:

- `uri` - URI for making authentication request
- `refresh_ms` - Period for resend request for refreshing JWKS. Default: 300000 ms.

Timeouts in milliseconds on the socket used for communicating with the server:
- `connection_timeout_ms` - Default: 1000 ms.
- `receive_timeout_ms` - Default: 1000 ms.
- `send_timeout_ms` - Default: 1000 ms.

Retry parameters:
- `max_tries` - The maximum number of attempts to make an authentication request. Default: 3
- `retry_initial_backoff_ms` - The backoff initial interval on retry. Default: 50 ms
- `retry_max_backoff_ms` - The maximum backoff interval. Default: 1000 ms

Note, that you can define multiple HTTP servers inside the `jwt_verifiers` section using distinct names.

### Enabling JWT authentication in `users.xml` {#enabling-jwt-auth-in-users-xml}

In order to enable HTTP authentication for the user, specify `jwt` section instead of `password` or similar sections in the user definition.

Parameters:
- `claims` - An optional string containing a json object that should be contained in the token payload.

Example (goes into `users.xml`):
```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <jwt>
            <claims>{"resource_access":{"account": {"roles": ["view-profile"]}}}</claims>
        </jwt>
    </my_user>
</clickhouse>
```

In this sample payload must contains "view-profile" in array on path resource_access.account.roles like

```
{
...
  "realm_access": {
    "roles": [
      "default-roles-master",
      "offline_access",
      "uma_authorization"
    ]
  },
...  
}
```

:::note
Note that HTTP authentication cannot be used alongside with any other authentication mechanism. The presence of any other sections like `password` alongside `http_authentication` will force ClickHouse to shutdown.
:::

### Enabling JWT authentication using SQL {#enabling-jwt-auth-using-sql}

When [SQL-driven Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled in ClickHouse, users identified by JWT authentication can also be created using SQL statements.

```sql
CREATE USER my_user IDENTIFIED WITH jwt CLAIMS '{"resource_access":{"account": {"roles": ["view-profile"]}}}'
```

...or, without additional JWT payload checks

```sql
CREATE USER my_user IDENTIFIED WITH jwt
```

## JWT authorization examples {#jwt-authorization-examples}

#### Console client

```
clickhouse-client -jwt <token>
```

#### HTTP requests

```
curl 'http://localhost:8080/?add_http_cors_header=1&default_format=JSONCompact&max_result_rows=1000&max_result_bytes=10000000&result_overflow_mode=break' \
 -H 'Authorization: Bearer <TOKEN>' \
 -H 'Content type: text/plain;charset=UTF-8' \
 --data-raw 'SELECT current_user()'
```
:::note
The token can be obtained (by priority):
- header X-ClickHouse-JWT-Token
- header Authorization
- request parameter "token". In this case, the "Bearer" prefix should not exist.
:::

### Passing session settings {#passing-session-settings}

If `settings_key` exists in the `jwt_verifiers` section or exists in the verifier section and the payload contains a sub-object of that `settings_key`, ClickHouse will attempt to parse its key:value pairs as string values ​​and set them as session settings for the currently authenticated user. If parsing fails, the JWT payload will be ignored.

The `settings_key` in the verifier section takes precedence over the `settings_key` from the `jwt_verifiers` section. If `settings_key` in the verifier section does not exist, the `settings_key` from the `jwt_verifiers` section will be used.
