---
slug: /en/operations/external-authenticators/oauth
title: "Token-based authentication"
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse users can be authenticated using tokens. This works in two ways:

- An existing user (defined in `users.xml` or in local access control paths) can be authenticated with a token if this user can be `IDENTIFIED WITH jwt`. 
- Use the information from the token or from an external Identity Provider (IdP) as a source of user definitions and allow locally undefined users to be authenticated with a valid token.

Although not all tokens are JWTs, under the hood both ways are treated as the same authentication method to maintain better compatibility.

# Token Processors

## Configuration
To use token-based authentication, add `token_processors` section to `config.xml` and define at least one token processor in it.
Its contents are different for different token processor types.

**Common parameters**
- `type` -- type of token processor. Supported values: "JWT", "Azure", "OpenID". Mandatory. Case-insensitive.
- `token_cache_lifetime` -- maximum lifetime of cached token (in seconds). Optional, default: 3600.
- `username_claim` -- name of claim (field) that will be treated as ClickHouse username. Optional, default: "sub".
- `groups_claim` -- name of claim (field) that contains list of groups user belongs to. This claim will be looked up in the token itself (in case token is a valid JWT, e.g. in Keycloak) or in response from `/userinfo`. Optional, default: "groups".

For each type, there are additional specific parameters.
If some parameters that are not required for current processor type are specified, they are ignored. 
If there are conflicting parameters (e.g `algo` is specified together with `jwks_uri`), an exception will be thrown.

## JWT (JSON Web Token)

JWT itself is a source of information about user.
It is decoded locally and its integrity is verified using either static key or JWKS (JSON Web Key Set), either local or remote.

`algo`, `static_jwks`/`static_jwks_file` and `jwks_uri` are defining different JWT processing workflows, and they cannot be specified together.
### JWT with static key:
```xml
<clickhouse>
    <token_processors>
        <my_static_key_validator>
          <type>jwt</type>
          <algo>HS256</algo>
          <static_key>my_static_secret</static_key>
        </my_static_key_validator>
    </token_processors>
</clickhouse>
```
**Parameters:**
- `algo` - Algorithm for signature validation. Mandatory. Supported values:

  | HMAC  | RSA   | ECDSA  | PSS   | EdDSA   |
    |-------| ----- | ------ | ----- | ------- |
  | HS256 | RS256 | ES256  | PS256 | Ed25519 |
  | HS384 | RS384 | ES384  | PS384 | Ed448   |
  | HS512 | RS512 | ES512  | PS512 |         |
  |       |       | ES256K |       |         |
  Also supports None (though not recommended).
`claims` - A string containing a JSON object that should be contained in the token payload. If this parameter is defined, token without corresponding payload will be considered invalid. Optional.
- `static_key` - key for symmetric algorithms. Mandatory for `HS*` family algorithms.
- `static_key_in_base64` - indicates if the `static_key` key is base64-encoded. Optional, default: `False`.
- `public_key` - public key for asymmetric algorithms. Mandatory except for `HS*` family algorithms and `None`.
- `private_key` - private key for asymmetric algorithms. Optional.
- `public_key_password` - public key password. Optional.
- `private_key_password` - private key password. Optional.

### JWT with static JWKS
```xml
<clickhouse>
    <token_processors>
        <my_static_jwks_validator>
          <type>jwt</type>
          <static_jwks>{"keys": [{"kty": "RSA", "alg": "RS256", "kid": "mykid", "n": "_public_key_mod_", "e": "AQAB"}]}</static_jwks>
        </my_static_jwks_validator>
    </token_processors>
</clickhouse>
```

**Parameters:**

- `static_jwks` - content of JWKS in JSON
- `static_jwks_file` - path to a file with JWKS
- `claims` - A string containing a JSON object that should be contained in the token payload. If this parameter is defined, token without corresponding payload will be considered invalid. Optional.
- `verifier_leeway` - Clock skew tolerance (seconds). Useful for handling small differences in system clocks between ClickHouse and the token issuer. Optional.

:::note
Only one of `static_jwks` or `static_jwks_file` keys must be present in one verifier
:::

:::note
Only RS* family algorithms are supported!
:::

### JWT with remote JWKS
```xml
<clickhouse>
    <token_processors>
        <basic_auth_server>
          <type>jwt</type>
          <jwks_uri>http://localhost:8000/.well-known/jwks.json</jwks_uri>
          <jwks_cache_lifetime>3600</jwks_cache_lifetime>
        </basic_auth_server>
    </token_processors>
</clickhouse>
```

**Parameters:**

- `uri` - JWKS endpoint. Mandatory.
- `jwks_cache_lifetime` - Period for resend request for refreshing JWKS. Optional, default: 3600.
- `claims` - A string containing a JSON object that should be contained in the token payload. If this parameter is defined, token without corresponding payload will be considered invalid. Optional.
- `verifier_leeway` - Clock skew tolerance (seconds). Useful for handling small differences in system clocks between ClickHouse and the token issuer. Optional.


## Processors with external providers

Some tokens cannot be decoded and validated locally. External service is needed in this case. "Azure" and "OpenID" (a generic type) are supported now.

### Azure
```xml
<clickhouse>
    <token_processors>
        <azure_processor>
          <type>azure</type>
        </azure_processor>
    </token_processors>
</clickhouse>
```

No additional parameters are required.

### OpenID
```xml
<clickhouse>
    <token_processors>
        <oid_processor_1>
          <type>openid</type>
          <configuration_endpoint>url/.well-known/openid-configuration</configuration_endpoint>
          <verifier_leeway>60</verifier_leeway>
          <jwks_cache_lifetime>3600</jwks_cache_lifetime>
        </oid_processor_1>
        <oid_processor_2>
          <type>openid</type>
          <userinfo_endpoint>url/userinfo</userinfo_endpoint>
          <token_introspection_endpoint>url/tokeninfo</token_introspection_endpoint>
          <jwks_uri>url/.well-known/jwks.json</jwks_uri>
          <verifier_leeway>60</verifier_leeway>
          <jwks_cache_lifetime>3600</jwks_cache_lifetime>
        </oid_processor_2>
    </token_processors>
</clickhouse>
```

:::note
Either `configuration_endpoint` or both `userinfo_endpoint` and `token_introspection_endpoint` (and, optionally, `jwks_uri`) shall be set. If none of them are set or all three are set, this is an invalid configuration that will not be parsed.
:::

**Parameters:**

- `configuration_endpoint` - URI of OpenID configuration (often ends with `.well-known/openid-configuration`);
- `userinfo_endpoint` - URI of endpoint that returns user information in exchange for a valid token;
- `token_introspection_endpoint` - URI of token introspection endpoint (returns information about a valid token);
- `jwks_uri` - URI of OpenID configuration (often ends with `.well-known/jwks.json`)
- `jwks_cache_lifetime` - Period for resend request for refreshing JWKS. Optional, default: 3600.
- `verifier_leeway` - Clock skew tolerance (seconds). Useful for handling small differences in system clocks between ClickHouse and the token issuer. Optional, default: 60

Sometimes a token is a valid JWT. In that case token will be decoded and validated locally if configuration endpoint returns JWKS URI (or `jwks_uri` is specified alongside `userinfo_endpoint` and `token_introspection_endpoint`).

### Tokens cache
To reduce number of requests to IdP, tokens are cached internally for a maximum period of `token_cache_lifetime` seconds.
If token expires sooner than `token_cache_lifetime`, then cache entry for this token will only be valid while token is valid.
If token lifetime is longer than `token_cache_lifetime`, cache entry for this token will be valid for `token_cache_lifetime`. 

## Enabling token authentication for a user in `users.xml` {#enabling-jwt-auth-in-users-xml}

In order to enable token-based authentication for the user, specify `jwt` section instead of `password` or other similar sections in the user definition.

Parameters:
- `claims` - An optional string containing a json object that should be contained in the token payload.

Example (goes into `users.xml`):
```xml
<clickhouse>
    <my_user>
        <jwt>
            <claims>{"resource_access":{"account": {"roles": ["view-profile"]}}}</claims>
        </jwt>
    </my_user>
</clickhouse>
```

Here, the JWT payload must contain `["view-profile"]` on path `resource_access.account.roles`, otherwise authentication will not succeed even with a valid JWT.

:::note
If `claims` is defined, this user will not be able to authenticate using opaque tokens, so, only JWT-based authentication will be available.
:::

```
{
...
  "resource_access": {
    "account": {
      "roles": ["view-profile"]
    }
  },
...
}
```

:::note
A user cannot have JWT authentication together with any other authentication method. The presence of any other sections like `password` alongside `jwt` will force ClickHouse to shut down.
:::

## Enabling token authentication using SQL {#enabling-jwt-auth-using-sql}

Users with "JWT" authentication type cannot be created using SQL now.

## Identity Provider as an External User Directory {#idp-external-user-directory}

If there is no suitable user pre-defined in ClickHouse, authentication is still possible: Identity Provider can be used as source of user information.
To allow this, add `token` section to the `users_directories` section of the `config.xml` file.

At each login attempt, ClickHouse tries to find the user definition locally and authenticate it as usual.
If a token is provided but the user is not defined, ClickHouse will treat the user as externally defined and will try to validate the token and get user information from the specified processor.
If validated successfully, the user will be considered existing and authenticated. The user will be assigned roles from the list specified in the `roles` section. 
All this implies that the SQL-driven [Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled and roles are created using the [CREATE ROLE](/docs/en/sql-reference/statements/create/role.md#create-role-statement) statement.

**Example**

```xml
<clickhouse>
    <user_directories>
        <token>
            <processor>token_processor_name</processor>
            <common_roles>
                <token_test_role_1 />
            </common_roles>
            <roles_filter>
                \bclickhouse-[a-zA-Z0-9]+\b
            </roles_filter>
        </token>
    </user_directories>
</clickhouse>
```

:::note
For now, no more than one `token` section can be defined inside `user_directories`. This _may_ change in future.
:::

**Parameters**

- `processor` — Name of one of processors defined in `token_processors` config section described above. This parameter is mandatory and cannot be empty.
- `common_roles` — Section with a list of locally defined roles that will be assigned to each user retrieved from the IdP. Optional.
- `roles_filter` — Regex string for groups filtering. Only groups matching this regex will be mapped to roles. Optional.
