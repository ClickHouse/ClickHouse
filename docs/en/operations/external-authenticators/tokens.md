---
slug: /en/operations/external-authenticators/oauth
title: "Token-based authentication"
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

An existing ClickHouse user (defined in `users.xml` or in local access control paths) can be authenticated with a token if this user can be `IDENTIFIED WITH jwt`. 

# Token Processors

## Configuration
To use token-based authentication, add `token_processors` section to `config.xml` and define at least one token processor in it.
Its contents are different for different token processor types.

**Common parameters**
- `type` -- type of token processor. Supported values: "JWT". Mandatory. Case-insensitive.
- `token_cache_lifetime` -- maximum lifetime of cached token (in seconds). Optional, default: 3600.
- `username_claim` -- name of claim (field) that will be treated as ClickHouse username. Optional, default: "sub".

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
