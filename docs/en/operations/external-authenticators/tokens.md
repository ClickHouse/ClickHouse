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

Token-based authentication is enabled by default. To disable it, set `enable_token_auth` to `0` in `config.xml`:

```xml
<enable_token_auth>0</enable_token_auth>
```

When disabled, token processors are not parsed, TokenAccessStorage is not available, and authentication via tokens (`--jwt` option or `Authorization: Bearer` header) is rejected.

To use token-based authentication, add `token_processors` section to `config.xml` and define at least one token processor in it.
Its contents are different for different token processor types.

**Common parameters**
- `type` -- type of token processor. Supported values: `jwt_static_key`, `jwt_static_jwks`, `jwt_dynamic_jwks`, `entra` (`azure` is accepted as a back-compat alias and resolves to the same `entra` processor — see the [Entra](#entra) section), `openid`. Mandatory. Case-insensitive.
- `token_cache_lifetime` -- maximum lifetime of cached token (in seconds). Optional, default: 3600.
- `username_claim` -- name of claim (field) that will be treated as ClickHouse username. Optional, default: "sub".
- `groups_claim` -- name of claim (field) that contains list of groups user belongs to. This claim will be looked up in the token itself (in case token is a valid JWT, e.g. in Keycloak) or in response from `/userinfo`. Optional, default: "groups".

For each type, there are additional specific parameters (some of them are mandatory).
If some parameters that are not required for current processor type are specified, they are ignored. 

## JWT (JSON Web Token)

JWT itself is a source of information about user.
It is decoded locally and its integrity is verified using either a local static key or JWKS (JSON Web Key Set), local or remote.

### JWT with static key:
```xml
<clickhouse>
    <token_processors>
        <my_static_key_validator>
          <type>jwt_static_key</type>
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
  Also supports None (not recommended and must *NEVER* be used in production).
- `claims` - A string containing a JSON object that should be contained in the token payload. If this parameter is defined, token without corresponding payload will be considered invalid. Optional.
- `static_key` - key for symmetric algorithms. Mandatory for `HS*` family algorithms.
- `static_key_in_base64` - indicates if the `static_key` key is base64-encoded. Optional, default: `False`.
- `public_key` - public key for asymmetric algorithms. Mandatory except for `HS*` family algorithms and `None`.
- `private_key` - private key for asymmetric algorithms. Optional.
- `public_key_password` - public key password. Optional.
- `private_key_password` - private key password. Optional.
- `expected_issuer` - Expected value of the `iss` (issuer) claim in the JWT. If specified, tokens with a different issuer will be rejected. Optional.
- `expected_audience` - Expected value of the `aud` (audience) claim in the JWT. If specified, tokens with a different audience will be rejected. Optional.
- `allow_no_expiration` - If `true`, tokens without the `exp` (expiration) claim are accepted. Otherwise they are rejected. Optional, default: `false`.

### JWT with static JWKS
```xml
<clickhouse>
    <token_processors>
        <my_static_jwks_validator>
          <type>jwt_static_jwks</type>
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
- `expected_issuer` - Expected value of the `iss` (issuer) claim in the JWT. If specified, tokens with a different issuer will be rejected. Optional.
- `expected_audience` - Expected value of the `aud` (audience) claim in the JWT. If specified, tokens with a different audience will be rejected. Optional.
- `allow_no_expiration` - If `true`, tokens without the `exp` (expiration) claim are accepted. Otherwise they are rejected. Optional, default: `false`.

:::note
Only one of `static_jwks` or `static_jwks_file` keys must be present in one verifier
:::

:::note
For JWKS-based validators (`jwt_static_jwks` and `jwt_dynamic_jwks`), RS* and ES* family algorithms are supported.
:::

### JWT with remote JWKS
```xml
<clickhouse>
    <token_processors>
        <basic_auth_server>
          <type>jwt_dynamic_jwks</type>
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
- `expected_issuer` - Expected value of the `iss` (issuer) claim in the JWT. If specified, tokens with a different issuer will be rejected. Optional.
- `expected_audience` - Expected value of the `aud` (audience) claim in the JWT. If specified, tokens with a different audience will be rejected. Optional.
- `allow_no_expiration` - If `true`, tokens without the `exp` (expiration) claim are accepted. Otherwise they are rejected. Optional, default: `false`.


## IdP-specific presets and generic external providers

This section covers two related kinds of processor: per-IdP convenience presets built on top of the generic JWT processors (currently `entra`), and the generic `openid` processor that talks to an arbitrary OIDC-compliant identity provider.

### Entra (Microsoft Entra ID, pure OIDC) {#entra}

`<type>entra</type>` is a preset for Microsoft Entra ID built on top of `jwt_dynamic_jwks`. Tokens are validated **locally** against Entra's per-tenant JWKS — no Microsoft Graph call, no userinfo round trip, no OIDC discovery fetch. `username_claim` and `groups_claim` are read directly from the JWT payload. Use this when the access token's `aud` is your own app (registered via Entra's *Expose an API* blade), not `https://graph.microsoft.com`.

:::note Migrating from the legacy `azure` processor
`<type>azure</type>` is now an **alias** for `<type>entra</type>` — at config-parse time the type string is rewritten and the rest of the pipeline is identical. The previous `azure` implementation (which round-tripped every token through Microsoft Graph's `/oidc/userinfo` and `/v1.0/me/memberOf` endpoints) has been removed entirely.

For operators upgrading: an `<type>azure</type>` block that previously had no other parameters will now fail to load with `'tenant_id' must be specified for 'entra' processor`. To migrate, add `<tenant_id>` (and ideally `<expected_audience>`) and make sure your application is configured to mint tokens whose `aud` is your own app, not Microsoft Graph. The setup recipe lives in `docs/entra-setup-draft.md`.
:::

Minimum configuration — only `tenant_id` is required; all other parameters have sensible defaults:

```xml
<clickhouse>
    <token_processors>
        <entra_prod>
          <type>entra</type>
          <tenant_id>aaaabbbb-0000-cccc-1111-dddd2222eeee</tenant_id>
        </entra_prod>
    </token_processors>
</clickhouse>
```

Example with common overrides (audience binding to a specific app, Entra-flavored username/groups claims):

```xml
<entra_prod>
    <type>entra</type>
    <tenant_id>aaaabbbb-0000-cccc-1111-dddd2222eeee</tenant_id>
    <expected_audience>api://clickhouse</expected_audience>
    <username_claim>preferred_username</username_claim>
    <groups_claim>roles</groups_claim>
</entra_prod>
```

**Parameters:**

- `tenant_id` — Microsoft Entra tenant identifier (a GUID, or an `*.onmicrosoft.com` domain). **Mandatory.** Multi-tenant aliases (`common`, `organizations`, `consumers`) are rejected because `JwksJwtProcessor` does exact-match issuer validation.

All remaining parameters are optional:

- `jwks_uri` — Override for the JWKS endpoint. Default: `https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys`. Override only for sovereign clouds (`login.microsoftonline.us`, `login.partner.microsoftonline.cn`).
- `expected_issuer` — Expected value of the `iss` claim. Default: `https://login.microsoftonline.com/{tenant_id}/v2.0` (derived from `tenant_id`). Override for v1.0 tokens (`https://sts.windows.net/{tenant_id}/`) or sovereign clouds.
- `expected_audience` — Expected value of the `aud` claim, normally your app's Application ID URI (e.g. `api://clickhouse`) or client ID. If unset, no audience check is performed (any signature-valid token from the tenant will authenticate); a warning is logged at startup so the gap is visible.
- `username_claim` — JWT claim to use as the ClickHouse username. Default: `sub`. Common Entra alternatives: `preferred_username`, `upn`, `oid`.
- `groups_claim` — JWT claim that carries the array of group identifiers. Default: `groups`. Set to `roles` if you use App Roles in Entra instead of security-group claims.
- `expected_typ`, `verifier_leeway`, `jwks_cache_lifetime`, `claims`, `allow_no_expiration`, `token_cache_lifetime` — Same as for `jwt_dynamic_jwks`.

:::note
The `groups` claim must be enabled in the app registration's manifest (`"groupMembershipClaims": "ApplicationGroup"` is recommended) and exposed in access tokens via `optionalClaims.accessToken`. Group identifiers in the token are object IDs (GUIDs) by default; map them to ClickHouse roles via the user-directory's `roles_mapping` block (see [Identity Provider as an External User Directory](#idp-external-user-directory)).
:::

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
- `expected_issuer` - Expected value of the `iss` (issuer) claim in the JWT. If specified, tokens with a different issuer will be rejected. Optional.
- `expected_audience` - Expected value of the `aud` (audience) claim in the JWT. If specified, tokens with a different audience will be rejected. Optional.
- `allow_no_expiration` - If `true`, tokens without the `exp` (expiration) claim are accepted. Otherwise they are rejected. Optional, default: `false`.

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
Per-user `claims` are enforced only when the token is a JWT (validated by a JWT processor such as `jwt_static_key`, `jwt_dynamic_jwks`, or `entra`). When the user authenticates with an opaque (access) token (e.g. via OpenID or Google token processors), claims are not checked and authentication succeeds if the token is otherwise valid.
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
            <default_profile>my_profile</default_profile>
            <roles_mapping>
                <map>
                    <from>8a1b2c3d-4e5f-6789-abcd-ef0123456789</from>
                    <to>ch_admin</to>
                </map>
                <map>
                    <from>9f8e7d6c-5b4a-3210-fedc-ba0987654321</from>
                    <to>ch_analyst</to>
                </map>
            </roles_mapping>
            <roles_filter>
                \bclickhouse-[a-zA-Z0-9]+\b
            </roles_filter>
            <roles_transform>s/-/_/g</roles_transform>
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
- `default_profile` — Name of a locally defined settings profile that will be assigned to each user retrieved from the IdP. If the profile does not exist, a warning will be logged and the user will be created without a profile. Optional.
- `roles_mapping` — Explicit map from incoming group identifier (e.g. an Entra security-group object ID) to a ClickHouse role name. Each entry is a `<map>` element with `<from>` and `<to>` children. Applied **before** `roles_filter` and `roles_transform`; groups absent from the map pass through unchanged, so the filter stage can be used to drop unmapped entries. Optional.
- `roles_filter` — Regex string for groups filtering. Only groups (after `roles_mapping` is applied) that match this regex will be considered. Optional.
- `roles_transform` — Sed-style transform pattern applied to group names (after `roles_mapping` and `roles_filter`) before mapping to roles. Format: `s/pattern/replacement/flags`. The `g` flag applies the replacement globally (all occurrences). Example: `s/-/_/g` converts `clickhouse-grp-dba` to `clickhouse_grp_dba`. Optional.

The three stages run in this order: `roles_mapping` → `roles_filter` → `roles_transform`. Stages are independent and any of them may be omitted.
