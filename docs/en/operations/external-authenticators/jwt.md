---
description: 'Guide to JWT-based authentication and ephemeral users in ClickHouse Cloud'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'JWT Authentication'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge/>

ClickHouse can authenticate users using JSON Web Tokens (JWTs). Unlike other external authenticators such as [LDAP](/operations/external-authenticators/ldap) or [Kerberos](/operations/external-authenticators/kerberos), JWT authentication does not verify the identity of pre-existing users. Instead, it dynamically creates **ephemeral users** from the claims embedded in each token. These users exist only in memory, receive access rights derived from token claims, and are automatically removed after the token expires.

This makes JWT authentication fundamentally different from password-based or certificate-based methods: there is no `CREATE USER ... IDENTIFIED WITH jwt` statement, and attempting it raises an exception. JWT users are fully managed by the token lifecycle.

## Overview {#overview}

The authentication flow works as follows:

1. A client presents a signed JWT via one of the supported transport mechanisms (HTTP `Authorization: Bearer` header, the TCP native protocol, or the gRPC `jwt` field).
2. ClickHouse validates the token signature.
3. Required claims (`exp`, `iat`, `iss`, `sub`, `aud`) are verified.
4. An ephemeral user is created in memory with access rights derived from the `clickhouse:grants` and `clickhouse:roles` token claims, intersected with a permission limit.
5. When the token expires, a background garbage collection task removes the user.

## Token claims {#token-claims}

### Required claims {#required-claims}

Every JWT presented to ClickHouse must contain the following claims:

| Claim | Description |
|---|---|
| `alg` | Signing algorithm (header claim). Supported values: `HS256`, `RS256`, `ES256`. |
| `exp` | Expiration time. Sets the ephemeral user's `valid_until`. |
| `iat` | Issued-at time. Used to prevent replay of older tokens for the same identity. |
| `iss` | Issuer. Matched against the provider's expected issuer. |
| `sub` | Subject. Becomes part of the generated username. |
| `aud` | Audience. Matched against the provider's expected audience. |

The `kid` (key ID) header claim is also required when JWKS-based key resolution is used.

:::note JWKS mode supports RSA keys only
While static-key providers accept any of `HS256`, `RS256`, or `ES256`, JWKS-based providers only accept JWKs whose `kty` is `RSA` (i.e., tokens signed with `RS256`). Tokens signed with HMAC (`HS256`) or EC (`ES256`) keys cannot be verified against a JWKS endpoint and will be rejected.
:::

### Other recognized claims {#other-recognized-claims}

| Claim | Description |
|---|---|
| `nbf` | Not-before time. This claim is not required, but if present, tokens are rejected before this time. |
| `jti` | Reserved. Accepted in tokens but not currently validated or used. |

### Optional claims {#optional-claims}

| Claim | Default name | Description |
|---|---|---|
| Grants | `clickhouse:grants` | A JSON array of SQL `GRANT` fragments, e.g. `["SELECT ON db.*", "INSERT ON db.table1"]`. Each element is parsed as the body of a `GRANT` statement. |
| Roles | `clickhouse:roles` | A JSON array of role names to assign, e.g. `["analyst", "reader"]`. |
The default claim names can be remapped to custom claim names if your identity provider uses different naming conventions.

### Example token header and payload {#example-token-header-and-payload}

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

## Ephemeral user behavior {#ephemeral-user-behavior}

JWT users differ from regular ClickHouse users in several important ways.

### Identity and naming {#identity-and-naming}

Each JWT user receives a deterministic UUID computed from the `iss`, `sub`, and `aud` claims. This UUID is **stable** across logins. A user who logs in multiple times with different tokens (but the same issuer, subject, and audience) always gets the same UUID.

The username, however, is **volatile**. It is constructed as:

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

The `<claims_hash>` portion changes whenever the `clickhouse:roles` or `clickhouse:grants` claims change. This means that tokens with different role or grant sets produce different usernames even for the same identity.

### Access rights {#access-rights}

Effective access rights are computed as:

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

Where `permission_limit` is the set of access rights held by a reference role or user configured as the upper bound. Rights requested by the token that exceed the limit are silently dropped.

### Token freshness {#token-freshness}

ClickHouse tracks the `iat` (issued-at) claim of the most recently authenticated token for each stable identity. If a token with an `iat` equal to or older than the stored value is presented, the server reuses the existing ephemeral user without re-evaluating claims. This prevents older tokens from downgrading a user's permissions.

### Lifetime and garbage collection {#lifetime-and-garbage-collection}

Ephemeral users are created when a token is first authenticated and removed by a background garbage collection task after `valid_until` (derived from `exp`) passes. The GC interval is controlled by the `gc_interval` parameter (default: 5 minutes).

Between GC runs, expired users may still be visible in `system.users` but can no longer authenticate.

### Persistent access assignments {#persistent-access-assignments}

Because the UUID is stable, you can assign settings profiles, quotas, row policies, and column masking policies to a JWT user using SQL statements. These assignments persist in the access control storage (on disk or in ZooKeeper) and survive token expiry and re-authentication.

Reference the user by their current username:

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
The username and UUID for a given identity can be found in the `name` and `id` columns of `system.users` while the user is active.
:::

Note that `ALTER USER` does not work on JWT users directly, as they are read-only. To assign settings profiles, quotas, or policies, use the `ALTER SETTINGS PROFILE`, `ALTER QUOTA`, or `ALTER ROW POLICY` statements as shown above.

## Differences from regular users {#differences-from-regular-users}

| Feature | JWT users | Regular users |
|---|---|---|
| Creation | Automatic from token claims | `CREATE USER` statement |
| Storage | In-memory only (ephemeral) | Disk, ZooKeeper, or config file |
| `CREATE USER ... IDENTIFIED WITH jwt` | Not supported (raises exception) | All other auth types supported |
| `ALTER USER` / `DROP USER` | Not supported | Supported |
| Backup and restore | Not included | Included |
| Username | Auto-generated, volatile | Administrator-chosen, fixed |
| UUID | Deterministic from `iss`+`sub`+`aud` | Random at creation time |
| Lifetime | Bounded by token `exp` | Until explicitly dropped |
| Access rights | Derived from token claims, capped by permission limit | Explicitly granted via `GRANT` |
| Host restrictions | Per-provider network configuration | Per-user `HOST` clause |
| Settings profiles | Assignable by UUID (persistent) | Directly configurable |
| Quotas and row policies | Assignable by UUID (persistent) | Directly configurable |
| Default roles | Not configurable | Configurable |

## SQL SECURITY DEFINER views {#sql-security-definer-views}

When an ephemeral JWT user creates a view with `SQL SECURITY DEFINER`, the server automatically creates a persistent shadow copy of the user to serve as the view's definer. This shadow user:

- Has the name `<original_jwt_username>:definer`
- Has `NO_AUTHENTICATION` (cannot be used to log in)
- Retains the same access rights as the original JWT user at the time the view was created

This ensures that the view continues to function after the ephemeral user's token expires and the original user is garbage-collected.

## Client usage {#client-usage}

### Passing a token directly {#passing-token-directly}

Use the `--jwt` flag with `clickhouse-client` to authenticate with a pre-obtained token:

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
The `--jwt` flag is mutually exclusive with `--user`. When `--jwt` is specified, the username is derived from the token.
:::

### HTTP interface {#http-interface}

Send the token as a Bearer token in the `Authorization` header:

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
Always send JWTs over HTTPS. A Bearer token sent over plain HTTP is exposed to anyone on the network path and is equivalent to leaking the credential.
:::

### OAuth2 device code login {#oauth2-device-code-login}

The `clickhouse-client` supports an interactive OAuth2 device code flow via the `--login` flag. For ClickHouse Cloud endpoints, the client automatically performs token exchange to obtain a ClickHouse-specific JWT. Tokens are refreshed transparently during the session. When a new token is obtained, the client reconnects automatically.

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

## ClickHouse Cloud built-in JWT authenticator {#clickhouse-cloud-built-in}

Every ClickHouse Cloud service comes with a predefined JWT authenticator that is used by SQL Console and the `clickhouse-client` `--login` flow. This authenticator is configured with:

| Parameter | Value |
|---|---|
| `iss` (issuer) | `ClickHouse` |
| `aud` (audience) | The service UUID (visible in the Cloud console URL) |
| `sub` (subject) | Your ClickHouse Cloud account email address |

The built-in authenticator has a permission limit set to the `default_role` role and the `default` user. This means the effective rights of any JWT user are intersected with the grants held by those two entities, so a token can never escalate privileges beyond what `default_role` and `default` are allowed to do.

You do not need to configure anything to use this authenticator. It is provisioned automatically when the service is created.

## Interserver communication {#interserver-communication}

When a query is forwarded to another shard or replica, the JWT token is included in the interserver protocol. The remote node re-authenticates the token independently, creating its own ephemeral user.

## Troubleshooting {#troubleshooting}

- **No access rights granted:** The referenced role or user may lack the required grants. Ensure the roles referenced in the `clickhouse:roles` exist and include the appropriate grants.
- **Token rejected:** Verify that `iss`, `aud`, and the signing algorithm in your token match what the JWT provider expects. If JWKS is used, ensure the token's `kid` matches a key in the provider's key set.
- **User disappears between queries:** Ephemeral users are removed after token expiry. Use a client that supports token refresh (e.g., `--login` mode) for long-running sessions.
- **`CREATE USER ... IDENTIFIED WITH jwt` fails:** This is expected. JWT users cannot be created via DDL. They are managed entirely by the token lifecycle.
