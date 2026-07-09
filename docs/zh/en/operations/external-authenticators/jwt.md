---
description: 'ClickHouse Cloud 中基于 JWT 的身份验证和临时用户指南'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'JWT 身份验证'
doc_type: '参考'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

ClickHouse 可以使用 JSON Web Token (JWT) 对用户进行身份验证。与 [LDAP](/zh/operations/external-authenticators/ldap) 或 [Kerberos](/zh/operations/external-authenticators/kerberos) 等其他外部身份验证器不同，JWT 身份验证不会验证预先存在的用户身份。相反，它会根据嵌入在每个令牌中的 claims 动态创建**临时用户**。这些用户仅存在于内存中，获得的访问权限来自令牌中的 claims，并会在令牌过期后自动移除。

这使得 JWT 身份验证与基于密码或证书的方法有本质区别：不存在 `CREATE USER ... IDENTIFIED WITH jwt` 语句，尝试这样做会引发异常。JWT 用户完全由令牌生命周期管理。

<div id="overview">
  ## 概述
</div>

身份验证流程如下：

1. 客户端通过一种受支持的传输方式提交已签名的 JWT (HTTP `Authorization: Bearer` 请求头、TCP 原生协议，或 gRPC `jwt` 字段) 。
2. ClickHouse 会验证令牌签名。
3. 系统会验证必需的声明 (`exp`、`iat`、`iss`、`sub`、`aud`) 。
4. 系统会在内存中创建一个临时用户，其访问权限来自令牌声明 `clickhouse:grants` 和 `clickhouse:roles`，并与权限上限取交集。
5. 当令牌过期后，后台垃圾回收任务会移除该用户。

<div id="token-claims">
  ## 令牌声明
</div>

<div id="required-claims">
  ### 必需声明
</div>

提交给 ClickHouse 的每个 JWT 都必须包含以下声明：

| Claim | 描述                                          |
| ----- | ------------------------------------------- |
| `alg` | 签名算法 (请求头声明) 。支持的值：`HS256`、`RS256`、`ES256`。 |
| `exp` | 过期时间。设置临时用户的 `valid_until`。                 |
| `iat` | 签发时间。用于防止同一身份的旧令牌被重放。                       |
| `iss` | 签发方。与提供商预期的签发方进行匹配。                         |
| `sub` | 主题。会成为生成的用户名的一部分。                           |
| `aud` | 受众。与提供商预期的受众进行匹配。                           |

当使用基于 JWKS 的密钥解析时，还必须提供 `kid` (密钥 ID) 请求头声明。

:::note JWKS 模式仅支持 RSA 密钥
虽然静态密钥提供商接受 `HS256`、`RS256` 或 `ES256` 中的任意一种，但基于 JWKS 的提供商仅接受 `kty` 为 `RSA` 的 JWK (即使用 `RS256` 签名的令牌) 。使用 HMAC (`HS256`) 或 EC (`ES256`) 密钥签名的令牌无法根据 JWKS 端点进行验证，并将被拒绝。
:::

<div id="other-recognized-claims">
  ### 其他已识别的声明
</div>

| 声明    | 描述                                   |
| ----- | ------------------------------------ |
| `nbf` | 生效时间下限。此声明不是必需的，但如果存在，则在该时间之前令牌会被拒绝。 |
| `jti` | 保留字段。令牌中可包含此声明，但当前不会对其进行验证或使用。       |

<div id="optional-claims">
  ### 可选声明
</div>

| 声明                                        | 默认名称                | 描述                                                                                                   |
| ----------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------- |
| 授权                                        | `clickhouse:grants` | 由 SQL `GRANT` 片段组成的 JSON 数组，例如 `["SELECT ON db.*", "INSERT ON db.table1"]`。每个元素都会被解析为 `GRANT` 语句的主体。 |
| 角色                                        | `clickhouse:roles`  | 要分配的角色名称 JSON 数组，例如 `["analyst", "reader"]`。                                                         |
| 如果你的身份提供商采用不同的命名约定，可以将默认声明名称重新映射为自定义声明名称。 |                     |                                                                                                      |

<div id="example-token-header-and-payload">
  ### 令牌请求头和载荷示例
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
  ## 临时用户的行为
</div>

JWT 用户与常规 ClickHouse 用户在几个重要方面存在差异。

<div id="identity-and-naming">
  ### 身份与命名
</div>

每个 JWT 用户都会获得一个根据 `iss`、`sub` 和 `aud` claims 计算出的确定性 UUID。这个 UUID 在多次登录之间是**稳定的**。同一用户即使用不同的令牌多次登录 (只要签发方、subject 和 audience 相同) ，也始终会获得相同的 UUID。

不过，用户名是**可变的**。它的构造方式如下：

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

`<claims_hash>` 部分会在 `clickhouse:roles` 或 `clickhouse:grants` 声明发生变化时改变。这意味着，即使是同一身份，拥有不同角色或授权集合的令牌也会生成不同的用户名。

<div id="access-rights">
  ### 访问权限
</div>

实际生效的访问权限按如下方式计算：

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

其中，`permission_limit` 是配置为上限的参考角色或用户所拥有的一组访问权限。标记请求的权限若超出该上限，将被静默丢弃。

<div id="token-freshness">
  ### 令牌新鲜度
</div>

ClickHouse 会跟踪每个稳定身份最近一次通过认证的令牌中的 `iat` (签发时间) 声明。如果提交的令牌其 `iat` 等于或早于已存储的值，服务器会复用现有的临时用户，而不会重新评估这些声明。这样可以防止较旧的令牌降低用户权限。

<div id="lifetime-and-garbage-collection">
  ### 生命周期和垃圾回收
</div>

临时用户会在标记首次完成身份验证时创建，并在 `valid_until` (由 `exp` 推导得出) 过期后，由后台垃圾回收任务删除。GC 间隔由 `gc_interval` 参数控制 (默认值：5 分钟) 。

在两次 GC 执行之间，已过期的用户可能仍会显示在 `system.users` 中，但已无法再通过身份验证。

<div id="persistent-access-assignments">
  ### 持久化访问分配
</div>

由于 UUID 是稳定的，您可以使用 SQL 语句将 settings profile、配额、行策略和列脱敏策略分配给 JWT 用户。这些分配会持久保存在访问控制存储中 (位于磁盘上或 ZooKeeper 中) ，并且在令牌过期和重新身份验证后仍然有效。

通过用户当前的用户名引用该用户：

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
对于给定的身份，在用户处于活动状态时，可以在 `system.users` 的 `name` 和 `id` 列中找到其用户名和 UUID。
:::

请注意，`ALTER USER` 不能直接用于 JWT 用户，因为它们是只读的。要分配 profile、配额或策略，请使用上文所示的 `ALTER SETTINGS PROFILE`、`ALTER QUOTA` 或 `ALTER ROW POLICY` 语句。

<div id="differences-from-regular-users">
  ## 与普通用户的差异
</div>

| Feature                               | JWT 用户                                 | 普通用户               |
| ------------------------------------- | -------------------------------------- | ------------------ |
| 创建方式                                  | 根据标记中的 claims 自动创建                     | `CREATE USER` 语句   |
| 存储                                    | 仅存于内存中 (临时)                            | 磁盘、ZooKeeper 或配置文件 |
| `CREATE USER ... IDENTIFIED WITH jwt` | 不支持 (会引发异常)                            | 支持所有其他认证类型         |
| `ALTER USER` / `DROP USER`            | 不支持                                    | 支持                 |
| 备份和恢复                                 | 不包括                                    | 包括                 |
| 用户名                                   | 自动生成，非固定                               | 由管理员指定，固定          |
| UUID                                  | 由 `iss`+`sub`+`aud` 确定性生成              | 在创建时随机生成           |
| 生命周期                                  | 受标记 `exp` 限制                           | 直到被显式删除            |
| 访问权限                                  | 从标记中的 claims 派生，并受 permission limit 限制 | 通过 `GRANT` 显式授予    |
| 主机限制                                  | 按提供商的网络配置                              | 按用户的 `HOST` 子句     |
| 设置 profile                            | 可按 UUID 分配 (持久)                        | 可直接配置              |
| 配额和行策略                                | 可按 UUID 分配 (持久)                        | 可直接配置              |
| 默认角色                                  | 不可配置                                   | 可配置                |

<div id="sql-security-definer-views">
  ## SQL SECURITY DEFINER 视图
</div>

当临时 JWT 用户使用 `SQL SECURITY DEFINER` 创建视图时，服务器会自动为该用户创建一个持久化的影子副本，作为该视图的定义者。这个影子用户：

* 名称为 `<original_jwt_username>:definer`
* 具有 `NO_AUTHENTICATION` (无法用于登录)
* 保留原始 JWT 用户在创建视图时拥有的相同访问权限

这样可确保在临时用户的令牌过期、原始用户被垃圾回收后，视图仍能继续正常工作。

<div id="client-usage">
  ## Client 用法
</div>

<div id="passing-token-directly">
  ### 直接传递令牌
</div>

使用 `clickhouse-client` 的 `--jwt` 选项，通过预先获取的令牌进行身份验证：

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
`--jwt` 标志与 `--user` 互斥。指定 `--jwt` 时，用户名会从该令牌中提取。
:::

<div id="http-interface">
  ### HTTP 接口
</div>

通过 `Authorization` 请求头以 Bearer 令牌的形式发送该令牌：

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
始终通过 HTTPS 发送 JWT。通过明文 HTTP 发送的 Bearer 令牌会暴露给网络路径上的任何人，这就等同于泄露凭证。
:::

<div id="oauth2-device-code-login">
  ### OAuth2 设备代码登录
</div>

`clickhouse-client` 支持通过 `--login` 标志进行交互式 OAuth2 设备代码登录流程。对于 ClickHouse Cloud 端点，客户端会自动执行令牌交换，以获取 ClickHouse 特有的 JWT。令牌会在会话期间自动刷新且对用户无感知。获取到新令牌后，客户端会自动重新连接。

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

<div id="clickhouse-cloud-built-in">
  ## ClickHouse Cloud 内置 JWT 身份验证器
</div>

每个 ClickHouse Cloud 服务都带有一个预定义的 JWT 身份验证器，供 SQL 控制台和 `clickhouse-client` 的 `--login` 流程使用。该身份验证器配置如下：

| 参数               | 值                                     |
| ---------------- | ------------------------------------- |
| `iss` (签发方)      | `ClickHouse`                          |
| `aud` (audience) | 服务 UUID (可在 Cloud Console 的 URL 中看到)  |
| `sub` (subject)  | 你的 ClickHouse Cloud 账户电子邮件地址          |

该内置身份验证器的权限上限设为 `default_role` 角色和 `default` 用户。这意味着，任何 JWT 用户的有效权限都会与这两个实体拥有的授权取交集，因此令牌绝不可能将特权提升到超出 `default_role` 和 `default` 允许范围之外。

你无需进行任何配置即可使用此身份验证器。服务创建时会自动预配它。

<div id="interserver-communication">
  ## 服务器间通信
</div>

当查询被转发到另一分片或副本时，JWT 令牌会包含在服务器间通信协议中。远程节点会独立地重新验证该令牌，并创建自己的临时用户。

<div id="troubleshooting">
  ## 故障排查
</div>

* **未授予访问权限：** 所引用的角色或用户可能缺少所需的授权。请确保 `clickhouse:roles` 中引用的角色存在，并且包含相应的授权。
* **令牌被拒绝：** 请检查令牌中的 `iss`、`aud` 和签名算法是否与 JWT 提供商的预期一致。如果使用 JWKS，请确保令牌的 `kid` 与提供商密钥集中的某个密钥匹配。
* **用户在查询之间消失：** 临时用户会在令牌过期后被移除。对于长时间运行的会话，请使用支持令牌刷新的客户端 (例如 `--login` 模式) 。
* **`CREATE USER ... IDENTIFIED WITH jwt` 失败：** 这是预期行为。JWT 用户不能通过 DDL 创建，而是完全由令牌生命周期管理。