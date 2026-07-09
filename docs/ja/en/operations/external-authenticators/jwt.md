---
description: 'ClickHouse Cloud における JWT ベースの認証と一時ユーザーに関するガイド'
sidebar_label: 'JWT'
sidebar_position: 55
slug: /operations/external-authenticators/jwt
title: 'JWT 認証'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

ClickHouse は JSON Web Tokens (JWT) を使用してユーザーを認証できます。[LDAP](/ja/operations/external-authenticators/ldap) や [Kerberos](/ja/operations/external-authenticators/kerberos) などの他の外部認証機構とは異なり、JWT 認証では既存ユーザーの本人確認は行いません。代わりに、各トークンに埋め込まれた クレーム から **一時ユーザー** を動的に作成します。これらのユーザーはメモリ内にのみ存在し、トークンの クレーム に基づいてアクセス権が付与され、トークンの有効期限が切れると自動的に削除されます。

このため、JWT 認証はパスワードベースや証明書ベースの方式とは本質的に異なります。`CREATE USER ... IDENTIFIED WITH jwt` ステートメントは存在せず、これを実行しようとすると例外が発生します。JWT ユーザーはトークンのライフサイクルによって完全に管理されます。

<div id="overview">
  ## 概要
</div>

認証フローは次のとおりです。

1. クライアントは、サポートされているいずれかの転送メカニズム (HTTP `Authorization: Bearer` ヘッダー、TCP ネイティブプロトコル、または gRPC の `jwt` フィールド) を介して、署名付き JWT を提示します。
2. ClickHouse はトークンの署名を検証します。
3. 必須のクレーム (`exp`、`iat`、`iss`、`sub`、`aud`) が検証されます。
4. `clickhouse:grants` および `clickhouse:roles` のトークンクレームから導出され、権限上限との積集合を取ったアクセス権を持つ一時ユーザーがメモリ内に作成されます。
5. トークンの有効期限が切れると、バックグラウンドのガベージコレクションタスクによってそのユーザーが削除されます。

<div id="token-claims">
  ## トークンクレーム
</div>

<div id="required-claims">
  ### 必須クレーム
</div>

ClickHouse に提示するすべての JWT には、次のクレームが含まれている必要があります。

| Claim | 説明                                                      |
| ----- | ------------------------------------------------------- |
| `alg` | 署名アルゴリズム (ヘッダークレーム) 。サポートされる値: `HS256`、`RS256`、`ES256`。 |
| `exp` | 有効期限。一時ユーザーの `valid_until` を設定します。                      |
| `iat` | 発行時刻。同一の ID に対する古いトークンのリプレイを防ぐために使用されます。                |
| `iss` | 発行者。プロバイダーに設定された想定発行者と照合されます。                           |
| `sub` | Subject。生成されるユーザー名の一部になります。                             |
| `aud` | audience。プロバイダーに設定された想定 audience と照合されます。               |

JWKS ベースのキー解決を使用する場合は、`kid` (キー ID) ヘッダークレームも必須です。

:::note JWKS モードでサポートされるのは RSA キーのみ
静的キーのプロバイダーでは `HS256`、`RS256`、`ES256` のいずれも使用できますが、JWKS ベースのプロバイダーで受け入れられるのは `kty` が `RSA` の JWK のみです (つまり、`RS256` で署名されたトークンのみ) 。HMAC (`HS256`) または EC (`ES256`) キーで署名されたトークンは JWKS エンドポイントでは検証できないため、拒否されます。
:::

<div id="other-recognized-claims">
  ### その他の認識されるクレーム
</div>

| クレーム  | 説明                                                   |
| ----- | ---------------------------------------------------- |
| `nbf` | 有効開始時刻。このクレームは必須ではありませんが、存在する場合、この時刻より前のトークンは拒否されます。 |
| `jti` | 予約済み。トークン内での指定は受け付けられますが、現時点では検証も利用もされません。           |

<div id="optional-claims">
  ### オプションのクレーム
</div>

| クレーム                                                           | デフォルト名              | 説明                                                                                                        |
| -------------------------------------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------- |
| Grants                                                         | `clickhouse:grants` | SQL `GRANT` 断片の JSON 配列。例: `["SELECT ON db.*", "INSERT ON db.table1"]`。各要素は `GRANT` ステートメントのボディとして解析されます。 |
| Roles                                                          | `clickhouse:roles`  | 割り当てるロール名の JSON 配列。例: `["analyst", "reader"]`。                                                            |
| ID プロバイダーで異なる命名規則を使用している場合は、デフォルトのクレーム名をカスタムのクレーム名に再マッピングできます。 |                     |                                                                                                           |

<div id="example-token-header-and-payload">
  ### トークンヘッダーとペイロードの例
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
  ## 一時ユーザーの挙動
</div>

JWT ユーザーは、通常の ClickHouse ユーザーとはいくつかの重要な点で異なります。

<div id="identity-and-naming">
  ### アイデンティティと命名
</div>

各 JWT ユーザーには、`iss`、`sub`、`aud` のクレームから算出される決定論的な UUID が割り当てられます。この UUID はログインをまたいでも**不変**です。異なるトークンで複数回ログインしても、issuer、subject、audience が同じであれば、常に同じ UUID が割り当てられます。

一方、ユーザー名 は**可変**です。これは次のように構成されます。

```text
JWT::<issuer>::<audience>::<subject>::<claims_hash>
```

`<claims_hash>` の部分は、`clickhouse:roles` または `clickhouse:grants` のクレームが変更されるたびに変化します。つまり、ロールや権限のセットが異なるトークンでは、同じアイデンティティであっても生成されるユーザー名が異なります。

<div id="access-rights">
  ### アクセス権
</div>

実効アクセス権は次のように計算されます。

```text
effective_rights = permission_limit ∩ (token_grants ∪ token_roles)
```

ここで `permission_limit` は、上限として設定された参照ロールまたはユーザーが持つアクセス権の集合です。トークンが要求したアクセス権のうち、この上限を超えるものは通知されることなく破棄されます。

<div id="token-freshness">
  ### トークンの鮮度
</div>

ClickHouse は、同一の identity ごとに、直近で認証されたトークンの `iat` (発行時刻) claim を追跡します。保存されている値以下の `iat` を持つトークンが提示された場合、サーバーは クレーム を再評価することなく、既存の一時ユーザーを再利用します。これにより、古いトークンによってユーザーの権限が引き下げられるのを防ぎます。

<div id="lifetime-and-garbage-collection">
  ### 有効期間とガベージコレクション
</div>

一時ユーザーは、トークンが最初に認証されたときに作成され、`valid_until` (`exp` から導出) を過ぎると、バックグラウンドのガベージコレクション タスクによって削除されます。GC の実行間隔は `gc_interval` パラメーター (デフォルト: 5 分) で制御されます。

GC の実行と実行の間は、期限切れのユーザーが `system.users` に表示されたままになることがありますが、認証には使用できなくなります。

<div id="persistent-access-assignments">
  ### 永続的なアクセス権の割り当て
</div>

UUID は不変のため、SQL 文を使って JWT ユーザーに設定プロファイル、クォータ、行ポリシー、カラムマスキングポリシーを割り当てることができます。これらの割り当てはアクセス制御ストレージ (ディスク上または ZooKeeper 内) に保存され、トークンの有効期限切れや再認証後も維持されます。

現在のユーザー名でユーザーを参照してください:

```sql
ALTER SETTINGS PROFILE my_profile ADD TO 'JWT::ClickHouse::my-service-id::jane.doe::<claims-hash>';
```

:::note
指定した identity の ユーザー名 と UUID は、ユーザーがアクティブな間、`system.users` の `name` カラムと `id` カラムで確認できます。
:::

JWT ユーザーは読み取り専用のため、`ALTER USER` を直接使用することはできません。設定プロファイル、クォータ、またはポリシーを割り当てるには、上記のとおり `ALTER SETTINGS PROFILE`、`ALTER QUOTA`、または `ALTER ROW POLICY` ステートメントを使用してください。

<div id="differences-from-regular-users">
  ## 一般ユーザーとの違い
</div>

| 機能                                    | JWTユーザー                     | 一般ユーザー                   |
| ------------------------------------- | --------------------------- | ------------------------ |
| 作成                                    | トークンのクレームから自動作成             | `CREATE USER` ステートメント    |
| 保存先                                   | メモリ内のみ (一時的)                | ディスク、ZooKeeper、または設定ファイル |
| `CREATE USER ... IDENTIFIED WITH jwt` | サポートされない (例外が発生する)          | その他のすべての認証方式をサポート        |
| `ALTER USER` / `DROP USER`            | サポートされない                    | サポートされる                  |
| バックアップと復元                             | 含まれない                       | 含まれる                     |
| ユーザー名                                 | 自動生成され、固定されない               | 管理者が指定し、固定               |
| UUID                                  | `iss`+`sub`+`aud` から決定論的に生成 | 作成時にランダムに生成              |
| 有効期間                                  | トークンの `exp` によって制限される       | 明示的に削除されるまで              |
| アクセス権                                 | トークンのクレームから導出され、権限上限で制限される  | `GRANT` によって明示的に付与される    |
| ホスト制限                                 | プロバイダーごとのネットワーク設定           | ユーザーごとの `HOST` 句         |
| 設定プロファイル                              | UUIDで割り当て可能 (永続的)           | 直接設定可能                   |
| クォータと行ポリシー                            | UUIDで割り当て可能 (永続的)           | 直接設定可能                   |
| デフォルトロール                              | 設定不可                        | 設定可能                     |

<div id="sql-security-definer-views">
  ## SQL SECURITY DEFINERビュー
</div>

一時ユーザーのJWTユーザーが `SQL SECURITY DEFINER` を指定してビューを作成すると、サーバーはそのビューのDEFINERとして使用するために、そのユーザーの永続的なシャドウコピーを自動的に作成します。このシャドウユーザーには、次の特徴があります。

* 名前は `<original_jwt_username>:definer`
* `NO_AUTHENTICATION` が設定される (ログインには使用できません)
* ビューの作成時点で、元のJWTユーザーと同じアクセス権を保持する

これにより、一時ユーザーのトークンが期限切れとなって元のユーザーがガベージコレクションされた後も、そのビューは引き続き機能します。

<div id="client-usage">
  ## Client の使い方
</div>

<div id="passing-token-directly">
  ### トークンを直接渡す
</div>

事前に取得したトークンで認証するには、`clickhouse-client` で `--jwt` フラグを使用します。

```bash
clickhouse-client --host your-instance.clickhouse.cloud --secure --jwt '<your_jwt_token>'
```

:::note
`--jwt` フラグと `--user` は同時に使用できません。`--jwt` を指定した場合、ユーザー名はトークンから取得されます。
:::

<div id="http-interface">
  ### HTTP インターフェイス
</div>

トークンを Bearer token として `Authorization` ヘッダーで送信します:

```bash
curl -H 'Authorization: Bearer <your_jwt_token>' \
    'https://your-instance.clickhouse.cloud:8443/?query=SELECT+currentUser()'
```

:::warning
JWT は必ず HTTPS 経由で送信してください。Bearer token を平文の HTTP で送信すると、ネットワーク経路上の第三者に漏洩するおそれがあり、認証情報を漏らすのと同じことになります。
:::

<div id="oauth2-device-code-login">
  ### OAuth2 デバイスコードログイン
</div>

`clickhouse-client` は、`--login` フラグを使用した対話型の OAuth2 デバイスコードフローをサポートしています。ClickHouse Cloud エンドポイントでは、クライアントは ClickHouse 固有の JWT を取得するためのトークン交換を自動的に実行します。トークンはセッション中に透過的に更新されます。新しいトークンを取得すると、クライアントは自動的に再接続します。

```bash
clickhouse-client --host your-instance.clickhouse.cloud --login
```

<div id="clickhouse-cloud-built-in">
  ## ClickHouse Cloud 組み込み JWT 認証器
</div>

すべての ClickHouse Cloud サービスには、SQL Console および `clickhouse-client` の `--login` フローで使用される、事前定義済みの JWT 認証器が用意されています。この認証器は、以下のように設定されています。

| パラメータ            | 値                                        |
| ---------------- | ---------------------------------------- |
| `iss` (issuer)   | `ClickHouse`                             |
| `aud` (audience) | サービス UUID (Cloud Console の URL で確認できます)  |
| `sub` (subject)  | ご利用の ClickHouse Cloud アカウントのメールアドレス      |

この組み込み認証器の権限上限は、`default_role` ロールおよび `default` ユーザーに設定されています。つまり、JWT ユーザーの実効権限は、これら 2 つのエンティティに付与されている権限との積集合になります。そのため、トークンによって `default_role` と `default` に許可された範囲を超える権限昇格はできません。

この認証器を使用するために追加の設定は不要です。サービスの作成時に自動的にプロビジョニングされます。

<div id="interserver-communication">
  ## サーバー間通信
</div>

クエリが別の分片またはレプリカに転送されると、JWTトークンがサーバー間プロトコルに含まれます。リモートノードはそのトークンを個別に再認証し、独自の一時ユーザーを作成します。

<div id="troubleshooting">
  ## トラブルシューティング
</div>

* **必要なアクセス権が付与されていない:** 参照先のロールまたはユーザーに必要なアクセス権が付与されていない可能性があります。`clickhouse:roles` で参照しているロールが存在し、適切なアクセス権が含まれていることを確認してください。
* **トークンが拒否される:** トークン内の `iss`、`aud`、および署名アルゴリズムが、JWT プロバイダーの想定と一致していることを確認してください。JWKS を使用している場合は、トークンの `kid` がプロバイダーのキーセット内のキーと一致していることを確認してください。
* **クエリ間でユーザーが消える:** 一時ユーザーはトークンの有効期限が切れると削除されます。長時間のセッションでは、トークンの更新をサポートするクライアント (例: `--login` モード) を使用してください。
* **`CREATE USER ... IDENTIFIED WITH jwt` が失敗する:** これは想定どおりの動作です。JWT ユーザーは DDL では作成できません。完全にトークンのライフサイクルによって管理されます。