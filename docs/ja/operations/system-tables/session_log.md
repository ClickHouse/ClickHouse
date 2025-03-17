---
slug: /ja/operations/system-tables/session_log
---
# session_log

すべての成功および失敗したログインとログアウトイベントに関する情報を含みます。

カラム:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — クエリを実行しているサーバーのホスト名。
- `type` ([Enum8](../../sql-reference/data-types/enum.md)) — ログイン/ログアウト結果。可能な値:
    - `LoginFailure` — ログインエラー。
    - `LoginSuccess` — 成功したログイン。
    - `Logout` — システムからのログアウト。
- `auth_id` ([UUID](../../sql-reference/data-types/uuid.md)) — 認証ID。ユーザーがログインするたびに自動生成されるUUID。
- `session_id` ([String](../../sql-reference/data-types/string.md)) — クライアントが[HTTP](../../interfaces/http.md)インターフェースを介して渡すセッションID。
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — ログイン/ログアウトの日付。
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — ログイン/ログアウトの時間。
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — マイクロ秒精度でのログイン/ログアウト開始時間。
- `user` ([String](../../sql-reference/data-types/string.md)) — ユーザー名。
- `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)) — 認証の種類。可能な値:
    - `NO_PASSWORD`
    - `PLAINTEXT_PASSWORD`
    - `SHA256_PASSWORD`
    - `DOUBLE_SHA1_PASSWORD`
    - `LDAP`
    - `KERBEROS`
    - `SSL_CERTIFICATE`
- `profiles` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — すべてのロールおよび/またはユーザーに設定されたプロファイルのリスト。
- `roles` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — プロファイルが適用されるロールのリスト。
- `settings` ([Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md), [String](../../sql-reference/data-types/string.md)))) — クライアントがログイン/ログアウトしたときに変更された設定。
- `client_address` ([IPv6](../../sql-reference/data-types/ipv6.md)) — ログイン/ログアウト時に使用されたIPアドレス。
- `client_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — ログイン/ログアウト時に使用されたクライアントポート。
- `interface` ([Enum8](../../sql-reference/data-types/enum.md)) — ログインが開始されたインターフェース。可能な値:
    - `TCP`
    - `HTTP`
    - `gRPC`
    - `MySQL`
    - `PostgreSQL`
- `client_hostname` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) または他のTCPクライアントが実行されているクライアントマシンのホスト名。
- `client_name` ([String](../../sql-reference/data-types/string.md)) — `clickhouse-client` または他のTCPクライアント名。
- `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — `clickhouse-client` または他のTCPクライアントのリビジョン。
- `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — `clickhouse-client` または他のTCPクライアントのメジャーバージョン。
- `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — `clickhouse-client` または他のTCPクライアントのマイナーバージョン。
- `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — `clickhouse-client` または他のTCPクライアントバージョンのパッチコンポーネント。
- `failure_reason` ([String](../../sql-reference/data-types/string.md)) — ログイン/ログアウト失敗の理由を含む例外メッセージ。

**例**

クエリ:

``` sql
SELECT * FROM system.session_log LIMIT 1 FORMAT Vertical;
```

結果:

``` text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
type:                    LoginSuccess
auth_id:                 45e6bd83-b4aa-4a23-85e6-bd83b4aa1a23
session_id:
event_date:              2021-10-14
event_time:              2021-10-14 20:33:52
event_time_microseconds: 2021-10-14 20:33:52.104247
user:                    default
auth_type:               PLAINTEXT_PASSWORD
profiles:                ['default']
roles:                   []
settings:                [('load_balancing','random'),('max_memory_usage','10000000000')]
client_address:          ::ffff:127.0.0.1
client_port:             38490
interface:               TCP
client_hostname:
client_name:             ClickHouse client
client_revision:         54449
client_version_major:    21
client_version_minor:    10
client_version_patch:    0
failure_reason:
```
