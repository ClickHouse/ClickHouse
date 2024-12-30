---
slug: /ja/operations/system-tables/quotas
---
# quotas

[クォータ](../../operations/system-tables/quotas.md)に関する情報を含みます。

カラム:
- `name` ([String](../../sql-reference/data-types/string.md)) — クォータ名。
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — クォータID。
- `storage`([String](../../sql-reference/data-types/string.md)) — クォータのストレージ。可能な値は、users.xmlファイルで設定された場合は「users.xml」、SQLクエリで設定された場合は「disk」です。
- `keys` ([Array](../../sql-reference/data-types/array.md)([Enum8](../../sql-reference/data-types/enum.md))) — クォータがどのように共有されるかを指定するキー。同じクォータとキーを使用する2つの接続は、同じリソース量を共有します。値:
    - `[]` — すべてのユーザーが同じクォータを共有します。
    - `['user_name']` — 同じユーザー名を持つ接続は同じクォータを共有します。
    - `['ip_address']` — 同じIPからの接続は同じクォータを共有します。
    - `['client_key']` — 同じキーを持つ接続は同じクォータを共有します。キーはクライアントによって明示的に提供される必要があります。[clickhouse-client](../../interfaces/cli.md)を使用する場合は、`--quota_key`パラメータでキー値を渡すか、クライアント構成ファイルに`quota_key`パラメータを使用します。HTTPインターフェースを使用する場合は、`X-ClickHouse-Quota`ヘッダーを使用します。
    - `['user_name', 'client_key']` — 同じ`client_key`を持つ接続は同じクォータを共有します。キーがクライアントによって提供されない場合、クォータは`user_name`で追跡されます。
    - `['client_key', 'ip_address']` — 同じ`client_key`を持つ接続は同じクォータを共有します。キーがクライアントによって提供されない場合、クォータは`ip_address`で追跡されます。
- `durations` ([Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 秒単位の時間間隔の長さ。
- `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 論理値。この値はクォータが適用されるユーザーを示します。値:
    - `0` — クォータは`apply_to_list`に指定されたユーザーに適用されます。
    - `1` — クォータは`apply_to_except`に記載されていないすべてのユーザーに適用されます。
- `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — クォータを適用すべきユーザー名/[ロール](../../guides/sre/user-management/index.md#role-management)のリスト。
- `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — クォータを適用しないユーザー名/ロールのリスト。

## 参照 {#see-also}

- [SHOW QUOTAS](../../sql-reference/statements/show.md#show-quotas-statement)
