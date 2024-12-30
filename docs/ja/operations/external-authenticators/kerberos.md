---
slug: /ja/operations/external-authenticators/kerberos
---
# Kerberos
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

既存で適切に設定されたClickHouseユーザーは、Kerberos認証プロトコルを通じて認証できます。

現在、Kerberosは既存ユーザーの外部認証装置としてのみ使用でき、そのユーザーは`users.xml`またはローカルアクセス制御経路で定義されます。これらのユーザーはHTTPリクエストしか使用できず、GSS-SPNEGOメカニズムを使用して認証できなければなりません。

このアプローチを取るには、システム内でKerberosが適切に設定され、ClickHouseの設定で有効にされている必要があります。

## ClickHouseでのKerberosの有効化 {#enabling-kerberos-in-clickhouse}

Kerberosを有効にするには、`config.xml`に`kerberos`セクションを含める必要があります。このセクションには追加のパラメータを含めることができます。

#### パラメータ:

- `principal` - セキュリティコンテキストを受け入れる際に取得および使用される正規のサービスプリンシパル名。
    - このパラメータは省略可能で、省略された場合はデフォルトのプリンシパルが使用されます。

- `realm` - 認証を、発信者のレルムが一致する要求に制限するために使用されるレルム。
    - このパラメータは省略可能で、省略された場合はレルムによる追加のフィルタリングは行われません。

- `keytab` - サービスのkeytabファイルへのパス。
    - このパラメータは省略可能で、省略された場合は、`KRB5_KTNAME`環境変数に設定されたサービスのkeytabファイルのパスを使用する必要があります。

例（`config.xml`に記述）:

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

プリンシパルを指定する場合:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

レルムによるフィルタリングを行う場合:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
`kerberos`セクションは一つだけ定義可能です。複数の`kerberos`セクションが存在すると、ClickHouseはKerberos認証を無効にします。
:::

:::note
`principal`と`realm`セクションを同時に指定することはできません。両方のセクションを指定すると、ClickHouseはKerberos認証を無効にします。
:::

## 既存ユーザーの外部認証装置としてのKerberos {#kerberos-as-an-external-authenticator-for-existing-users}

Kerberosは、ローカルに定義されたユーザー（`users.xml`またはローカルアクセス制御経路で定義されたユーザー）の識別を確認する方法として使用できます。現在、**HTTPインターフェース上の要求のみ**が*kerberized*（GSS-SPNEGOメカニズムを介して）されることが可能です。

Kerberosプリンシパル名の形式は通常、次のパターンに従います：

- *primary/instance@REALM*

*/instance*部分は0回以上発生することがあります。**認証が成功するためには、発信者の正規のプリンシパル名の*primary*部分がkerberizedユーザー名と一致する必要があります**。

### `users.xml`でのKerberos有効化 {#enabling-kerberos-in-users-xml}

ユーザーに対してKerberos認証を有効にするには、ユーザー定義で`password`やそれに類似するセクションの代わりに`kerberos`セクションを指定します。

パラメータ:

- `realm` - 認証を、発信者のレルムが一致する要求に制限するために使用されるレルム。
    - このパラメータは省略可能で、省略された場合はレルムによる追加のフィルタリングは行われません。

例（`users.xml`に記述）:

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
Kerberos認証は他の認証メカニズムと併用することはできません。`kerberos`と一緒に`password`のようなセクションが存在すると、ClickHouseは停止します。
:::

:::info Reminder
ユーザー`my_user`が`kerberos`を使用するようになった場合、前述の通り、メインの`config.xml`ファイルでKerberosを有効にする必要があります。
:::

### SQLを使用したKerberosの有効化 {#enabling-kerberos-using-sql}

ClickHouseで[SQL駆動のアクセス制御とアカウント管理](/docs/ja/guides/sre/user-management/index.md#access-control)が有効な場合、Kerberosで識別されるユーザーもSQL文を使用して作成できます。

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

レルムによるフィルタリングを行わない場合:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```
