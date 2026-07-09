---
description: '既存の適切に設定済みのClickHouseユーザーは、Kerberos認証プロトコルを使用して認証できます。'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

既存の ClickHouse ユーザーが適切に設定されていれば、Kerberos 認証プロトコルを使用して認証できます。

現在、Kerberos は `users.xml` またはローカルアクセス制御パスで定義された既存ユーザーに対する外部認証機構としてのみ使用できます。これらのユーザーは HTTP リクエストのみを使用でき、GSS-SPNEGO メカニズムで認証できる必要があります。

この方式では、システム側で Kerberos を設定し、ClickHouse の設定で有効にする必要があります。

<div id="enabling-kerberos-in-clickhouse">
  ## ClickHouse で Kerberos を有効にする
</div>

Kerberos を有効にするには、`config.xml` に `kerberos` セクションを追加します。このセクションには、追加のパラメータを指定できます。

<div id="parameters">
  #### パラメーター
</div>

* `principal` - セキュリティコンテキストの受け入れ時に取得され、使用される正規形式のサービスプリンシパル名。
  * このパラメーターは任意です。省略した場合は、デフォルトのプリンシパルが使用されます。

* `realm` - レルム。認証を、イニシエーターのレルムがこれと一致するリクエストのみに制限するために使用されます。
  * このパラメーターは任意です。省略した場合、レルムによる追加のフィルタリングは適用されません。

* `keytab` - サービス keytab ファイルへの path。
  * このパラメーターは任意です。省略した場合、サービス keytab ファイルへの path は `KRB5_KTNAME` 環境変数で設定する必要があります。

例 (`config.xml` に記述) :

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

principal を指定する場合:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

レルムでフィルタリングする場合:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
定義できる `kerberos` セクションは 1 つだけです。`kerberos` セクションが複数ある場合、ClickHouse は Kerberos 認証を無効にします。
:::

:::note
`principal` セクションと `realm` セクションを同時に指定することはできません。`principal` セクションと `realm` セクションの両方がある場合、ClickHouse は Kerberos 認証を無効にします。
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## 既存ユーザー向け外部認証機構としての Kerberos
</div>

Kerberos は、ローカルで定義されたユーザー (`users.xml` またはローカルアクセス制御パスで定義されたユーザー) の本人確認を行う方式として使用できます。現在、Kerberos 化できるのは HTTP インターフェイス経由のリクエスト**のみ**です (GSS-SPNEGO メカニズム経由) 。

Kerberos のプリンシパル名の形式は通常、次のパターンに従います。

* *primary/instance@REALM*

*/instance* の部分は 0 回以上現れる場合があります。**認証を成功させるには、イニシエーターの正規プリンシパル名の *primary* 部分が、Kerberos 化されたユーザー名と一致している必要があります**

<div id="enabling-kerberos-in-users-xml">
  ### `users.xml` で Kerberos を有効にする
</div>

ユーザーの Kerberos 認証を有効にするには、ユーザー定義で `password` などのセクションの代わりに `kerberos` セクションを指定します。

パラメータ:

* `realm` - 認証を、リクエストのイニシエーターのレルムがこの値と一致するものだけに制限するために使用するレルムです。
  * このパラメータは省略可能です。省略した場合、レルムによる追加の絞り込みは行われません。

例 (`users.xml` に記述) :

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
Kerberos 認証は、他の認証方式と併用できない点に注意してください。`kerberos` とあわせて `password` などの別のセクションが存在すると、ClickHouse は強制的にシャットダウンします。
:::

:::info リマインダー
なお、ユーザー `my_user` が `kerberos` を使用する場合は、前述のとおりメインの `config.xml` ファイルで Kerberos を有効化しておく必要があります。
:::

<div id="enabling-kerberos-using-sql">
  ### SQL を使用した Kerberos の有効化
</div>

ClickHouse で [SQL-driven Access Control and Account Management](/ja/operations/access-rights#access-control-usage) が有効な場合、Kerberos で識別されるユーザーも SQL ステートメントで作成できます。

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...または、レルムを指定せずに:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```