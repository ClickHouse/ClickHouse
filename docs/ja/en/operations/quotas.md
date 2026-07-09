---
description: 'ClickHouse でリソース使用量のクォータを設定および管理するためのガイド'
sidebar_label: 'Quotas'
sidebar_position: 51
slug: /operations/quotas
title: 'Quotas'
doc_type: 'guide'
---

:::note ClickHouse Cloud の クォータ
クォータ は ClickHouse Cloud でサポートされていますが、[DDL構文](/ja/sql-reference/statements/create/quota) を使用して作成する必要があります。以下で説明する XML設定 の方法は **サポートされていません**。
:::

クォータ を使用すると、一定期間にわたるリソース使用量を制限したり、リソースの使用状況を追跡したりできます。
クォータ はユーザー設定で構成します。通常は &#39;users.xml&#39; です。

システムには、単一のクエリの複雑さを制限する機能もあります。[Restrictions on query complexity](../operations/settings/query-complexity.md) のセクションを参照してください。

クエリ複雑度の制限とは対照的に、クォータ には次の特徴があります。

* 単一のクエリを制限するのではなく、一定期間に実行できる一連のクエリに制限を設けます。
* 分散クエリ処理において、すべてのリモートサーバーで消費されたリソースを計上します。

クォータ を定義する &#39;users.xml&#39; ファイルのセクションを見ていきましょう。

```xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <query_selects>0</query_selects>
            <query_inserts>0</query_inserts>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

既定では、クォータは使用量を制限せずに、1時間ごとのリソース使用量を追跡します。
各期間について計算されたリソース使用量は、各リクエストの後にサーバーログに出力されます。

```xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <query_selects>100</query_selects>
        <query_inserts>100</query_inserts>
        <written_bytes>5000000</written_bytes>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
        <failed_sequential_authentications>5</failed_sequential_authentications>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <query_selects>10000</query_selects>
        <query_inserts>10000</query_inserts>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <result_bytes>160000000000</result_bytes>
        <read_rows>500000000000</read_rows>
        <result_bytes>16000000000000</result_bytes>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

&#39;statbox&#39; クォータ では、1時間ごとおよび24時間ごと (86,400秒) に制限が設定されます。この時間間隔は、実装依存で定義される固定時点を起点としてカウントされます。つまり、24時間の時間間隔は必ずしも午前0時に始まるわけではありません。

時間間隔が終了すると、収集されたすべての値がクリアされます。次の1時間については、クォータ の計算が最初からやり直されます。

制限できる項目は次のとおりです。

`queries` – リクエストの総数。

`query_selects` – SELECT リクエストの総数。

`query_inserts` – INSERT リクエストの総数。

`errors` – 例外をスローしたクエリ数。

`result_rows` – 結果として返された行の総数。

`result_bytes` - 結果として返された行の合計サイズ。

`read_rows` – すべてのリモートサーバーでクエリを実行するために、テーブルから読み取られた行の総数。

`read_bytes` - すべてのリモートサーバーでクエリを実行するために、テーブルから読み取られた合計サイズ。

`written_bytes` - 書き込み操作の合計サイズ。

`execution_time` – クエリの総実行時間 (秒単位、実時間) 。

`failed_sequential_authentications` - 連続した認証エラーの総数。

`queries_per_normalized_hash` – 単一の正規化クエリに対する実行回数の上限です。正規化クエリとは、リテラルをプレースホルダーに置き換えたクエリのことで、`SELECT 1` と `SELECT 2` は同じ正規化クエリと見なされます。この制限は、個々の異なる正規化クエリパターンごとに独立して追跡されます。

少なくとも 1 つの期間でこの制限を超えると、どの制限に違反したか、どの期間に対するものか、そして次の期間がいつ始まるか (再びクエリを送信できるようになる時点) を示すテキストを含む例外がスローされます。

クォータでは、複数のキーのリソース使用状況をそれぞれ独立してレポートするために &quot;quota key&quot; 機能を使用できます。以下にその例を示します。

```xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)

        Instead of <keyed_by_ip /> you can use <keyed_by_forwarded_ip />, so the address
        from the X-Forwarded-For header is used as the quota key.

        For both <keyed_by_ip /> and <keyed_by_forwarded_ip /> you can additionally specify
        <ipv4_prefix_bits> and <ipv6_prefix_bits> to group clients by subnet instead of by a
        single address: the IP address is masked to the given prefix length before being used
        as the quota key. For example, <ipv4_prefix_bits>24</ipv4_prefix_bits> shares one bucket
        across a /24 IPv4 subnet, and <ipv6_prefix_bits>64</ipv6_prefix_bits> across a /64 IPv6
        subnet. These elements can only be used together with <keyed_by_ip /> or
        <keyed_by_forwarded_ip />.
    -->
    <keyed />
```

クォータ は正規化クエリハッシュでキー付けすることもでき、その場合、個々の異なるクエリパターンごとに独立した クォータ バケットが割り当てられます。XML 設定では、これは `<keyed_by_normalized_query_hash />` と記述します。

```xml
<my_quota>
    <keyed_by_normalized_query_hash />
    <interval>
        <duration>3600</duration>
        <queries>100</queries>
    </interval>
</my_quota>
```

同じ内容は、DDL構文でも表現できます。

```sql
CREATE QUOTA my_quota KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO my_user;
```

この例では、ユーザーは1時間あたり、各異なる正規化クエリを最大100回実行できます。`SELECT number FROM numbers(1)` と `SELECT number FROM numbers(2)` は同じバケットを共有します (正規化形式が同じであるため) が、`SELECT number, number FROM numbers(1)` は別のバケットを使用します。

クォータは、config の &#39;users&#39; セクションでユーザーに割り当てます。&quot;アクセス権&quot; セクションを参照してください。

分散クエリ処理では、累積値はリクエスト元のサーバーに保存されます。したがって、ユーザーが別のサーバーに移ると、そのサーバーのクォータは最初からカウントし直されます。

サーバーが再起動されると、クォータはリセットされます。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseでシングルページアプリケーションを構築する](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)