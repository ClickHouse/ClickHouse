---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "\u30AF\u30A9\u30FC\u30BF"
---

# クォータ {#quotas}

クォータを使用すると、一定期間のリソース使用量を制限したり、リソースの使用を追跡したりできます。
クォータはユーザー設定で設定されます。 ‘users.xml’.

このシステムには、単一のクエリの複雑さを制限する機能もあります。 セクションを参照 “Restrictions on query complexity”).

クエリの複雑さの制限とは対照的に、クォータ:

-   単一のクエリを制限するのではなく、一定期間にわたって実行できるクエリのセットに制限を設定します。
-   口座のために費やすべてのリモートサーバーのための分散クエリ処となります。

のセクションを見てみましょう ‘users.xml’ クォータを定義するファイル。

``` xml
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
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

既定では、クォータは、使用量を制限することなく、各時間のリソース消費量を追跡します。
各間隔ごとに計算されたリソース消費量は、各要求の後にサーバーログに出力されます。

``` xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

のために ‘statbox’ クォータ、制限は、時間ごとおよび24時間ごと(86,400秒)に設定されます。 時間間隔は、実装定義の固定モーメントから開始してカウントされます。 つまり、24時間の間隔は必ずしも深夜に開始されるわけではありません。

間隔が終了すると、収集された値はすべてクリアされます。 次の時間は、クォータの計算がやり直されます。

制限できる金額は次のとおりです:

`queries` – The total number of requests.

`errors` – The number of queries that threw an exception.

`result_rows` – The total number of rows given as a result.

`read_rows` – The total number of source rows read from tables for running the query on all remote servers.

`execution_time` – The total query execution time, in seconds (wall time).

制限を超えた場合は、どの制限を超えたか、どの間隔を超えたか、および新しい間隔が開始されたとき(クエリを再度送信できるとき)に関するテキス

クォータは、 “quota key” 複数のキーのリソースを個別に報告する機能。 これの例を次に示します:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

クォータは ‘users’ 設定のセクション。 セクションを参照 “Access rights”.

分散クエリ処理の場合、累積金額は要求元サーバーに格納されます。 ここでは、ユーザーが別のサーバーの定員がありま “start over”.

サーバーを再起動すると、クォータがリセットされます。

[元の記事](https://clickhouse.com/docs/en/operations/quotas/) <!--hide-->
