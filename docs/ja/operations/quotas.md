---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 51
toc_title: "\u30AF\u30A9\u30FC\u30BF"
---

# クォータ {#quotas}

クォータを使用すると、一定期間にわたってリソースの使用を制限したり、単にリソースの使用を追跡したりできます。
クォータは、ユーザー設定で設定されます。 これは通常 ‘users.xml’.

システムには、単一のクエリの複雑さを制限する機能もあります。 セクションを見る “Restrictions on query complexity”).

クエリの複雑さの制限とは対照的に、クォータ:

-   単一のクエリを制限するのではなく、ある期間にわたって実行できるクエリのセットに制限を設定します。
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

デフォルトでは、クォータだけでトラック資源の消費のそれぞれの時間を狭く限定することなく、利用
各間隔で計算されたリソース消費は、各要求の後にサーバーログに出力されます。

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

のための ‘statbox’ クォータ、制限は、毎時および24時間（86,400秒）ごとに設定されます。 時間間隔は、実装定義の固定moment間から開始してカウントされます。 言い換えれば、24時間の間隔は必ずしも真夜中に始まるとは限りません。

間隔が終了すると、収集された値はすべてクリアされます。 次の時間には、クォータの計算が最初からやり直されます。

制限できる金額は次のとおりです:

`queries` – The total number of requests.

`errors` – The number of queries that threw an exception.

`result_rows` – The total number of rows given as the result.

`read_rows` – The total number of source rows read from tables for running the query, on all remote servers.

`execution_time` – The total query execution time, in seconds (wall time).

制限を少なくとも一回の間隔で超えた場合は、制限を超過したテキスト、間隔、および新しい間隔の開始時(クエリを再度送信できる場合)についての

クォータは以下を使用できます “quota key” 複数のキーのリソースを個別に報告する機能。 これの例は次のとおりです:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip /> so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

クォータはユーザーに割り当てられます。 ‘users’ 設定のセクション。 セクションを見る “Access rights”.

分散クエリ処理では、累積金額がリクエスタサーバに格納されます。 ここでは、ユーザーが別のサーバーの定員がありま “start over”.

サーバーを再起動すると、クォータがリセットされます。

[元の記事](https://clickhouse.tech/docs/en/operations/quotas/) <!--hide-->
