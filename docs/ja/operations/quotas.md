---
slug: /ja/operations/quotas
sidebar_position: 51
sidebar_label: Quotas
title: Quotas
---

クォータは、リソース使用量を一定期間内で制限したり、リソースの使用を追跡することを可能にします。クォータは通常「users.xml」に設定されるユーザー設定に含まれます。

システムには、単一のクエリの複雑さを制限する機能もあります。詳細は、[クエリの複雑さに関する制限](../operations/settings/query-complexity.md)のセクションを参照してください。

クエリの複雑さの制限とは対照的に、クォータは以下を特徴とします：

- 単一のクエリを制限するのではなく、一定期間内で実行できるクエリのセットに対して制限をかけます。
- 分散クエリ処理のための全てのリモートサーバーで消費されたリソースを考慮します。

次に、クォータを定義する「users.xml」ファイルのセクションを見てみましょう。

``` xml
<!-- クォータ -->
<quotas>
    <!-- クォータ名 -->
    <default>
        <!-- 一定期間の制限。異なる制限を持つ多くのインターバルを設定できます。 -->
        <interval>
            <!-- インターバルの長さ。 -->
            <duration>3600</duration>

            <!-- 無制限。指定された時間間隔のデータを収集します。 -->
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

デフォルトでは、クォータは毎時のリソース消費を追跡し、使用を制限しません。
各インターバルごとに計算されたリソース消費は、リクエストごとにサーバーログに出力されます。

``` xml
<statbox>
    <!-- 一定期間の制限。異なる制限を持つ多くのインターバルを設定できます。 -->
    <interval>
        <!-- インターバルの長さ。 -->
        <duration>3600</duration>

        <queries>1000</queries>
        <query_selects>100</query_selects>
        <query_inserts>100</query_inserts>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <query_selects>10000</query_selects>
        <query_inserts>10000</query_inserts>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

‘statbox’ クォータの場合、1時間ごとおよび24時間（86,400秒）ごとに制限が設定されています。時間間隔は、実装で定義された固定の時点から始まるため、24時間のインターバルは必ずしも深夜に始まるわけではありません。

インターバルが終了すると、すべての収集された値はクリアされます。次の時間のクォータ計算は再び始まります。

次の制限可能な項目があります：

`queries` – リクエストの総数。

`query_selects` – selectリクエストの総数。

`query_inserts` – insertリクエストの総数。

`errors` – 例外をスローしたクエリの数。

`result_rows` – 結果として与えられた行の総数。

`read_rows` – すべてのリモートサーバーでクエリを実行するためにテーブルから読み取られたソース行の総数。

`execution_time` – 合計のクエリ実行時間（秒：ウォールタイム）。

少なくとも1つの時間間隔で制限を超えると、どの制限がどのインターバルで超えられたか、新しいインターバルがいつ始まるかについてのメッセージが添えられた例外がスローされます（クエリが再び送信できる場合）。

クォータは「クォータキー」機能を使用して、複数のキーに対して独立してリソースを報告することができます。以下にその例を示します：

``` xml
<!-- グローバルレポートデザイナー用 -->
<web_global>
    <!-- keyed – クォータ_key "key" はクエリパラメーターで渡され、キーの値ごとにクォータが別々に追跡されます。
        例えば、ユーザー名をキーとして渡せば、クォータはユーザー名ごとに別々に計算されます。
        キーを使用するのはプログラムがクォータ_keyを送信している場合にのみ適切です。ユーザーが直接送信する場合は適当ではありません。

        <keyed_by_ip /> と記述することもでき、IPアドレスがクォータキーとして使用されます。
        （ただし、ユーザーがIPv6アドレスを比較的容易に変更できることを念頭に置いてください。）
    -->
    <keyed />
```

クォータは設定の「users」セクションでユーザーに割り当てられます。「アクセス権」のセクションを参照してください。

分散クエリ処理では、蓄積された量は要求者のサーバーに保存されます。そのため、ユーザーが別のサーバーに移動すると、そのサーバーでのクォータは「やり直し」になります。

サーバーが再起動されると、クォータはリセットされます。
