---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u95A2\u6570"
toc_priority: 32
toc_title: "\u306F\u3058\u3081\u306B"
---

# 関数 {#functions}

通常の関数（それらは単に呼び出されます）-関数の少なくとも\*二つの種類があります “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function doesn't depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

本項では通常の関数について説明する。 のための集計関数の項をご参照ください “Aggregate functions”.

\*-機能の第三のタイプがあります ‘arrayJoin’ テーブル関数は別にも言及することができます。\*

## 強力なタイピング {#strong-typing}

標準のSQLとは対照的に、ClickHouseには強力な型指定があります。 言い換えれば、型間で暗黙的な変換は行われません。 各関数は、特定の型セットに対して機能します。 つまり、型変換関数を使用する必要があることがあります。

## 共通の部分式の除去 {#common-subexpression-elimination}

同じAST(同じレコードまたは構文解析結果)を持つクエリ内のすべての式は、同じ値を持つと見なされます。 このような式は連結され、一度実行されます。 同一のサブクエリもこの方法で削除されます。

## 結果のタイプ {#types-of-results}

すべての関数は、結果として単一の戻り値を返します（複数の値ではなく、ゼロ値ではありません）。 結果の型は、通常、値ではなく、引数の型によってのみ定義されます。 例外は、tupleElement関数(a.N演算子)とtoFixedString関数です。

## 定数 {#constants}

簡単にするために、特定の関数は、いくつかの引数の定数のみで動作できます。 たとえば、LIKE演算子の右引数は定数でなければなりません。
ほとんどすべての関数は、定数引数の定数を返します。 例外は、乱数を生成する関数です。
その ‘now’ 関数は、異なる時間に実行されたクエリに対して異なる値を返しますが、定数は単一のクエリ内でのみ重要であるため、結果は定数と見なされます。
定数式は定数とも見なされます（たとえば、LIKE演算子の右半分は複数の定数から構成できます）。

関数は、定数と非定数引数のために異なる方法で実装することができます（異なるコードが実行されます）。 しかし、定数と同じ値だけを含む真の列の結果は、お互いに一致する必要があります。

## ヌル処理 {#null-processing}

関数の動作は次のとおりです:

-   関数の引数の少なくとも一つが `NULL`、関数の結果もあります `NULL`.
-   各関数の説明で個別に指定される特殊な動作。 ClickHouseのソースコードでは、これらの関数は `UseDefaultImplementationForNulls=false`.

## 不変性 {#constancy}

Functions can't change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## エラー処理 {#error-handling}

データが無効な場合、一部の関数は例外をスローすることがあります。 この場合、クエリはキャンセルされ、エラーテキストがクライアントに返されます。 分散処理の場合、いずれかのサーバーで例外が発生すると、他のサーバーもクエリを中止しようとします。

## 引数式の評価 {#evaluation-of-argument-expressions}

ほぼすべてのプログラミング言語の一つの引数が評価される。 これは通常、演算子です `&&`, `||`,and `?:`.
しかし、ClickHouseでは、関数（演算子）の引数は常に評価されます。 これは、各行を別々に計算するのではなく、列の部分全体が一度に評価されるためです。

## 分散クエリ処理のための関数の実行 {#performing-functions-for-distributed-query-processing}

分散クエリ処理の場合、リモートサーバーではできるだけ多くのステージのクエリ処理が実行され、残りのステージ(中間結果とそれ以降のすべてをマージ)はリクエ

つまり、異なるサーバーで機能を実行できます。
たとえば、クエリでは `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   もし `distributed_table` 少なくとも二つの破片を持っている、機能 ‘g’ と ‘h’ は、リモートサーバ上で実行され、 ‘f’ 要求元サーバー上で実行されます。
-   もし `distributed_table` 一つだけの破片、すべてを持っています ‘f’, ‘g’,and ‘h’ 機能は、このシャードのサーバー上で実行されます。

通常、関数の結果は、実行されるサーバーに依存しません。 しかし、時にはこれが重要です。
たとえば、辞書を操作する関数は、それらが実行されているサーバー上に存在する辞書を使用します。
別の例は、 `hostName` 実行しているサーバーの名前を返す関数 `GROUP BY` aのサーバーによって `SELECT` クエリ。

クエリ内の関数がリクエスタサーバで実行されているが、リモートサーバで実行する必要がある場合は、リクエスタサーバにラップすることができます。 ‘any’ 関数を集計するか、キーに追加します `GROUP BY`.

[元の記事](https://clickhouse.com/docs/en/query_language/functions/) <!--hide-->
