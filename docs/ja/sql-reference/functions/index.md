---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Functions
toc_priority: 32
toc_title: "\u5C0E\u5165"
---

# 機能 {#functions}

少なくとも\*つのタイプの関数があります-通常の関数（これらは単に呼び出されます “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function doesn’t depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

このセクションでは、定期的な機能を説明します。 のための集計関数の項をご参照ください “Aggregate functions”.

\*-その機能の第三のタイプがあります ‘arrayJoin’ テーブルの機能も別々に言及することができます。\*

## 強力なタイピング {#strong-typing}

標準sqlとは対照的に、clickhouseは強力な型付けをしています。 言い換えれば、型間の暗黙の変換は行われません。 各関数は、特定のタイプのセットに対して機能します。 これは、時には型変換関数を使用する必要があることを意味します。

## 共通部分式の削除 {#common-subexpression-elimination}

同じast（構文解析の同じレコードまたは同じ結果）を持つクエリ内のすべての式は、同じ値を持つとみなされます。 このような式は連結され、一度実行されます。 この方法では、同一のサブクエリも削除されます。

## 結果のタイプ {#types-of-results}

すべての関数は、結果として単一の戻り値を返します（複数の値ではなく、ゼロの値ではありません）。 結果の型は、通常、値ではなく引数の型によってのみ定義されます。 例外は、tupleelement関数(a.n演算子)とtofixedstring関数です。

## 定数 {#constants}

簡単にするために、特定の関数はいくつかの引数の定数のみで動作します。 たとえば、like演算子のright引数は定数でなければなりません。
ほとんどすべての関数は定数引数の定数を返します。 例外は、乱数を生成する関数です。
その ‘now’ 関数は、異なる時間に実行されたクエリに対して異なる値を返しますが、定数は単一のクエリ内でのみ重要であるため、結果は定数と見なされます。
定数式も定数と見なされます(たとえば、like演算子の右半分は複数の定数から構築できます)。

関数は、定数と非定数引数（異なるコードが実行される）のために異なる方法で実装することができます。 しかし、定数と同じ値のみを含む真の列の結果は、互いに一致する必要があります。

## ヌル処理 {#null-processing}

関数の動作は次のとおりです:

-   関数の引数のうち少なくとも一つが `NULL`、機能結果はまたあります `NULL`.
-   各機能の説明で個別に指定される特別な動作。 のclickhouseソースコードは、これらの機能の `UseDefaultImplementationForNulls=false`.

## 不変性 {#constancy}

Functions can’t change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## エラー処理 {#error-handling}

データが無効な場合、一部の関数は例外をスローする可能性があります。 この場合、クエリは取り消され、エラーテキストがクライアントに返されます。 分散処理の場合、いずれかのサーバーで例外が発生すると、他のサーバーもクエリを中止しようとします。

## 引数式の評価 {#evaluation-of-argument-expressions}

ほぼすべてのプログラミング言語の一つの引数が評価される。 これは通常、演算子です `&&`, `||`、と `?:`.
しかし、clickhouseでは、関数（演算子）の引数は常に評価されます。 これは、各行を別々に計算するのではなく、列の全部分が一度に評価されるためです。

## 分散クエリ処理のための関数の実行 {#performing-functions-for-distributed-query-processing}

分散クエリ処理では、できるだけ多くのクエリ処理がリモートサーバーで実行され、その他のステージ(中間結果とそれ以降のすべてのステージ)はリクエスター

つまり、異なるサーバーで機能を実行できます。
たとえば、クエリでは `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   もし `distributed_table` は、少なくとも二つのシャード、機能 ‘g’ と ‘h’ リモートサーバー上で実行される。 ‘f’ 要求元サーバーで実行されます。
-   もし `distributed_table` 一つだけシャード、すべてを持っています ‘f’, ‘g’、と ‘h’ 機能は、このシャードのサーバー上で実行されます。

関数の結果は、通常、実行されるサーバーに依存しません。 しかし、時にはこれが重要です。
たとえば、辞書を操作する関数は、実行しているサーバー上に存在する辞書を使用します。
別の例は、 `hostName` この関数は、実行されているサーバーの名前を返します。 `GROUP BY` aのサーバーによって `SELECT` クエリ。

クエリ内の関数がリクエストサーバー上で実行されているが、リモートサーバー上で実行する必要がある場合は、次のようにラップします。 ‘any’ 関数を集めるか、それをキーに追加します `GROUP BY`.

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/) <!--hide-->
