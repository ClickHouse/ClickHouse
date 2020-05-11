---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 66
toc_title: "\u305D\u306E\u4ED6"
---

# その他の機能 {#other-functions}

## ホスト名() {#hostname}

この関数が実行されたホストの名前を持つ文字列を返します。 分散処理の場合、機能がリモートサーバー上で実行される場合、これはリモートサーバーホストの名前です。

## FQDN {#fqdn}

完全修飾ドメイン名を返します。

**構文**

``` sql
fqdn();
```

この関数は、大文字と小文字を区別しません。

**戻り値**

-   完全修飾ドメイン名の文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT FQDN();
```

結果:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## ベース名 {#basename}

最後のスラッシュまたはバックスラッシュの後の文字列の末尾の部分を抽出します。 この関数は、パスからファイル名を抽出するためによく使用されます。

``` sql
basename( expr )
```

**パラメータ**

-   `expr` — Expression resulting in a [文字列](../../sql-reference/data-types/string.md) タイプ値。 すべての円記号は、結果の値でエスケープする必要があります。

**戻り値**

以下を含む文字列:

-   最後のスラッシュまたはバックスラッシュの後の文字列の末尾の部分。

        If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

-   スラッシュまたはバックスラッシュがない場合は、元の文字列。

**例えば**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x) {#visiblewidthx}

テキスト形式（タブ区切り）でコンソールに値を出力するときのおおよその幅を計算します。
この関数は、システムがpretty形式を実装するために使用します。

`NULL` に対応する文字列として表される。 `NULL` で `Pretty` フォーマット。

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName(x) {#totypenamex}

渡された引数の型名を含む文字列を返します。

もし `NULL` 入力として関数に渡され、その後、それが返されます `Nullable(Nothing)` 内部に対応するタイプ `NULL` ClickHouseでの表現。

## ブロックサイズ() {#function-blocksize}

ブロックのサイズを取得します。
ClickHouseでは、クエリは常にブロック(列部分のセット)で実行されます。 この関数は、それを呼び出したブロックのサイズを取得することができます。

## マテリアライズ(x) {#materializex}

一つの値だけを含む完全な列に定数を変換します。
ClickHouseでは、完全な列と定数はメモリ内で異なる方法で表されます。 関数は定数引数と通常の引数（異なるコードが実行される）では異なる動作をしますが、結果はほとんど常に同じです。 この関数は、この動作のデバッグ用です。

## ignore(…) {#ignore}

以下を含む任意の引数を受け取る `NULL`. 常に0を返します。
ただし、引数はまだ評価されます。 これはベンチマークに使用できます。

## スリープ(秒) {#sleepseconds}

眠る ‘seconds’ 各データブロックの秒。 整数または浮動小数点数を指定できます。

## sleepEachRow(秒) {#sleepeachrowseconds}

眠る ‘seconds’ 各行の秒。 整数または浮動小数点数を指定できます。

## currentDatabase() {#currentdatabase}

現在のデータベースの名前を返します。
この関数は、データベースを指定する必要があるcreate tableクエリのテーブルエンジンパラメーターで使用できます。

## currentUser() {#other-function-currentuser}

現在のユーザーのログインを返します。 ユーザのログインは、そのクエリを開始し、ケースdistibutedクエリで返されます。

``` sql
SELECT currentUser();
```

エイリアス: `user()`, `USER()`.

**戻り値**

-   現在のユーザーのログイン。
-   クエリを開始したユーザーのログイン。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT currentUser();
```

結果:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## isFinite(x) {#isfinitex}

引数が無限でなくnanでない場合はfloat32とfloat64を受け取り、uint8を1に返します（それ以外の場合は0）。

## イシンフィナイト(x) {#isinfinitex}

引数が無限の場合はfloat32とfloat64を受け取り、uint8を1に戻し、それ以外の場合は0を返します。 nanの場合は0が返されることに注意してください。

## ifNotFinite {#ifnotfinite}

浮動小数点値が有限かどうかをチェックします。

**構文**

    ifNotFinite(x,y)

**パラメータ**

-   `x` — Value to be checked for infinity. Type: [フロート\*](../../sql-reference/data-types/float.md).
-   `y` — Fallback value. Type: [フロート\*](../../sql-reference/data-types/float.md).

**戻り値**

-   `x` もし `x` 有限です。
-   `y` もし `x` 有限ではない。

**例えば**

クエリ:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

結果:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

同様の結果を得るには、次のようにします [三項演算子](conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## isNaN(x) {#isnanx}

引数がnanの場合はfloat32とfloat64を受け取り、uint8を1に返します。

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

データベース名、テーブル名、列名などの定数文字列を受け入れます。 列がある場合はuint8定数式を1に、それ以外の場合は0を返します。 hostnameパラメーターを設定すると、テストはリモートサーバーで実行されます。
テーブルが存在しない場合、関数は例外をスローします。
入れ子になったデータ構造内の要素の場合、この関数は列の存在をチェックします。 入れ子になったデータ構造自体の場合、関数は0を返します。

## バー {#function-bar}

ユニコードアート図を作成できます。

`bar(x, min, max, width)` に比例する幅を持つバンドを描画します `(x - min)` とに等しい `width` 文字の場合 `x = max`.

パラメータ:

-   `x` — Size to display.
-   `min, max` — Integer constants. The value must fit in `Int64`.
-   `width` — Constant, positive integer, can be fractional.

バンドは、シンボルの第八に精度で描かれています。

例えば:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## 変換 {#transform}

いくつかの要素の明示的に定義されたマッピングに従って値を他の要素に変換します。
この関数には二つの違いがあります:

### transform(x,array\_from,array\_to,デフォルト) {#transformx-array-from-array-to-default}

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in ‘from’ に。

`default` – Which value to use if ‘x’ 値のいずれにも等しくありません。 ‘from’.

`array_from` と `array_to` – Arrays of the same size.

タイプ:

`transform(T, Array(T), Array(U), U) -> U`

`T` と `U` 数値、文字列、または日付または日時の型を指定できます。
同じ文字（tまたはu）が示されている場合、数値型の場合、これらは一致する型ではなく、共通の型を持つ型である可能性があります。
たとえば、最初の引数はint64型を持つことができ、二番目の引数はarray(uint16)型を持つことができます。

この ‘x’ 値は、次のいずれかの要素に等しくなります。 ‘array\_from’ 配列の場合は、既存の要素(同じ番号が付けられています)を返します。 ‘array\_to’ 配列だ それ以外の場合は、 ‘default’. 一致する要素が複数ある場合 ‘array\_from’、それはマッチのいずれかを返します。

例えば:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### トランスフォーム(x,array\_from,array\_to) {#transformx-array-from-array-to}

最初のバリエーションとは異なります ‘default’ 引数は省略する。
この ‘x’ 値は、次のいずれかの要素に等しくなります。 ‘array\_from’ 配列の場合は、マッチする要素(同じ番号を付けられた要素)を返します。 ‘array\_to’ 配列だ それ以外の場合は、 ‘x’.

タイプ:

`transform(T, Array(T), Array(T)) -> T`

例えば:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## formatReadableSize(x) {#formatreadablesizex}

サイズ(バイト数)を受け入れます。 サフィックス(kib、mibなど)を含む丸められたサイズを返します。)文字列として。

例えば:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## 少なくとも(a,b) {#leasta-b}

Aとbの最小値を返します。

## 最大(a,b) {#greatesta-b}

Aとbの最大値を返します。

## アップタイム() {#uptime}

サーバーの稼働時間を秒単位で返します。

## バージョン() {#version}

サーバーのバージョンを文字列として返します。

## タイムゾーン() {#timezone}

サーバーのタイムゾーンを返します。

## bloknumber {#blocknumber}

行があるデータブロックのシーケンス番号を返します。

## rowNumberInBlock {#function-rownumberinblock}

データブロック内の行の序数を返します。 異なるデータブロックは常に再計算されます。

## rowNumberInAllBlocks() {#rownumberinallblocks}

データブロック内の行の序数を返します。 この機能のみを考慮した影響のデータブロックとなります。

## 隣人 {#neighbor}

指定された列の現在の行の前または後に来る指定されたオフセットで行へのアクセスを提供するウィンドウ関数。

**構文**

``` sql
neighbor(column, offset[, default_value])
```

関数の結果は、影響を受けるデータブロックと、ブロック内のデータの順序によって異なります。
ORDER BYを使用してサブクエリを作成し、サブクエリの外部から関数を呼び出すと、期待される結果を得ることができます。

**パラメータ**

-   `column` — A column name or scalar expression.
-   `offset` — The number of rows forwards or backwards from the current row of `column`. [Int64](../../sql-reference/data-types/int-uint.md).
-   `default_value` — Optional. The value to be returned if offset goes beyond the scope of the block. Type of data blocks affected.

**戻り値**

-   の値 `column` で `offset` 現在の行からの距離 `offset` 値はブロック境界の外側ではありません。
-   のデフォルト値 `column` もし `offset` 値はブロック境界の外側です。 もし `default_value` 与えられ、それが使用されます。

型:影響を受けるデータブロックの種類または既定値の種類。

**例えば**

クエリ:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

結果:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

クエリ:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

結果:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

この関数は、年間指標の値を計算するために使用できます:

クエリ:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

結果:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## ランニングダイファレンス(x) {#other_functions-runningdifference}

Calculates the difference between successive row values ​​in the data block.
最初の行に対して0を返し、後続の各行に対して前の行との差を返します。

関数の結果は、影響を受けるデータブロックと、ブロック内のデータの順序によって異なります。
ORDER BYを使用してサブクエリを作成し、サブクエリの外部から関数を呼び出すと、期待される結果を得ることができます。

例えば:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

ご注意-ブロックサイズは結果に影響します。 それぞれの新しいブロックでは、 `runningDifference` 状態がリセットされます。

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

``` sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstvalue {#runningdifferencestartingwithfirstvalue}

と同じ [runningDifference](./other-functions.md#other_functions-runningdifference)、差は、最初の行の値であり、最初の行の値を返し、後続の各行は、前の行からの差を返します。

## マクナムトストリング(num) {#macnumtostringnum}

UInt64番号を受け取ります。 ビッグエンディアンのMACアドレスとして解釈します。 対応するMACアドレスをAA:BB:CC:DD:EE:FF形式で含む文字列を返します。

## MACStringToNum(s) {#macstringtonums}

MACNumToStringの逆関数。 MACアドレスに無効な形式がある場合は、0を返します。

## MACStringToOUI(s) {#macstringtoouis}

AA:BB:CC:DD:EE:FF形式のMACアドレスを受け付けます。 最初の三つのオクテットをUInt64の数値として返します。 MACアドレスに無効な形式がある場合は、0を返します。

## getSizeOfEnumType {#getsizeofenumtype}

フィールドの数を返します [列挙型](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**パラメータ:**

-   `value` — Value of type `Enum`.

**戻り値**

-   を持つフィールドの数 `Enum` 入力値。
-   型が型でない場合は、例外がスローされます `Enum`.

**例えば**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## blockSerializedSize {#blockserializedsize}

（圧縮を考慮せずに）ディスク上のサイズを返します。

``` sql
blockSerializedSize(value[, value[, ...]])
```

**パラメータ:**

-   `value` — Any value.

**戻り値**

-   値のブロックのためにディスクに書き込まれるバイト数(圧縮なし)。

**例えば**

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName {#tocolumntypename}

RAM内の列のデータ型を表すクラスの名前を返します。

``` sql
toColumnTypeName(value)
```

**パラメータ:**

-   `value` — Any type of value.

**戻り値**

-   を表すために使用されるクラスの名前を持つ文字列 `value` RAMのデータ型。

**違いの例`toTypeName ' and ' toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

この例では、 `DateTime` データタイプはメモリに記憶として `Const(UInt32)`.

## dumpColumnStructure {#dumpcolumnstructure}

RAM内のデータ構造の詳細な説明を出力します

``` sql
dumpColumnStructure(value)
```

**パラメータ:**

-   `value` — Any type of value.

**戻り値**

-   を表すために使用される構造体を記述する文字列。 `value` RAMのデータ型。

**例えば**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

データ型の既定値を出力します。

ユーザーが設定したカスタム列の既定値は含まれません。

``` sql
defaultValueOfArgumentType(expression)
```

**パラメータ:**

-   `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**戻り値**

-   `0` 数字のために.
-   文字列の空の文字列。
-   `ᴺᵁᴸᴸ` のために [Nullable](../../sql-reference/data-types/nullable.md).

**例えば**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## 複製 {#other-functions-replicate}

単一の値を持つ配列を作成します。

内部実装のために使用される [arrayJoin](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**パラメータ:**

-   `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
-   `x` — The value that the resulting array will be filled with.

**戻り値**

値で満たされた配列 `x`.

タイプ: `Array`.

**例えば**

クエリ:

``` sql
SELECT replicate(1, ['a', 'b', 'c'])
```

結果:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable {#filesystemavailable}

返金額の残存スペースのファイルシステムのファイルのデータベースはあります。 それは常に合計空き領域よりも小さいです ([filesystemFree](#filesystemfree) でもスペースはOS.

**構文**

``` sql
filesystemAvailable()
```

**戻り値**

-   バイト単位で使用可能な残りのスペースの量。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

**例えば**

クエリ:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

結果:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## filesystemFree {#filesystemfree}

データベースのファイルがあるファイルシステム上の空き領域の合計を返します。 また見なさい `filesystemAvailable`

**構文**

``` sql
filesystemFree()
```

**戻り値**

-   バイト単位の空き領域の量。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

**例えば**

クエリ:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

結果:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## filesystemCapacity {#filesystemcapacity}

ファイルシステムの容量をバイト単位で返します。 評価のために、 [パス](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) データディレク

**構文**

``` sql
filesystemCapacity()
```

**戻り値**

-   ファイルシステムの容量情報(バイト単位)。

タイプ: [UInt64](../../sql-reference/data-types/int-uint.md).

**例えば**

クエリ:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

結果:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## finalizeAggregation {#function-finalizeaggregation}

集約関数の状態を取ります。 集計結果を返します(ファイナライズされた状態)。

## runningAccumulate {#function-runningaccumulate}

集約関数の状態を取り、値を持つ列を返します,ブロックラインのセットのためにこれらの状態の蓄積の結果であります,最初から現在の行へ.これ
たとえば、集計関数の状態（例：runningaccumulate（uniqstate（userid）））を取り、ブロックの各行について、前のすべての行と現在の行の状態をマージしたときの集計関数の結果を返しま
したがって、関数の結果は、ブロックへのデータの分割とブロック内のデータの順序に依存します。

## joinGet {#joinget}

この関数を使用すると、テーブルからのデータと同じ方法でデータを抽出できます [辞書](../../sql-reference/dictionaries/index.md).

データの取得 [参加](../../engines/table-engines/special/join.md#creating-a-table) 指定された結合キーを使用するテーブル。

サポートされているのは、 `ENGINE = Join(ANY, LEFT, <join_keys>)` 声明。

**構文**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**パラメータ**

-   `join_storage_table_name` — an [識別子](../syntax.md#syntax-identifiers) 検索が実行される場所を示します。 識別子は既定のデータベースで検索されます(パラメータを参照 `default_database` の設定ファイル)。 デフォル `USE db_name` またはを指定しデータベースのテーブルのセパレータ `db_name.db_table`、例を参照してください。
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**戻り値**

キーのリストに対応する値のリストを返します。

ソーステーブルに特定のものが存在しない場合 `0` または `null` に基づいて返されます [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) 設定。

詳細について `join_use_nulls` で [結合操作](../../engines/table-engines/special/join.md).

**例えば**

入力テーブル:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

クエリ:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

結果:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model\_name, …) {#function-modelevaluate}

外部モデルを評価します。
モデル名とモデル引数を受け取ります。 float64を返します。

## throwIf(x\[,custom\_message\]) {#throwifx-custom-message}

引数がゼロ以外の場合は例外をスローします。
custom\_message-オプションのパラメータです。

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## id {#identity}

引数として使用されたのと同じ値を返します。 デバッグに使用され、試験が可能でャックのクエリーの性能を満たします。 がクエリーの分析のために利用できる可能性指標分析装置が外部サンプリング方式な見てみよう `identity` 機能。

**構文**

``` sql
identity(x)
```

**例えば**

クエリ:

``` sql
SELECT identity(42)
```

結果:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## randomprintableasii {#randomascii}

のランダムなセットを持つ文字列を生成します [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) 印刷可能な文字。

**構文**

``` sql
randomPrintableASCII(length)
```

**パラメータ**

-   `length` — Resulting string length. Positive integer.

        If you pass `length < 0`, behavior of the function is undefined.

**戻り値**

-   のランダムなセットを持つ文字列 [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) 印刷可能な文字。

タイプ: [文字列](../../sql-reference/data-types/string.md)

**例えば**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/other_functions/) <!--hide-->
