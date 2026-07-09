---
description: 'テキスト内の検索語をすばやく見つけます。'
keywords: ['全文検索', 'テキスト索引', '索引', '索引']
sidebar_label: 'テキスト索引による全文検索'
slug: /engines/table-engines/mergetree-family/textindexes
title: 'テキスト索引による全文検索'
doc_type: 'reference'
---

テキスト索引 ([転置索引](https://en.wikipedia.org/wiki/Inverted_index)とも呼ばれます) を使用すると、テキストデータに対して高速な全文検索を実行できます。
テキスト索引には、トークンと、そのトークンを含む行番号との対応関係が格納されます。
トークンは、トークン化と呼ばれる処理によって生成されます。
たとえば、ClickHouseのデフォルトのトークナイザーは、英語の文 &quot;The cat likes mice.&quot; を [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;] というトークンに変換します。

例として、1 つのカラムと 3 行からなるテーブルを考えます

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

対応するトークンは以下のとおりです：

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

通常は大文字と小文字を区別せずに検索するため、トークンを小文字に変換します。

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

また、ほぼすべての行に現れる &quot;I&quot;、&quot;the&quot;、&quot;and&quot; のような不要語も削除します。

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

テキスト索引には、概念的には次の情報が含まれます。

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

検索トークンを指定すると、この索引構造によって一致するすべての行をすばやく見つけることができます。

<div id="creating-a-text-index">
  ## テキスト索引の作成
</div>

テキスト索引は、ClickHouse バージョン 26.2 以降で一般提供 (GA) されています。
これらのバージョンでは、テキスト索引を使用するために特別な設定は必要ありません。
本番環境での利用には、ClickHouse バージョン &gt;= 26.2 の使用を強く推奨します。

:::note
テキスト索引は、[compatibility](../../../operations/settings/settings#compatibility) 設定に関係なく、ClickHouse バージョン &gt;= 26.2 で使用できます。
:::

テキスト索引を作成するには、次の構文を使用します。

```sql title="Query"
CREATE TABLE table
(
    key UInt64,
    str String,
    INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )
)
ENGINE = MergeTree
ORDER BY key
```

テキスト索引は、以下の型のカラムに定義できます。

* [String](/ja/sql-reference/data-types/string.md) および [FixedString](/ja/sql-reference/data-types/fixedstring.md)
* [Array(String)](/ja/sql-reference/data-types/array.md) および [Array(FixedString)](/ja/sql-reference/data-types/array.md)
* [Map](/ja/sql-reference/data-types/map.md) ([mapKeys](/ja/sql-reference/functions/tuple-map-functions.md/#mapKeys) 関数および [mapValues](/ja/sql-reference/functions/tuple-map-functions.md/#mapValues) 関数経由)
* [JSON](/ja/sql-reference/data-types/newjson.md) ([JSONAllPaths](/ja/sql-reference/functions/json-functions.md/#JSONAllPaths) 関数および [`JSONAllValues`](/ja/sql-reference/functions/json-functions.md#JSONAllValues) 関数経由)

[Nullable(T)](/ja/sql-reference/data-types/nullable.md) 型および [LowCardinality()](/ja/sql-reference/data-types/lowcardinality.md) 型のカラムもサポートされており、`Array(Nullable(String or FixedString))` も含まれます。

また、既存のテーブルにテキスト索引を追加することもできます。

```sql title="Query"
ALTER TABLE table
    ADD INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )

```

既存のテーブルに索引を追加する場合は、既存のテーブルパーツに対してもその索引をマテリアライズすることを推奨します (そうしないと、索引のないパーツの検索では低速な総当たりスキャンにフォールバックします) 。

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

テキスト索引を削除するには、次を実行してください

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**トークナイザー引数 (必須)&#x20;**。`tokenizer` 引数では、使用するトークナイザーを指定します。

* `splitByNonAlpha` は、英数字以外の ASCII 文字で文字列を分割します (関数 [splitByNonAlpha](/ja/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha) を参照) 。
* `splitByString(S)` は、ユーザー定義の区切り文字列 `S` で文字列を分割します (関数 [splitByString](/ja/sql-reference/functions/splitting-merging-functions.md/#splitByString) を参照) 。
  区切り文字列は省略可能なパラメータで指定できます。たとえば、`tokenizer = splitByString([', ', '; ', '\n', '\\'])` のように指定します。
  各文字列は複数文字で構成できます (例の `', '` など) 。
  明示的に指定しない場合 (たとえば `tokenizer = splitByString`) 、デフォルトの区切り文字列リストは単一の空白文字 `[' ']` です。
* `asciiCJK` は、Unicode の単語境界規則を使用して文字列をトークンに分割します ([Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/) に類似) 。ASCII の英数字とアンダースコアは、コネクタを含むトークンを形成します (文字には ASCII `:`、同種の文字には `.` と `'` を使用) 。[CJK](https://en.wikipedia.org/wiki/CJK_characters) 文字を含む非 ASCII の Unicode 文字は、1 文字単位のトークンになります。
* `ngrams(N)` は、文字列を同じ長さの `N`-gram に分割します (関数 [ngrams](/ja/sql-reference/functions/splitting-merging-functions.md/#ngrams) を参照) 。
  ngram の長さは、1 から 8 までの省略可能な整数パラメータで指定できます。たとえば `tokenizer = ngrams(3)` のように指定します。
  明示的に指定しない場合 (たとえば `tokenizer = ngrams`) 、デフォルトの ngram サイズは 3 です。
* `sparseGrams(min_length, max_length, min_cutoff_length)` は、文字列を `min_length` 文字以上 `max_length` 文字以下 (両端を含む) の可変長 n-gram に分割します (関数 [sparseGrams](/ja/sql-reference/functions/string-functions#sparseGrams) を参照) 。
  明示的に指定しない限り、`min_length` と `max_length` のデフォルト値はそれぞれ 3 と 100 です。
  パラメータ `min_cutoff_length` を指定した場合、長さが `min_cutoff_length` 以上の n-gram のみが返されます。
  `ngrams(N)` と比べると、`sparseGrams` トークナイザーは可変長の N-gram を生成するため、元のテキストをより柔軟に表現できます。
  たとえば `tokenizer = sparseGrams(3, 5, 4)` の場合、内部的には入力文字列から 3-gram、4-gram、5-gram を生成しますが、返されるのは 4-gram と 5-gram のみです。
* `array` はトークン化を行いません。つまり、各行の値全体が 1 つのトークンになります (関数 [array](/ja/sql-reference/functions/array-functions.md/#array) を参照) 。

使用可能なすべてのトークナイザーは、[system.tokenizers](../../../operations/system-tables/tokenizers.md) に一覧されています。

:::note
`splitByString` トークナイザーは、区切り文字列を左から右の順に適用して分割します。
そのため、曖昧さが生じることがあります。
たとえば、区切り文字列 `['%21', '%']` を指定すると、`%21abc` は `['abc']` としてトークン化されます。一方、区切り文字列の順序を `['%', '%21']` に入れ替えると、`['21abc']` が出力されます。
多くの場合、長い区切り文字列が優先的に一致するようにするのが望ましいでしょう。
通常は、区切り文字列を長さの降順で渡すことで実現できます。
区切り文字列がたまたま [prefix code](https://en.wikipedia.org/wiki/Prefix_code) を成している場合は、任意の順序で渡せます。
:::

トークナイザーが入力文字列をどのように分割するかを確認するには、[tokens](/ja/sql-reference/functions/splitting-merging-functions.md/#tokens) 関数と [tokensForLikePattern](/ja/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern) 関数を使用できます。

例:

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*非ASCII入力の扱い。*
テキスト索引は、あらゆる言語・文字セットのテキストデータに対して構築できます。
非ASCIIテキストには、CJK文字を含むUnicodeの単語境界を正しく処理できる `asciiCJK` トークナイザーを推奨します。
:::

**プリプロセッサ引数 (省略可)&#x20;**。プリプロセッサは、トークン化の前に入力文字列に適用される式を指します。

プリプロセッサ引数の一般的な使用例は次のとおりです

1. 大文字・小文字を区別しない照合を可能にするための小文字化/大文字化、または case folding。例: [lower](/ja/sql-reference/functions/string-functions.md/#lower)、[lowerUTF8](/ja/sql-reference/functions/string-functions.md/#lowerUTF8)、[caseFoldUTF8](/ja/sql-reference/functions/string-functions.md/#caseFoldUTF8)。
2. UTF-8 の正規化。例: [normalizeUTF8NFC](/ja/sql-reference/functions/string-functions.md/#normalizeUTF8NFC)、[normalizeUTF8NFD](/ja/sql-reference/functions/string-functions.md/#normalizeUTF8NFD)、[normalizeUTF8NFKC](/ja/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC)、[normalizeUTF8NFKD](/ja/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD)、[normalizeUTF8NFKCCasefold](/ja/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold)、[toValidUTF8](/ja/sql-reference/functions/string-functions.md/#toValidUTF8)。
3. アクセントなどの不要な文字や部分文字列の削除または変換。例: [extractTextFromHTML](/ja/sql-reference/functions/string-functions.md/#extractTextFromHTML)、[substring](/ja/sql-reference/functions/string-functions.md/#substring)、[idnaEncode](/ja/sql-reference/functions/string-functions.md/#idnaEncode)、[translate](/ja/sql-reference/functions/string-replace-functions.md/#translate)、[removeDiacriticsUTF8](/ja/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8)。

プリプロセッサ式は、型が [String](/ja/sql-reference/data-types/string.md) または [FixedString](/ja/sql-reference/data-types/fixedstring.md) の入力値を、同じ型の値に変換する必要があります。
テキスト索引が `Nullable(T)` または `LowCardinality(T)` 型のカラム上に構築されている場合、プリプロセッサ式は nullable または low-cardinality の値を受け取れる必要があります (つまり、例外をスローしてはなりません) 。

例:

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

また、プリプロセッサ式は、テキスト索引が定義されているカラムまたは式だけを参照しなければなりません。

例:

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* 許可されません: `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

非決定論的関数の使用は許可されていません。

:::note
プリプロセッサは、原理的には、索引対象のカラムまたは式をプリプロセッサ式でラップするのと同等です。
たとえば、`INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` の `lower` プリプロセッサは、`INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')` で代用できます。
後者の形式には、代用されたプリプロセッサが WHERE 句のフィルタ条件に一致する場合にのみ適用されるという欠点があります。
たとえば、`WHERE hasAllTokens(lower(col), [...])` は一致しますが、`WHERE hasAllTokens(col, [...])` は一致しません。
そのため、より適切な利用体験のために、プリプロセッサ式を使用することを推奨します。
:::

関数 [hasToken](/ja/sql-reference/functions/string-search-functions.md/#hasToken)、[hasAllTokens](/ja/sql-reference/functions/string-search-functions.md/#hasAllTokens)、[hasAnyTokens](/ja/sql-reference/functions/string-search-functions.md/#hasAnyTokens)、および [hasPhrase](/ja/sql-reference/functions/string-search-functions.md/#hasPhrase) は、検索語をまずプリプロセッサで変換し、その後トークン化します。
プリプロセッサはテキスト索引のパスに対してのみ適用されるため、これらの関数の結果は、テキスト索引を使用するクエリと使用しないクエリ (例: `SETTINGS use_skip_indexes = 0`) で異なる場合があることに注意してください。

たとえば、

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, 'Foo');
```

と等価です:

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx lower(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, lower('Foo'));
```

この場合、プリプロセッサ式は配列の各要素を個別に変換します。

例:

```sql title="Query"
CREATE TABLE table
(
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr))

    -- This is not legal:
    INDEX idx_illegal arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = arraySort(arr))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(arr, 'foo');
```

[Map](/ja/sql-reference/data-types/map.md) 型のカラムに構築するテキスト索引でプリプロセッサを定義するには、索引を
マップのキーと値のどちらに対して構築するかを決める必要があります。

例:

```sql title="Query"
CREATE TABLE table
(
    map Map(String, String),
    INDEX idx mapKeys(map)  TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(map)))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'foo');
```

**ポストプロセッサ引数 (省略可)&#x20;**。ポストプロセッサは、トークン化後の各出力トークンに適用される式です。

入力文字列全体をトークナイザーがトークンへ分割する前に変換するプリプロセッサとは異なり、ポストプロセッサはトークンそのものを 1 つずつ処理します。
本質的にトークン単位で行う変換には、これが自然な適用箇所です。

ポストプロセッサ引数の一般的なユースケースは次のとおりです。

1. **ストップワード (極めて頻出するトークン) のフィルタリング**。&quot;the&quot;、&quot;a&quot;、&quot;is&quot; のような非常によく使われるトークンは、検索上の関連性が低い一方で、索引を肥大化させます。
   ポストプロセッサを使えば、これらを空トークンに変換して除外できます。空トークンは無視され、つまり索引には追加されません。
   例: `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **タイムスタンプの除去**。ログ行は、`2024-01-15T10:23:45` のような構造化されたタイムスタンプで始まることや、それを含むことがよくあります。
   タイムスタンプのトークンを索引化すると、検索上の関連性がない文字列で索引が膨らみます。
   タイムスタンプを無視するには、相補的な 2 つの方法があります。
   * **ポストプロセッサ方式**: `splitByString` トークナイザー (空白で分割) を使ってタイムスタンプ全体を 1 つのトークンにし、その後 `parseDateTimeOrNull` で検出して除外します。
     例: `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     タイムゾーンオフセットや小数秒を含むタイムスタンプには、明示的な format string を指定せずに `parseDateTimeBestEffortOrNull(str)` を使います。
   * **プリプロセッサ方式**: トークン化の*前*に、正規表現を使ってログ行全体からタイムスタンプを取り除きます。
     例: `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     この方法はどのトークナイザーでも機能し、タイムスタンプの文字がそもそもトークン化されないため、より効率的です。
     2 つの方法は組み合わせることもできます。たとえば、プリプロセッサでタイムスタンプを除去しつつ、ポストプロセッサで残りのトークンを正規化またはフィルタリングできます (小文字化 + `ERROR` や `INFO` のような重大度を表す語の除去など) 。
3. **ステミング**。各トークンを語幹に対応付けることで、同じ語根を共有する語形の違いにも一致できるようになり、検索の再現率が向上します。
   たとえば英語のステミングでは、&quot;running&quot;、&quot;runs&quot;、&quot;run&quot; はいずれも &quot;run&quot; に語幹化されるため、これらのどの語形でクエリしてもすべてに一致します。
   ClickHouse には、いくつかの言語向けに組み込みの [stem](/ja/sql-reference/functions/string-functions.md/#stem) 関数があります。
   例: `stem(str, 'en')`
4. **大文字小文字の正規化**。大文字小文字を区別しない一致を実現するために、トークンを小文字または大文字に変換します。例: [lower](/ja/sql-reference/functions/string-functions.md/#lower)、[lowerUTF8](/ja/sql-reference/functions/string-functions.md/#lowerUTF8)。
   小文字化や大文字化には、ポストプロセッサではなくプリプロセッサの使用を推奨します。

ポストプロセッサ式は、型が [String](/ja/sql-reference/data-types/string.md) のトークンを同じ型のトークンに変換します。
また、ポストプロセッサ式が参照できるのは、テキスト索引の定義対象となっているカラムまたは式だけです。
カラムの型が `Array(String)` の場合でも、ポストプロセッサは個々のトークンを通常の `String` 値として処理します。

非決定論的関数の使用は禁止されています。

ポストプロセッサは、索引の構築時に生成される各トークンに適用されます (`array` トークナイザーでは、各配列要素が 1 つのトークンになります) 。クエリ時の動作は、関数によって異なります。

* `hasToken`、`hasAllTokens`、`hasAnyTokens`、`hasPhrase` (サポートされている任意のトークナイザーを使用) では、ポストプロセッサは検索対象のトークンと検索 needle の両方に適用されるため、完全に正規化されたマッチング (たとえば、大文字と小文字を区別しない検索) が可能になります。`hasPhrase` では、ポストプロセッサ適用後のトークンが隙間なく配置されるため、ポストプロセッサによってトークンが削除されても位置上のギャップは生じず、そのトークンをまたいでフレーズが一致します。たとえば、`the` を削除するストップワード用ポストプロセッサを使用すると、`hasPhrase(col, 'see cat')` はドキュメント `see the cat` に一致します。
* その他すべての関数 (`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`) では、索引ヒントのルックアップに対してのみ検索 needle にポストプロセッサが適用され、行レベルの述語では引き続き元のカラム値と比較されます。

例:

* ポストプロセッサ式を使用してストップワードを削除する:

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)
    )
)
ENGINE = MergeTree
ORDER BY tuple();
```

* ポストプロセッサ式を使用してタイムスタンプを削除します:

```sql
-- Log lines: '2024-01-15T10:23:45 ERROR connection failed'
-- The splitByString tokenizer (default: whitespace) keeps the full timestamp as one token.
-- parseDateTimeOrNull detects and drops it; non-timestamp words are kept.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByString',
        postprocessor = if(isNull(parseDateTimeOrNull(line, '%Y-%m-%dT%H:%i:%S')), line, '')
    )
)
ENGINE = MergeTree ORDER BY id;

-- Only message-level words are indexed; timestamp tokens are not stored.
SELECT count() FROM logs WHERE hasAllTokens(line, ['ERROR']);       -- fast index lookup
SELECT count() FROM logs WHERE hasAllTokens(line, ['2024-01-15T10:23:45']);  -- returns 0: token was never indexed
```

* プリプロセッサ式でタイムスタンプを削除します:

```sql
-- The preprocessor strips the ISO timestamp prefix before tokenization.
-- Any tokenizer can be used; timestamp characters are never seen by the tokenizer.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer   = 'splitByNonAlpha',
        preprocessor = replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')
    )
)
ENGINE = MergeTree ORDER BY id;
```

* プリプロセッサ式とポストプロセッサ式を組み合わせて、タイムスタンプを削除します:

```sql
-- Preprocessor strips the timestamp, then lowercases the remainder.
-- Postprocessor drops the severity word (error, info, warn, debug) after tokenization.
-- Result: only substantive message words are stored in the index.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByNonAlpha',
        preprocessor = lower(replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')),
        postprocessor = if(line IN ('error', 'info', 'warn', 'warning', 'debug', 'critical'), '', line)
    )
)
ENGINE = MergeTree ORDER BY id;

-- Example log line: '2024-01-15T10:23:45 ERROR connection failed'
-- After preprocessor:  'error connection failed'
-- After tokenization:  ['error', 'connection', 'failed']
-- After postprocessor: ['connection', 'failed']   ← 'error' dropped as severity word
SELECT count() FROM logs WHERE hasAllTokens(line, ['connection']);
```

* トークンの語幹化にポストプロセッサ式を使用します:

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = stem(str, 'en')
    )
)
ENGINE = MergeTree
ORDER BY tuple();

-- The query token 'running' is stemmed to 'run' before the lookup,
-- matching rows that contain 'run', 'runs', 'ran', 'running', etc.
SELECT count() FROM table WHERE hasAllTokens(str, ['running']);
```

**関数のサポート**。

テキスト索引を参照する述語では、索引ルックアップで索引構築時に格納されたものと同じトークンが使われるように、グラニュールレベルのチェックの前に検索値にプリプロセッサとポストプロセッサが適用されます。
ほとんどの関数 (`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`) では、テキスト索引は無関係なデータブロックをスキップするためにのみ使われます。ClickHouse はその後も、元のカラムデータに対して元の述語を使い、残った各行を検証します。
トークン検索関数 (`hasToken`, `hasAllTokens`, `hasAnyTokens`) では、テキスト索引がプライマリの評価経路になります。ClickHouse は、索引構築時に適用されたものと同じプリプロセッサ、トークナイザー、ポストプロセッサを通して needle を正規化し、この正規化済みの形式を、索引付き・索引なしの両方のテーブルパーツで使用します。ポストプロセッサがある場合、haystack のトークンもクエリ時に正規化されます (`array` だけでなく、すべてのトークナイザーが対象です) 。そのため、比較の両側が一貫して変換され、結果は、索引を直接読むかどうか (設定 `query_plan_direct_read_from_text_index`) 、または特定のパーツにマテリアライズされた索引があるかどうかに依存しません。たとえば、`lower` ポストプロセッサを使うと、`hasAllTokens(col, ['FOO'])` で大文字小文字を区別しないマッチングを有効にできます。
`positions` がない場合、`hasPhrase` は索引をヒントとしてのみ使用し、残った各行を元の述語で検証します。さらに、ポストプロセッサはフレーズと haystack のトークンの両方を同じ方法で正規化するため、結果は読み取り経路に依存せず、ポストプロセッサが削除したトークンによってフレーズの隣接関係が崩れることもありません。`positions = 1` の場合、`hasPhrase` は正確な direct read を使用します (ポストプロセッサがある場合は、引き続きそれも適用されます) 。
ポストプロセッサによって空文字列に変換される検索トークンは無視され、つまり検索フレーズには存在しないものとして扱われます。

| 関数                                                                                          | プリプロセッサ対応                    | 対応トークナイザー                                                | ポストプロセッサ対応 |
| ------------------------------------------------------------------------------------------- | ---------------------------- | -------------------------------------------------------- | ---------- |
| `=`                                                                                         | はい                           | すべて                                                      | はい         |
| `IN`                                                                                        | はい                           | すべて                                                      | はい         |
| [hasToken](/ja/sql-reference/functions/string-search-functions.md/#hasToken)                   | はい                           | すべて (`splitByNonAlpha` 向けに設計)                            | はい         |
| [hasAnyTokens(col, str)](/ja/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | はい                           | すべて                                                      | はい         |
| [hasAllTokens(col, str)](/ja/sql-reference/functions/string-search-functions.md/#hasAllTokens) | はい                           | すべて                                                      | はい         |
| [hasAnyTokens(col, arr)](/ja/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | いいえ (配列要素がそのままトークンとして扱われます)  | すべて                                                      | はい         |
| [hasAllTokens(col, arr)](/ja/sql-reference/functions/string-search-functions.md/#hasAllTokens) | いいえ (配列要素がそのままトークンとして扱われます)  | すべて                                                      | はい         |
| [hasPhrase](/ja/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | はい                           | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | はい         |
| [startsWith](/ja/sql-reference/functions/string-functions.md/#startsWith)                      | はい                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | はい         |
| [endsWith](/ja/sql-reference/functions/string-functions.md/#endsWith)                          | はい                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | はい         |
| [like](/ja/sql-reference/functions/string-search-functions.md/#like)                           | はい¹                          | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | はい¹        |
| [match](/ja/sql-reference/functions/string-search-functions.md/#match)                         | はい¹                          | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | はい¹        |
| [ilike](/ja/sql-reference/functions/string-search-functions.md/#like)                          | はい² (`lower`/`upper` のみ)     | `splitByNonAlpha`, `array`²                              | いいえ²       |
| [mapContainsKey](/ja/sql-reference/functions/tuple-map-functions#mapContainsKey)               | はい                           | すべて                                                      | はい         |
| [mapContainsValue](/ja/sql-reference/functions/tuple-map-functions#mapContainsValue)           | はい                           | すべて                                                      | はい         |
| [mapContainsKeyLike](/ja/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | はい                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | はい         |
| [mapContainsValueLike](/ja/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | はい                           | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | はい         |
| [has](/ja/sql-reference/functions/array-functions.md/#has)                                     | はい                           | `array`                                                  | はい         |
| [hasAny](/ja/sql-reference/functions/array-functions.md/#hasAny)                               | はい                           | `array`                                                  | はい         |
| [hasAll](/ja/sql-reference/functions/array-functions.md/#hasAll)                               | はい                           | `array`                                                  | はい         |

¹ `LIKE` と `match` は、記載されたトークナイザーではヒントとして direct read を使用し、それ以外では総当たりスキャンにフォールバックします。
`LIKE` はさらに、プリプロセッサおよびポストプロセッサを使用しない `splitByNonAlpha` と `array` トークナイザーで、*direct read (ヒントなし)&#x20;*&#x20;(`use_text_index_like_evaluation_by_dictionary_scan` で有効化) もサポートします。

² `ILIKE` は direct read (ヒントなし) でのみサポートされます (`use_text_index_like_evaluation_by_dictionary_scan = 1`、`splitByNonAlpha` または `array` トークナイザー) 。
索引をヒントとして使うフォールバックはありません。設定が無効か、トークナイザーがサポート対象外の場合、`ILIKE` で索引は使用されません。
プリプロセッサがある場合は `lower` または `upper` である必要があります。ポストプロセッサはサポートされません。

**Experimental: 位置引数 (オプション)&#x20;**。

実験的なパラメータ `positions` (デフォルト: `0`) は、索引がトークン位置を格納するかどうかを制御します。
`1` に設定すると、索引は位置データ (`.pos` ファイル内) も追加で格納するようになり、[`hasPhrase`](#functions-example-hasphrase) 関数で direct read による厳密なフレーズ一致が可能になります。
位置を格納すると、索引のディスク上のサイズと書き込みコストが増えるため、この機能は明示的に有効化する必要があります。
ディスク上のフォーマットはまだ stable ではないため、このパラメータは Experimental であり、将来の release で変更される可能性があります。
したがって、`positions = 1` で索引を作成するには、MergeTree setting [`allow_experimental_text_index_positions`](/ja/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) を enabled にする必要があります。
posting list のみの格納を維持するには、`positions = 0` (デフォルト) を設定します。この引数を指定せずに作成されたテキスト索引には、位置情報は含まれません。

:::warning
この引数は Experimental であり、テスト用途でのみ使用してください。
位置情報の格納を有効にするには、MergeTree setting [`allow_experimental_text_index_positions`](/ja/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) を設定してください。
:::

<details markdown="1">
  <summary>省略可能な高度なパラメータ</summary>

  以下の高度なパラメータのデフォルト値は、ほぼすべての状況で適切に機能します。
  これらを変更することは推奨しません。

  省略可能なパラメータ `dictionary_block_size` (デフォルト: 512) は、Dictionary block のサイズを行数で指定します。

  省略可能なパラメータ `dictionary_block_frontcoding_compression` (デフォルト: 1) は、Dictionary block で front coding を圧縮として使用するかどうかを指定します。

  省略可能なパラメータ `posting_list_block_size` (デフォルト: 1048576) は、posting list block のサイズを行数で指定します。

  省略可能なパラメータ `posting_list_codec` (デフォルト: `none`) は、posting list に使用する codec を指定します。

  * `none` - posting list は追加の圧縮なしで格納されます。
  * `bitpacking` - [差分 (delta) coding](https://en.wikipedia.org/wiki/Delta_encoding) を適用した後、[bit-packing](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) を適用します (いずれも固定サイズの block ごと) 。SELECT queries が遅くなるため、現時点では推奨されません。

  上記の高度なパラメータは、対応する MergeTree settings を通じて table レベルで設定することもできます: [`text_index_dictionary_block_size`](/ja/operations/settings/merge-tree-settings#text_index_dictionary_block_size)、[`text_index_dictionary_block_frontcoding_compression`](/ja/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression)、[`text_index_posting_list_block_size`](/ja/operations/settings/merge-tree-settings#text_index_posting_list_block_size)、および [`text_index_posting_list_codec`](/ja/operations/settings/merge-tree-settings#text_index_posting_list_codec)。
  これらは、パラメータを明示的に指定していない table のすべてのテキスト索引に適用されます。

  table レベルの settings の主な Use case は、すべての table parts でテキスト索引を削除して再作成することなく、既存の table の索引パラメータを変更することです。
  table レベルの setting を変更すると、新しいパラメータは新しいパーツ向けに構築されるテキスト索引にのみ適用され、既存のパーツは現在の layout を維持します。

  たとえば、索引定義で指定した引数は table setting よりも優先順位 が高くなります。

  ```sql
  CREATE TABLE table(
      s String,
      -- この索引は 'bitpacking' を使用し、以下の table レベルのデフォルトを上書きします:
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- この索引は table setting から 'none' を継承します:
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*索引 粒度。*
テキスト索引は、ClickHouse では [スキップ索引](/ja/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types) の一種として実装されています。
ただし、他のスキップ索引とは異なり、テキスト索引では無限の 粒度 (1 億) が使用されます。
これは、テキスト索引の table definition で確認できます。

Example:

```sql title="Query"
CREATE TABLE table(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(2)))
ENGINE = MergeTree()
ORDER BY k;

SHOW CREATE TABLE table;
```

```result title="Response"
┌─statement──────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.table                                            ↴│
│↳(                                                                     ↴│
│↳    `k` UInt64,                                                       ↴│
│↳    `s` String,                                                       ↴│
│↳    INDEX idx s TYPE text(tokenizer = ngrams(2)) GRANULARITY 100000000↴│ <-- here
│↳)                                                                     ↴│
│↳ENGINE = MergeTree                                                    ↴│
│↳ORDER BY k                                                            ↴│
│↳SETTINGS index_granularity = 8192                                      │
└────────────────────────────────────────────────────────────────────────┘
```

索引粒度が非常に大きいため、テキスト索引はパート全体を対象に作成されます。
明示的に指定された索引粒度は無視されます。

<div id="using-a-text-index">
  ## テキスト索引の使用
</div>

SELECTクエリでテキスト索引を使うのは簡単で、一般的な文字列検索関数は自動的に索引を利用します。
カラムまたはテーブルパートに索引がない場合、文字列検索関数は低速な総当たりスキャンにフォールバックします。

:::note
テキスト索引の検索には、関数 `hasAnyTokens` と `hasAllTokens` の使用を推奨します。詳しくは[以下](#functions-example-hasanytokens-hasalltokens)を参照してください。
これらの関数は、利用可能なすべてのトークナイザーと、あらゆるプリプロセッサ式およびポストプロセッサ式に対応しています。
一方、その他のサポート対象の関数は歴史的にテキスト索引より前から存在していたため、多くの場合、従来の動作を維持する必要がありました (たとえば、プリプロセッサやポストプロセッサには対応していません) 。
:::

<div id="functions-support">
  ### 対応している関数
</div>

`WHERE` 句または `PREWHERE` 句でテキスト関数を使用する場合は、テキスト索引を使用できます。

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/ja/sql-reference/functions/comparison-functions.md/#equals)) は、指定された検索語全体と一致します。

例:

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/ja/sql-reference/functions/in-functions)) は `equals` に似ていますが、すべての検索語にマッチします。

例:

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
テキスト索引では、`NOT IN` (`notIn`) はサポートされていません。
:::

<div id="functions-example-like-match">
  #### `LIKE` と `match`
</div>

:::note
現在、これらの関数が絞り込みにテキスト索引を使用できるのは、索引のトークナイザーが `splitByNonAlpha`、`ngrams`、`sparseGrams` のいずれかである場合に限られます。
:::

:::note
`NOT LIKE` (`notLike`) はテキスト索引ではサポートされていません。
:::

テキスト索引で `LIKE` ([like](/ja/sql-reference/functions/string-search-functions.md/#like)) および [match](/ja/sql-reference/functions/string-search-functions.md/#match) 関数を使用するには、ClickHouse が検索語から完全なトークンを抽出できる必要があります。
`ngrams` トークナイザーを使用する索引では、ワイルドカードに挟まれた検索文字列の長さが N-gram 長以上であれば、この条件を満たします。

`splitByNonAlpha` トークナイザーを使用するテキスト索引の例:

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

この例の`support`は、`support`、`supports`、`supporting` などに一致する可能性があります。
この種のクエリは部分文字列クエリであり、テキスト索引では高速化できません。

LIKEクエリでテキスト索引を活用するには、LIKEのパターンを次のように書き換える必要があります。

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

`support` の左右にスペースがあることで、この語をトークンとして抽出できます。

幸い、ClickHouse では転置索引を利用して LIKE クエリを大幅に高速化できる特別なケースがあります。

詳しくは、[LIKE/ILIKE パフォーマンスチューニングのセクション](#like-ilike-queries-perf) を参照してください。

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` と `multiMatchAny`
</div>

[multiSearchAny](/ja/sql-reference/functions/string-search-functions.md/#multiSearchAny) と、その UTF-8 バリアントである [multiSearchAnyUTF8](/ja/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) は、複数のリテラルな部分文字列のうち、いずれかが haystack に含まれるかどうかを判定します。[multiMatchAny](/ja/sql-reference/functions/string-search-functions.md/#multiMatchAny) は、複数の正規表現のうち、いずれかに一致するかどうかを判定します。
これらの関数は、`LIKE` および `match` と同じ条件でテキスト索引を使用します (上記を参照) 。つまり、ClickHouse が各 needle から完全な トークン を抽出でき、かつ needles のリストが定数である必要があります。
いずれかの needle が含まれる可能性のある グラニュール は読み取られます。

`multiMatchAny` では、単一の pattern を トークン 要件に落とし込めない場合 (たとえば任意の document に一致する `.*` など) 、テキスト索引は使用できず、クエリはフルスキャンにフォールバックします。

`LIKE` や `match` と同様に、部分文字列検索と正規表現検索は `ngrams` および `sparseGrams` トークナイザーで最も効果的に機能します。
これらのトークナイザーは、重なり合う文字 N-gram を索引化します。そのため、needle は N-gram に分解され、語の途中で始まる場合や終わる場合でも、needle が部分文字列として現れる位置であれば、その N-gram は索引内に存在します。
したがって、needle の長さが N-gram サイズ以上であれば、そのまま使用できます。

`ngrams` トークナイザーを使用したテキスト索引の例:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

一方、`splitByNonAlpha` トークナイザーは、完全なトークン (単語全体) だけを索引化します。
needle は単語の途中で始まったり終わったりすることがあるため、ClickHouse は各 needle の先頭と末尾のトークンを破棄します。したがって、索引がグラニュールを枝刈りできるのは、完全なトークンを使う場合に限られます。
`splitByNonAlpha` で部分文字列検索や正規表現検索に索引を使わせるには、各 needle を区切り文字 (空白など) で囲み、1 つ以上の完全なトークンになるようにします。

`splitByNonAlpha` トークナイザーを使用するテキスト索引の例:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` and `endsWith`
</div>

`LIKE` と同様に、関数 [startsWith](/ja/sql-reference/functions/string-functions.md/#startsWith) と [endsWith](/ja/sql-reference/functions/string-functions.md/#endsWith) がテキスト索引を利用できるのは、検索語から完全なトークンを抽出できる場合に限られます。
`ngrams` トークナイザーを使用する索引では、ワイルドカードで区切られた検索文字列の長さが N-gram 長以上であれば、これに該当します。
テキスト索引でポストプロセッサを使用している場合でも、正規化後に抽出されたヒントトークンが空でなければ、これらの関数は Hint mode で引き続き索引を利用できます。正規化によってヒントトークンがすべて削除された場合、そのpredicateでは索引は使用されません。

`splitByNonAlpha` トークナイザーを使用するテキスト索引の例:

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

この例では、トークンと見なされるのは `clickhouse` のみです。
`support` は `support`、`supports`、`supporting` などに一致する可能性があるため、トークンではありません。

`clickhouse supports` で始まるすべての行を見つけるには、検索パターンの末尾にスペースを入れてください。

```sql
startsWith(comment, 'clickhouse supports ')`
```

同様に、`endsWith` も先頭にスペースを付けて使用してください。

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
`hasToken` は、`splitByNonAlpha` 以外のトークナイザーや、プリプロセッサ / ポストプロセッサ式を使用するテキスト索引でルックアップに使う場合、いくつか注意点があります。
代わりに `hasAnyTokens` と `hasAllTokens` を使用することを推奨します。

大文字と小文字を区別しないバリアントである `hasTokenCaseInsensitive` と `hasTokenCaseInsensitiveOrNull` はテキスト索引に対応していません。テキスト索引付きカラムに対しても、常に全行スキャンとして実行されます。大文字と小文字を区別しないマッチングには、`lower(...)` のプリプロセッサまたはポストプロセッサを使用し、`hasToken` / `hasAllTokens` / `hasAnyTokens` と組み合わせてください。
:::

関数 [hasToken](/ja/sql-reference/functions/string-search-functions.md/#hasToken) は、指定された単一のトークンにマッチします。

前述の関数とは異なり、これらは検索語をトークン化しません (入力が単一のトークンであることを前提とします) 。

例:

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` と `hasAllTokens`
</div>

関数 [hasAnyTokens](/ja/sql-reference/functions/string-search-functions.md/#hasAnyTokens) と [hasAllTokens](/ja/sql-reference/functions/string-search-functions.md/#hasAllTokens) は、指定されたトークンのいずれか、またはすべてに一致するかどうかを判定します。

これら 2 つの関数では、検索トークンとして、索引カラムで使用されているものと同じトークナイザーでトークン化される文字列、または検索前にトークン化を行わない、処理済みトークンの配列を指定できます。
詳細については、各関数のドキュメントを参照してください。

例:

```sql
-- Search tokens passed as string argument
SELECT count() FROM table WHERE hasAnyTokens(comment, 'clickhouse olap');
SELECT count() FROM table WHERE hasAllTokens(comment, 'clickhouse olap');

-- Search tokens passed as Array(String)
SELECT count() FROM table WHERE hasAnyTokens(comment, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAllTokens(comment, ['clickhouse', 'olap']);
```

<div id="functions-example-hasphrase">
  #### `hasPhrase`
</div>

関数 [hasPhrase](/ja/sql-reference/functions/string-search-functions.md/#hasPhrase) はフレーズに対して一致判定を行います。すべてのトークンが連続して、かつ検索文字列内と同じ順序で現れる必要があります。

すべてのトークンがどこかに含まれていればよい `hasAllTokens` とは異なり、`hasPhrase` ではそれらが連続した並びで現れる必要があります。
検索フレーズは、索引カラムに設定されたものと同じトークナイザーを使用してトークン化されます。
テキスト索引でポストプロセッサを使用している場合、検索フレーズも索引ルックアップの前に正規化されます。
この関数を使用するには、`splitByNonAlpha`、`splitByString`、`ngrams`、`asciiCJK` のいずれかのトークナイザーが必要です。

例:

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

Array 関数 [has](/ja/sql-reference/functions/array-functions#has) は、文字列の配列内の単一のトークンに一致します。

例:

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` と `hasAll`
</div>

配列関数 [hasAny](/ja/sql-reference/functions/array-functions#hasAny) と [hasAll](/ja/sql-reference/functions/array-functions#hasAll) は、索引付きの配列カラムに、定数の検索文字列集合のいずれかまたはすべてが含まれているかどうかを判定します。

例:

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

関数 [mapContains](/ja/sql-reference/functions/tuple-map-functions#mapContainsKey) (`mapContainsKey` のエイリアス) は、map のキー内で、検索対象の文字列から抽出されたトークンとの照合を行います。
この挙動は、`String` カラムに対する `equals` 関数に似ています。
テキスト索引は、`mapKeys(map)` 式に対して作成されている場合にのみ使用されます。

例:

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

関数 [mapContainsValue](/ja/sql-reference/functions/tuple-map-functions#mapContainsValue) は、map の値に含まれる検索対象の文字列から抽出されたトークンに一致するかどうかを判定します。
この動作は、`String` カラムに対する `equals` 関数に似ています。
テキスト索引は、`mapValues(map)` 式に対して作成されている場合にのみ使用されます。

例:

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` と `mapContainsValueLike`
</div>

関数 [mapContainsKeyLike](/ja/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) と [mapContainsValueLike](/ja/sql-reference/functions/tuple-map-functions#mapContainsValueLike) は、mapのすべてのキーまたは値 (それぞれ) が指定したパターンに一致するかどうかを判定します。

例:

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

アクセス[operator[]](/ja/sql-reference/operators#access-operators)は、テキスト索引と組み合わせることで、キーと値の絞り込みに使用できます。テキスト索引が使用されるのは、それが`mapKeys(map)`または`mapValues(map)`の式、あるいはその両方に対して作成されている場合に限られます。

例:

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

`Array(T)` 型および `Map(K, V)` 型のカラムでテキスト索引を使用する例については、以下を参照してください。

<div id="text-index-example-array">
  ### Array(String) カラムのインデックス化
</div>

著者がキーワードでブログ記事を分類するブログプラットフォームを想像してみましょう。
ユーザーがトピックを検索したりクリックしたりして、関連するコンテンツを見つけられるようにしたいとします。

次のテーブル定義を考えてみましょう。

```sql
CREATE TABLE posts
(
    post_id UInt64,
    title String,
    content String,
    keywords Array(String)
)
ENGINE = MergeTree
ORDER BY (post_id);
```

テキスト索引がない場合、特定のキーワード (例: `clickhouse`) を含む投稿を見つけるには、全件をスキャンする必要があります。

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

プラットフォームの規模が大きくなるにつれて、クエリはすべての行の `keywords` 配列をすべて調べる必要があるため、処理はますます遅くなります。
このパフォーマンス上の問題を解消するため、`keywords` カラムにテキスト索引を定義します。

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### Mapカラムの索引作成
</div>

多くのオブザーバビリティのユースケースでは、ログメッセージは「要素」に分割され、それぞれ適切なデータ型で保存されます。たとえば、timestamp には日時、log level には enum などが使われます。
メトリクスのフィールドは、キー・バリューのペアとして保存するのが適しています。
運用チームは、デバッグ、セキュリティインシデント、監視のために、ログを効率的に検索できる必要があります。

次の logs テーブルを考えてみましょう。

```sql
CREATE TABLE logs
(
    id UInt64,
    timestamp DateTime,
    message String,
    attributes Map(String, String)
)
ENGINE = MergeTree
ORDER BY (timestamp);
```

テキスト索引がない場合、[Map](/ja/sql-reference/data-types/map.md) データの検索にはフルテーブルスキャンが必要です:

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

ログの量が増えると、これらのクエリは遅くなります。

その対策として、[Map](/ja/sql-reference/data-types/map.md) のキーと値に対するテキスト索引を作成します。
フィールド名や属性の型でログを検索する必要がある場合は、[mapKeys](/ja/sql-reference/functions/tuple-map-functions.md/#mapKeys) を使ってテキスト索引を作成します。

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

属性の実際の内容を検索する必要がある場合は、[mapValues](/ja/sql-reference/functions/tuple-map-functions.md/#mapValues) を使用してテキスト索引を作成します。

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

クエリの例:

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### JSON カラムへの索引付け
</div>

テキスト索引は、`JSON` カラムに対して次の 3 つの方法で使用できます。

1. **特定のサブカラムに対する索引** — 通常のカラムと同様に、既知の JSON パスにテキスト索引を作成します。これにより、そのパス上の*値*が索引付けされます。
2. **[JSONAllPaths](/ja/sql-reference/functions/json-functions.md/#JSONAllPaths) を使用したパスベースの索引** — 各グラニュールに含まれる*すべてのパス*を索引付けし、クエリ対象のパスを含む可能性のないグラニュールをスキップします。`Map` カラムの場合と似ています。
3. **[JSONAllValues](/ja/sql-reference/functions/json-functions.md#JSONAllValues) を使用した値ベースの索引** — すべての JSON パスにまたがる*すべての値*を索引付けし、単一の索引で任意の JSON サブカラムに対する全文検索を高速化します。

<div id="json-indexes-on-subcolumns">
  #### 特定のサブカラムに対する索引
</div>

通常のカラムと同じ構文で、任意の JSON サブカラムにスキップ索引を作成できます。

索引式で JSON サブカラムを参照する方法は 2 つあります。

* **JSON 型ヒント**で宣言した**型付きパス** — 名前で直接アクセスします: `json.a`.
* **明示的なキャスト**を伴う**動的パス** — `::` キャスト構文を使用します: `json.b::String`.

索引定義の例:

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id String),
    INDEX idx_sensor data.sensor_id TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_location data.location::String TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number , 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

クエリ例:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id = 'id_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: text
        Condition: (mode: All; tokens: ["5", "id"])
        Parts: 1/2
        Granules: 1/8
```

クエリの例:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: text
        Condition: (mode: All; tokens: ["5", "room"])
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  #### JSONAllPaths を使用したパスベースの索引
</div>

`Map` カラムと同様に、[JSON](/ja/sql-reference/data-types/newjson.md) カラムにも [`JSONAllPaths`](/ja/sql-reference/functions/json-functions.md/#JSONAllPaths) を使用してテキスト索引を作成できます。
この索引は各グラニュールに存在する JSON パスの集合を保存し、クエリ対象のパスが存在しないグラニュールをスキップするために利用します。

索引定義の例:

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

`EXPLAIN indexes = 1` を使用すると、スキップ索引が使われていることを確認できます。
パスが一方のパートにしか存在しない場合、索引によってもう一方のパートはスキップされます。

例:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

あるパスがどのパーツにも存在しない場合、すべてのパーツとグラニュールはスキップされます。

例:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["nonexistent"])
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` でも索引が使用されます。パス が存在しないグラニュールはスキップされます (その場合、値は `NULL` になるためです) :

例:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallvalues">
  #### JSONAllValues を使った値ベースの索引
</div>

テキスト索引を使用すると、関数 [`JSONAllValues`](/ja/sql-reference/functions/json-functions.md#JSONAllValues) を介して [JSON](/ja/sql-reference/data-types/newjson.md) カラムに対する検索を高速化できます。

`JSONAllValues` は、JSON カラム内のすべての値を `Array(String)` として返します。
文字列以外のデータ型の値 (たとえば整数や配列) は、テキスト表現に変換されます。
`JSONAllValues` を使って構築したテキスト索引は、各行内のすべての JSON パスにまたがるこれらのテキスト表現を索引付けします。
この索引により、個々の JSON サブカラムを条件にしたクエリを高速化できます。
クエリが特定のサブカラム (たとえば `data.user_name = 'alice'`) で絞り込む場合、テキスト索引は、JSON の値のいずれにも検索トークンを含まない行 (およびグラニュール) をすばやくスキップできます。

:::note
異なる JSON パスに同じトークンが含まれている場合、この索引で偽陽性が発生することがあります。
たとえば、行 1 に `{"a": "hello", "b": "world"}` があり、クエリが `data.a = 'world'` を検索する場合、テキスト索引は `world` がパス `a` ではなく `b` に属していることを区別できません。
そのような場合、この索引はその行をスキップせず、実際のカラムデータに対するフィルタが最終的な評価を行います。
これは、索引が高速な事前フィルタとして機能する、他のテキスト索引のユースケースと同じ挙動です。
:::

<div id="json-all-values-creating-the-index">
  ##### 索引の作成
</div>

索引定義の例:

```sql
CREATE TABLE events
(
    id UInt64,
    data JSON,
    INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="json-all-values-supported-query-patterns">
  ##### サポートされるクエリパターン
</div>

索引を作成すると、`String` カラムで使えるのと同じ関数、およびすべてのカラムで使える関数 `equals` を使用して、JSON サブカラムに対するクエリを高速化できます。

サブカラムへのアクセス:

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

明示的な `CAST` を使ったサブカラムへのアクセス:

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

`IN` 演算子:

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### フレーズ検索
</div>

通常のテキスト索引の検索では、たとえば

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

指定されたトークンを任意の順序で含むすべての行に一致します。
この例では、行 `While she stayed in Tokyo, the weather was great.` がフィルターに一致します。

対照的に、フレーズ検索は、指定された順序どおりにトークンが並んでいる場合に一致します。
たとえば、

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

たとえば `How is the weather in Tokyo?` のように、トークン列 `weather in Tokyo` を含む任意の行に一致しますか？

テキスト索引は、フレーズ内のすべてのトークンの posting list の積集合を取り、候補となる granule を特定することで、フレーズ検索を高速化します。
その後、ClickHouse はそれらの granule 内で、トークンが正確に隣接していることを検証します。
この処理は比較的コストが高く、通常のテキスト検索クエリより低速です。
フレーズ検索クエリを高速化するには、テキスト索引で位置情報の保存を有効にしてください (上記の `パラメータ` を参照) 。

`hasPhrase` は、トークナイザー `splitByNonAlpha`、`splitByString`、`ngrams`、`asciiCJK` と組み合わせて使用できます。
指定したフレーズ文字列は、索引で使用しているトークナイザーでトークン化されます。
フレーズ内の区切り文字は無視されます。`splitByNonAlpha` をトークナイザーとして使用している場合、`hasPhrase(text, 'quick+brown')` は `hasPhrase(text, 'quick brown')` と同等です。

<div id="text-index-phrase-search-example">
  #### 例
</div>

```sql
CREATE TABLE tab (
    id UInt32,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'weather in New York'),
    (2, 'New weather in York'),
    (3, 'weather in New Orleans');
```

```sql title="Query"
SELECT id, text FROM tab WHERE hasPhrase(text, 'weather in New York');
```

```result title="Response"
   ┌─id─┬─text────────────────┐
1. │  1 │ weather in New York │
   └────┴─────────────────────┘
```

2 行目 (`'New weather in York'`) は、トークンの順序が異なるため一致しません。
3 行目 (`'weather in New Orleans'`) は、トークン `'York'` を含まないため一致しません。

<div id="performance-tuning">
  ## パフォーマンスチューニング
</div>

<div id="direct-read">
  ### Direct read
</div>

一部の種類のテキスト検索クエリは、&quot;direct read&quot; と呼ばれる最適化によって大幅に高速化できます。

例:

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

direct read 最適化では、基になるテキストカラムにアクセスせず、テキスト索引のみ (つまりテキスト索引のルックアップ) を使ってクエリに応答します。
テキスト索引のルックアップで読み取るデータ量は比較的少ないため、ClickHouse の通常のスキップ索引より大幅に高速です (通常のスキップ索引では、スキップ索引のルックアップに続いて、残りのグラニュールの読み込みとフィルタリングが行われます) 。

direct read は、次の 2 つの設定で制御されます。

* 設定 [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (デフォルトは true) 。direct read を全体として有効にするかどうかを指定します。
* 設定 [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) は、ClickHouse バージョン &lt; 26.4 では direct read の前提条件でした。

**サポートされる関数**

direct read 最適化は、`hasToken`、`hasAllTokens`、`hasAnyTokens` 関数をサポートします。
テキスト索引が `array` トークナイザーで定義されている場合は、`equals`、`has`、`hasAny`、`hasAll`、`mapContainsKey`、`mapContainsValue` 関数でも direct read がサポートされます。
これらの関数は、`AND`、`OR`、`NOT` 演算子で組み合わせることもできます。
また、`WHERE` 句または `PREWHERE` 句には、追加の非テキスト検索関数フィルター (テキストカラムまたは他のカラムに対するもの) を含めることもできます。その場合でも direct read 最適化は使われますが、効果はやや低くなります (適用されるのは、サポートされているテキスト検索関数に対してのみです) 。

クエリで direct read が使われていることを確認するには、`EXPLAIN PLAN actions = 1` を付けてクエリを実行します。
例として、direct read を無効にしたクエリは

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

戻り値

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

これに対し、`query_plan_direct_read_from_text_index = 1` を指定して同じクエリを実行すると

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

戻り値

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

2つ目の EXPLAIN PLAN の出力には、仮想カラム `__text_index_<index_name>_<function_name>_<id>` が含まれます。
このカラムが存在する場合、direct read が使用されています。

WHERE フィルター句にテキスト検索関数のみが含まれている場合、クエリはカラムデータをまったく読み取らずに済むため、direct read によって最大の性能上のメリットが得られます。
ただし、クエリ内の他の箇所でテキストカラムにアクセスしている場合でも、direct read によるパフォーマンス改善は引き続き得られます。

**ヒントとしての Direct read**

ヒントとしての Direct read は、通常の direct read と同じ原理に基づいていますが、基になるテキストカラムを除外する代わりに、テキスト索引データから構築された追加のフィルターを加えます。
これは、テキスト索引だけを読み取ると偽陽性が発生する関数で使用されます。

サポートされている関数は、`like`、`startsWith`、`endsWith`、`equals`、`has`、`hasPhrase`、`mapContainsKey`、`mapContainsValue` です。

この追加フィルターは、他のフィルターと組み合わせることで結果セットをさらに絞り込むための追加の選択性を提供し、他のカラムから読み取るデータ量の削減に役立ちます。

ヒントとしての Direct read は、設定 [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) (デフォルトで有効) で制御されます。

ヒントなしのクエリの例:

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

戻り値

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

一方、同じクエリを `query_plan_text_index_add_hint = 1` で実行すると

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

返す

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

2つ目の EXPLAIN PLAN の出力では、追加の連言項 (`__text_index_...`) がフィルタ条件に加えられていることがわかります。
[PREWHERE](/ja/sql-reference/statements/select/prewhere) の最適化により、フィルタ条件は3つの連言項に分解され、計算コストの低いものから順に適用されます。
このクエリでは、適用順序は `__text_index_...`、次に `greaterOrEquals(...)`、最後に `like(...)` です。
この順序により、`WHERE` 句の後でクエリで使用される重いカラムを読み込む前に、テキスト索引と元のフィルタでスキップされるグラニュールよりもさらに多くのデータグラニュールをスキップでき、読み取るデータ量をいっそう削減できます。

<div id="like-ilike-queries-perf">
  ### LIKE/ILIKE クエリ
</div>

LIKE/ILIKE のクエリパターンが `%<alpha-numeric-characters-without-spaces>%` で、テキスト索引のトークナイザーが `splitByNonAlpha` または `array` の場合、ClickHouse は転置索引を利用して LIKE/ILIKE クエリを大幅に高速化します。これを実現するために、ClickHouse は一致するパターンを見つける際、フルテーブルスキャンする代わりに転置索引の Dictionary をスキャンします。

この最適化を有効にすると、LIKE/ILIKE クエリはフルテーブルスキャンより大幅に高速になるはずです。ただし、パターンが Dictionary 内の大半のトークンに一致する場合は、フルテーブルスキャンよりも性能が低下することがあります。幸い、それを防ぐためのフォールバック機構があります。

この最適化は、次の設定で制御されます。

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

フォールバック機構は、次の 2 つの設定で制御されます。

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

この最適化は、関数 `like` と `ilike` のみをサポートします。

<div id="caching">
  ### キャッシュ
</div>

テキスト索引の一部をメモリ内に保持するために、サーバー全体で利用されるさまざまな cache があります ([実装の詳細](#implementation) のセクションを参照) 。
現在は、I/O を削減するために、テキスト索引のデシリアライズ済みヘッダー、トークン、ポスティングリスト用の cache があります。
設定 [use&#95;text&#95;index&#95;header&#95;cache](/ja/operations/settings/settings#use_text_index_header_cache)、[use&#95;text&#95;index&#95;tokens&#95;cache](/ja/operations/settings/settings#use_text_index_tokens_cache)、[use&#95;text&#95;index&#95;postings&#95;cache](/ja/operations/settings/settings#use_text_index_postings_cache) を使用すると、クエリによる各 cache への読み書きを無効にできます。

cache をクリアするには、ステートメント [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches) を使用します。

cache を設定するには、以下のサーバー設定を参照してください。

<div id="caching-tokens">
  #### トークンcacheの設定
</div>

| 設定                                                                                                                                                  | 説明                                   |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/ja/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | テキスト索引トークンcacheのcacheポリシー名。          |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/ja/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | cacheの最大サイズ (バイト単位) 。                |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/ja/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | cache内のデシリアライズ済みトークンの最大数。            |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/ja/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | テキスト索引トークンcacheの全体サイズに対する、保護キューのサイズ。 |

<div id="caching-header">
  #### ヘッダーcacheの設定
</div>

| Setting                                                                                                                                             | Description                                          |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/ja/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | テキスト索引ヘッダーcacheのcacheポリシー名。                          |
| [text&#95;index&#95;header&#95;cache&#95;size](/ja/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | cacheの最大サイズ (バイト単位) 。                                |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/ja/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | cache内のデシリアライズ済みヘッダーの最大数。                            |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/ja/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | テキスト索引ヘッダーcache内の保護キューのサイズを、cache全体のサイズに対する比率で表したもの。 |

<div id="caching-posting-lists">
  #### ポスティングリストcacheの設定
</div>

| Setting                                                                                                                                                 | Description                                       |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/ja/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | テキスト索引ポスティングリストcacheのキャッシュポリシー名。                       |
| [text&#95;index&#95;postings&#95;cache&#95;size](/ja/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | cacheの最大サイズ (バイト単位) 。                             |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/ja/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | cacheに保持できるデシリアライズ済みポスティングリストの最大数。                |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/ja/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | テキスト索引ポスティングリストcacheにおいて、保護キューがcache全体に占めるサイズの割合。 |

<div id="limitations">
  ## 制限事項
</div>

現在、テキスト索引には次の制限があります。

* トークン数が非常に多いテキスト索引 (例: 100億トークン) のマテリアライズでは、大量のメモリを消費することがあります。テキスト
  索引のマテリアライズは、直接実行される場合 (`ALTER TABLE <table> MATERIALIZE INDEX <index>`) と、パーツのマージ時に間接的に行われる場合があります。
* 4,294,967,296 (= 2^32 = 約42億) 行を超えるパーツでは、テキスト索引をマテリアライズできません。マテリアライズ済みのテキスト索引がない場合、クエリはそのパーツ内で低速な総当たり検索にフォールバックします。最悪ケースの見積もりとして、あるパーツには String 型の単一カラムしか含まれておらず、MergeTree setting `max_bytes_to_merge_at_max_space_in_pool` (デフォルト: 150 GB) が変更されていないと仮定します。この場合、そのカラムの1行あたりの平均文字数が 29.5 文字未満であれば、この状況が発生します。実際には、テーブルには通常ほかのカラムも含まれるため、しきい値はこれよりさらに大幅に小さくなります (ほかのカラムの数、型、サイズによって異なります) 。

<div id="text-index-vs-bloom-filter-indexes">
  ## テキスト索引と Bloom filter ベースの索引の比較
</div>

文字列に対する述語は、テキスト索引や Bloom filter ベースの索引 (索引タイプ `bloom_filter`、`ngrambf_v1`、`tokenbf_v1`、`sparse_grams`) によって高速化できますが、両者は設計思想と想定ユースケースの点で本質的に異なります。

**Bloom filter 索引**

* false positives が生じる可能性のある確率的データ構造に基づいています。
* 集合への所属に関する問い合わせにしか答えられません。つまり、そのカラムにトークン X が含まれている可能性があるか、あるいは X が含まれていないことが確実か、ということだけを判定できます。
* クエリ実行時に大まかな範囲の skipping を可能にするため、granule レベルの情報を保持します。
* 適切なチューニングが難しいです (例については[こちら](mergetree#n-gram-bloom-filter)を参照してください) 。
* 比較的コンパクトです (part あたり数キロバイトから数メガバイト) 。

**テキスト索引**

* トークンに対して決定論的な転置索引を構築します。索引自体によって false positives が発生することはありません。
* テキスト検索 workloads 向けに特化して最適化されています。
* 効率的な用語ルックアップを可能にするため、行レベルの情報を保持します。
* 比較的大きくなります (part あたり数十〜数百メガバイト) 。

Bloom filter ベースの索引がサポートする全文検索は、あくまで「副次的な効果」にすぎません。

* 高度なトークン化や前処理をサポートしていません。
* 複数トークンの検索をサポートしていません。
* 転置索引に期待されるような性能特性は提供しません。

これに対して、テキスト索引は全文検索専用に設計されています。

* トークン化と前処理を提供します
* `hasAllTokens`、`LIKE`、`match` などのテキスト検索関数を効率的にサポートします。
* 大規模なテキストコーパスに対して、scalability が大幅に優れています。

<div id="implementation">
  ## 実装の詳細
</div>

各テキスト索引は、2つの (抽象的な) データ構造で構成されます。

* 各トークンをポスティングリストに対応付ける辞書
* それぞれが行番号の集合を表す、ポスティングリストの集合

テキスト索引はパーツ全体に対して構築されます。
他のスキップ索引とは異なり、テキスト索引はデータパーツのマージ時に再構築するのではなく、マージできます (以下を参照) 。

索引の作成時には、3つのファイルが作成されます (パーツごと) 。

**辞書ブロックファイル (.dct)**

テキスト索引内のトークンはソートされ、512トークンごとの辞書ブロックに格納されます (ブロックサイズはパラメータ `dictionary_block_size` で設定可能です) 。
辞書ブロックファイル (.dct) は、1つのパーツ内のすべてのインデックスグラニュールにある辞書ブロック全体で構成されます。

**インデックスヘッダーファイル (.idx)**

インデックスヘッダーファイルには、各辞書ブロックについて、そのブロックの先頭トークンと辞書ブロックファイル内での相対オフセットが含まれます。

このスパースインデックス構造は、ClickHouse の [スパース主キー索引](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)) に似ています。

**ポスティングリストファイル (.pst)**

すべてのトークンのポスティングリストは、ポスティングリストファイル内に順番に配置されます。
容量を節約しつつ高速な積集合および和集合の操作を可能にするため、ポスティングリストは [roaring bitmaps](https://roaringbitmap.org/) として保存されます。
ポスティングリストが `posting_list_block_size` より大きい場合は、複数のブロックに分割され、ポスティングリストファイルに順次格納されます。

**位置ファイル (.pos)**

省略可能で、インデックス引数 `positions = 1` の場合にのみ作成されます。
一致する行内でのトークンの位置を保存します。

**テキスト索引のマージ**

データパーツがマージされる際、テキスト索引を最初から再構築する必要はありません。代わりに、マージ処理の別ステップで効率的にマージできます。
このステップでは、各入力パーツのテキスト索引のソート済み辞書が読み込まれ、新しい統合辞書に結合されます。
ポスティングリスト内の行番号も、初期マージフェーズで作成された旧行番号から新行番号への対応付けを使って、マージ後のデータパーツ内での新しい位置を反映するよう再計算されます。
このテキスト索引のマージ方法は、`_part_offset` カラムを持つ [projections](/ja/docs/sql-reference/statements/alter/projection#projection-indexes) のマージ方法に似ています。
ソースパーツで索引がマテリアライズされていない場合は、まず索引を構築して一時ファイルに書き込み、その後、ほかのパーツの索引やほかの一時インデックスファイルの索引とまとめてマージされます。

**デバッグ**

テーブル関数 [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) を使用して、テキスト索引の内部を調査できます。

<div id="hacker-news-dataset">
  ## 例: Hacker News データセット
</div>

大量のテキストを含む大規模なデータセットに対して、テキスト索引がもたらすパフォーマンス改善を見ていきます。
人気サイト Hacker News 上のコメント 2,870 万行を使用します。
以下は、テキスト索引がないテーブルです:

```sql
CREATE TABLE hackernews (
    id UInt64,
    deleted UInt8,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    children Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
)
ENGINE = MergeTree
ORDER BY (type, author);
```

S3 内の Parquet ファイルには 28.7M 行あります。これらを `hackernews` テーブルに挿入してみましょう:

```sql
INSERT INTO hackernews
    SELECT * FROM s3Cluster(
        'default',
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
        'Parquet',
        '
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32');
```

`ALTER TABLE` を使用して `comment` カラムにテキスト索引を追加し、その後これをマテリアライズします:

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

では、`hasToken`、`hasAnyTokens`、`hasAllTokens` 関数を使ってクエリを実行してみましょう。
以下の例では、標準的な索引スキャンと direct read 最適化の劇的な性能差を示します。

<div id="using-hasToken">
  ### 1. `hasToken` を使う
</div>

`hasToken` は、テキストに特定の 1 つのトークンが含まれているかどうかを確認します。
大文字と小文字を区別するトークン &#39;ClickHouse&#39; を検索します。

**Direct read 無効時 (Standard scan)&#x20;**&#xA;デフォルトでは、ClickHouse はスキップ索引を使ってグラニュールを絞り込み、その後、それらのグラニュールのカラムデータを読み取ります。
Direct read を無効にすることで、この動作を再現できます。

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.362 sec. Processed 24.90 million rows, 9.51 GB
```

**Direct read 有効時 (Fast index read)&#x20;**&#xA;次に、Direct read を有効にした状態 (デフォルト) で、同じクエリを実行します。

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.008 sec. Processed 3.15 million rows, 3.15 MB
```

direct readクエリは、索引のみを参照するため、45倍以上高速であり (0.362秒に対して0.008秒) 、処理するデータ量も大幅に少なくなります (9.51 GBに対して3.15 MB) 。

<div id="using-hasAnyTokens">
  ### 2. `hasAnyTokens` の使用
</div>

`hasAnyTokens` は、テキストに指定したトークンのうち少なくとも1つが含まれているかどうかを確認します。
&#39;love&#39; または &#39;ClickHouse&#39; を含むコメントを検索します。

**Direct read 無効 (Standard scan)&#x20;**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 1.329 sec. Processed 28.74 million rows, 9.72 GB
```

**Direct read が有効 (Fast index read)&#x20;**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 0.015 sec. Processed 27.99 million rows, 27.99 MB
```

この一般的な&quot;OR&quot;検索では、さらにはっきりとした高速化が見られます。
カラム全体のスキャンを回避することで、クエリはほぼ89倍高速になります (1.329秒 対 0.015秒) 。

<div id="using-hasAllTokens">
  ### 3. `hasAllTokens` の使用
</div>

`hasAllTokens` は、テキストに指定したすべてのトークンが含まれているかどうかを確認します。
&#39;love&#39; と &#39;ClickHouse&#39; の両方を含むコメントを検索します。

**Direct read 無効時 (Standard scan)&#x20;**&#xA;Direct read を無効にしていても、標準のスキップ索引は引き続き有効です。
28.7M 行を 147.46K 行まで絞り込めますが、それでもカラムから 57.03 MB を読み取る必要があります。

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.184 sec. Processed 147.46 thousand rows, 57.03 MB
```

**Direct read が有効 (Fast index read)&#x20;**&#xA;Direct read では索引データを使ってクエリに直接応答するため、読み取るのは 147.46 KB のみです。

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.007 sec. Processed 147.46 thousand rows, 147.46 KB
```

この &quot;AND&quot; 検索では、direct read 最適化は標準的なスキップ索引スキャンと比べて 26 倍以上高速です (0.184s に対して 0.007s) 。

<div id="compound-search">
  ### 4. 複合検索: OR, AND, NOT, ...
</div>

direct read 最適化は、複合的なブール式にも適用されます。
ここでは、&#39;ClickHouse&#39; OR &#39;clickhouse&#39; の大文字と小文字を区別しない検索を実行します。

**direct read 無効 (Standard scan)&#x20;**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.450 sec. Processed 25.87 million rows, 9.58 GB
```

**Direct read 有効 (Fast index read)&#x20;**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.013 sec. Processed 25.87 million rows, 51.73 MB
```

索引の結果を組み合わせることで、direct read クエリは34倍高速になり (0.450秒に対して0.013秒) 、9.58 GBのカラムデータを読み込まずに済みます。
このケースでは、`hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])` のほうが、より効率的で推奨される構文です。

<div id="related-content">
  ## 関連コンテンツ
</div>

* Blog: [ClickHouse 全文検索の一般提供を発表](https://clickhouse.com/blog/full-text-search-ga-release)
* Blog: [オブジェクトストレージ向けの高性能な全文検索を構築する](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* ビデオ: [ClickHouse における全文検索入門](https://www.youtube.com/watch?v=9zPmf1a_heU)
* ビデオ: [舞台裏: ClickHouse のスケールと速度を支える全文検索](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* プレゼンテーション: [ClickHouse 全文検索の内側: 高速でネイティブな列指向](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* プレゼンテーション: [反転索引の概要、必要性、実装方法 — FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**旧資料**

* Blog: [ClickHouse における反転索引の紹介](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* Blog: [ClickHouse 全文検索の内側: 高速でネイティブな列指向](https://clickhouse.com/blog/clickhouse-full-text-search)
* ビデオ: [全文索引: 設計と実験](https://www.youtube.com/watch?v=O_MnyUkrIq8)