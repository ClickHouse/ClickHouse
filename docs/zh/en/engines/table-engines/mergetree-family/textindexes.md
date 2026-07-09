---
description: '快速在文本中查找搜索词。'
keywords: ['full-text search', 'text index', 'index', 'indices']
sidebar_label: '使用文本索引进行全文搜索'
slug: /engines/table-engines/mergetree-family/textindexes
title: '使用文本索引进行全文搜索'
doc_type: 'reference'
---

文本索引 (也称为[倒排索引](https://en.wikipedia.org/wiki/Inverted_index)) 可在文本数据上实现快速的全文搜索。
文本索引存储的是从标记到包含该标记的行号的映射。
标记由称为分词的过程生成。
例如，ClickHouse 的默认分词器会将英文句子 &quot;The cat likes mice.&quot; 转换为标记 [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;]。

例如，假设有一个只有一列三行的表

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

对应的标记如下：

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

我们通常会进行不区分大小写的搜索，因此会将这些标记统一转换为小写：

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

我们还会移除诸如 &quot;I&quot;、&quot;the&quot; 和 &quot;and&quot; 之类的停用词，因为它们几乎出现在每一行中：

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

从概念上讲，文本索引包含以下信息：

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

给定一个搜索标记，该索引结构可以快速找到所有匹配的行。

<div id="creating-a-text-index">
  ## 创建文本索引
</div>

文本索引在 ClickHouse 26.2 及以上版本中已正式可用 (GA) 。
在这些版本中，使用文本索引无需配置任何特殊设置。
我们强烈建议在生产环境中使用 ClickHouse 26.2 及以上版本。

:::note
无论 [compatibility](../../../operations/settings/settings#compatibility) 设置为何，文本索引都可用于任何 ClickHouse 26.2 及以上版本。
:::

要创建文本索引，请使用以下语法：

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

文本索引可定义在以下类型的列上：

* [String](/zh/sql-reference/data-types/string.md) 和 [FixedString](/zh/sql-reference/data-types/fixedstring.md)，
* [Array(String)](/zh/sql-reference/data-types/array.md) 和 [Array(FixedString)](/zh/sql-reference/data-types/array.md)，
* [Map](/zh/sql-reference/data-types/map.md) (通过 [mapKeys](/zh/sql-reference/functions/tuple-map-functions.md/#mapKeys) 和 [mapValues](/zh/sql-reference/functions/tuple-map-functions.md/#mapValues) 函数) ，以及
* [JSON](/zh/sql-reference/data-types/newjson.md) (通过 [JSONAllPaths](/zh/sql-reference/functions/json-functions.md/#JSONAllPaths) 和 [`JSONAllValues`](/zh/sql-reference/functions/json-functions.md#JSONAllValues) 函数) 。

也支持 [Nullable(T)](/zh/sql-reference/data-types/nullable.md) 和 [LowCardinality()](/zh/sql-reference/data-types/lowcardinality.md) 类型的列，包括 `Array(Nullable(String or FixedString))`。

或者，如需为现有表添加文本索引：

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

如果为现有表添加索引，我们建议为现有的 parts 物化该索引 (否则，对没有索引的 parts 进行搜索时，就会退回到速度较慢的穷举扫描) 。

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

要删除文本索引，请运行

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**分词器参数 (必需)&#x20;**。`tokenizer` 参数用于指定分词器：

* `splitByNonAlpha` 按非字母数字的 ASCII 字符拆分字符串 (参见函数 [splitByNonAlpha](/zh/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)) 。
* `splitByString(S)` 按用户定义的特定分隔符字符串 `S` 拆分字符串 (参见函数 [splitByString](/zh/sql-reference/functions/splitting-merging-functions.md/#splitByString)) 。
  可通过可选参数指定分隔符，例如 `tokenizer = splitByString([', ', '; ', '\n', '\\'])`。
  请注意，每个分隔符字符串都可以由多个字符组成 (如示例中的 `', '`) 。
  如果未显式指定，则默认分隔符列表 (例如 `tokenizer = splitByString`) 为单个空格 `[' ']`。
* `asciiCJK` 使用 Unicode 单词边界规则将字符串拆分为标记 (类似于 [Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)) 。ASCII 字母数字字符和下划线会与连接符一起构成标记 (字母使用 ASCII `:`，同类字符使用 `.` 和 `'`) 。非 ASCII Unicode 字符 (包括 [CJK](https://en.wikipedia.org/wiki/CJK_characters) 字符) 会成为单字符标记。
* `ngrams(N)` 将字符串拆分为长度相同的 `N`-gram (参见函数 [ngrams](/zh/sql-reference/functions/splitting-merging-functions.md/#ngrams)) 。
  可通过 1 到 8 之间的可选整数参数指定 ngram 长度，例如 `tokenizer = ngrams(3)`。
  如果未显式指定，则默认 ngram 长度 (例如 `tokenizer = ngrams`) 为 3。
* `sparseGrams(min_length, max_length, min_cutoff_length)` 将字符串拆分为可变长度的 n-gram，其长度至少为 `min_length`、至多为 `max_length` (含) 个字符 (参见函数 [sparseGrams](/zh/sql-reference/functions/string-functions#sparseGrams)) 。
  除非显式指定，否则 `min_length` 和 `max_length` 默认为 3 和 100。
  如果提供了参数 `min_cutoff_length`，则只返回长度大于或等于 `min_cutoff_length` 的 n-gram。
  与 `ngrams(N)` 相比，`sparseGrams` 分词器生成的是可变长度的 N-gram，因此能更灵活地表示原始文本。
  例如，`tokenizer = sparseGrams(3, 5, 4)` 会在内部从输入字符串生成 3-、4-、5-gram，但只返回 4-gram 和 5-gram。
* `array` 不执行分词，也就是说，每个行值都是一个标记 (参见函数 [array](/zh/sql-reference/functions/array-functions.md/#array)) 。

所有可用的分词器均列在 [system.tokenizers](../../../operations/system-tables/tokenizers.md) 中。

:::note
`splitByString` 分词器会按从左到右的顺序应用拆分分隔符。
这可能会导致歧义。
例如，分隔符字符串 `['%21', '%']` 会将 `%21abc` 分词为 `['abc']`，而如果将这两个分隔符字符串改为 `['%', '%21']`，则会输出 `['21abc']`。
在大多数情况下，你会希望匹配时优先使用较长的分隔符。
通常可通过按长度降序传入分隔符字符串来实现这一点。
如果这些分隔符字符串恰好构成 [prefix code](https://en.wikipedia.org/wiki/Prefix_code)，则可以按任意顺序传入。
:::

要了解分词器如何拆分输入字符串，可以使用 [tokens](/zh/sql-reference/functions/splitting-merging-functions.md/#tokens) 和 [tokensForLikePattern](/zh/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern) 函数：

示例：

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*处理非 ASCII 输入。*
可基于任何语言和字符集的文本数据创建文本索引。
对于非 ASCII 文本，推荐使用 `asciiCJK` 分词器，因为它能够正确处理 Unicode 词边界，包括 CJK 字符。
:::

**预处理器参数 (可选)&#x20;**。预处理器是指在分词前应用于输入字符串的表达式。

预处理器参数的典型用例包括

1. 转换为小写/大写，或进行大小写折叠以启用不区分大小写的匹配，例如 [lower](/zh/sql-reference/functions/string-functions.md/#lower)、[lowerUTF8](/zh/sql-reference/functions/string-functions.md/#lowerUTF8)、[caseFoldUTF8](/zh/sql-reference/functions/string-functions.md/#caseFoldUTF8)。
2. UTF-8 规范化，例如 [normalizeUTF8NFC](/zh/sql-reference/functions/string-functions.md/#normalizeUTF8NFC)、[normalizeUTF8NFD](/zh/sql-reference/functions/string-functions.md/#normalizeUTF8NFD)、[normalizeUTF8NFKC](/zh/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC)、[normalizeUTF8NFKD](/zh/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD)、[normalizeUTF8NFKCCasefold](/zh/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold)、[toValidUTF8](/zh/sql-reference/functions/string-functions.md/#toValidUTF8)。
3. 删除或转换不需要的字符或子字符串，例如去除重音符号，可使用 [extractTextFromHTML](/zh/sql-reference/functions/string-functions.md/#extractTextFromHTML)、[substring](/zh/sql-reference/functions/string-functions.md/#substring)、[idnaEncode](/zh/sql-reference/functions/string-functions.md/#idnaEncode)、[translate](/zh/sql-reference/functions/string-replace-functions.md/#translate)、[removeDiacriticsUTF8](/zh/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8)。

预处理器表达式必须将 [String](/zh/sql-reference/data-types/string.md) 或 [FixedString](/zh/sql-reference/data-types/fixedstring.md) 类型的输入值转换为相同类型的值。
如果文本索引是基于 `Nullable(T)` 或 `LowCardinality(T)` 类型的列构建的，则预处理器表达式应能接受 Nullable 或 LowCardinality 值 (即不抛出异常) 。

示例：

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

此外，预处理器表达式只能引用该文本索引所定义在其上的列或表达式。

示例：

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* 不允许：`INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

禁止使用非确定性函数。

:::note
原则上，预处理器等价于用预处理器表达式包装索引列或表达式。
例如，`INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` 中的 `lower` 预处理器，可以通过 `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')` 来模拟。
后一种形式的缺点是，只有当这个模拟的预处理器与 WHERE 子句中的过滤条件相匹配时，它才会生效。
例如，`WHERE hasAllTokens(lower(col), [...])` 会匹配，而 `WHERE hasAllTokens(col, [...])` 则不会。
因此，为了获得更好的使用体验，我们建议使用预处理器表达式。
:::

函数 [hasToken](/zh/sql-reference/functions/string-search-functions.md/#hasToken)、[hasAllTokens](/zh/sql-reference/functions/string-search-functions.md/#hasAllTokens)、[hasAnyTokens](/zh/sql-reference/functions/string-search-functions.md/#hasAnyTokens) 和 [hasPhrase](/zh/sql-reference/functions/string-search-functions.md/#hasPhrase) 会先使用预处理器转换搜索词，再对其进行分词。
请注意，由于预处理器只会应用于文本索引路径，因此这些函数在使用文本索引的查询与不使用文本索引的查询之间，结果可能会有所不同 (例如 `SETTINGS use_skip_indexes = 0`) 。

例如，

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

等价于：

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

在这种情况下，预处理表达式会分别转换数组中的每个元素。

示例：

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

要为基于 [Map](/zh/sql-reference/data-types/map.md) 类型列构建的文本索引定义预处理器，用户需要决定该索引是
基于映射的键构建，还是基于映射的值构建。

示例：

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

**后处理器参数 (可选)&#x20;**。后处理器是指在完成分词后，应用于每个输出标记的表达式。

与预处理器不同，预处理器会在分词器将整个输入字符串拆分为标记之前对其进行转换；而后处理器处理的是标记本身，并且一次只处理一个。
因此，它非常适合用于那些天然属于标记级别的转换。

后处理器参数的典型用例包括：

1. **过滤停用词 (出现频率极高的标记)&#x20;**。像 &quot;the&quot;、&quot;a&quot; 和 &quot;is&quot; 这类非常常见的标记几乎没有搜索相关性，反而会让索引膨胀。
   你可以使用后处理器将它们转换为空标记，从而将其丢弃——空标记会被忽略，也就是不会加入索引。
   示例：`if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **移除时间戳**。日志行通常以结构化时间戳开头，或包含结构化时间戳，例如 `2024-01-15T10:23:45`。
   对时间戳标记建立索引会让索引充斥大量没有搜索价值的字符串。
   忽略时间戳有两种可以互补的方法：
   * **后处理器方法**：使用 `splitByString` 分词器 (按空白拆分) ，这样整个时间戳会成为单个标记，然后使用 `parseDateTimeOrNull` 检测并丢弃它。
     示例：`if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     对于带有时区偏移或小数秒的时间戳，可使用 `parseDateTimeBestEffortOrNull(str)`，无需显式指定格式字符串。
   * **预处理器方法**：在分词*之前*，使用正则表达式从整条日志中去除时间戳。
     示例：`replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     该方法适用于任何分词器，而且效率更高，因为时间戳字符根本不会进入分词阶段。
     两种方法也可以结合使用：由预处理器去除时间戳，再由后处理器对剩余标记进行规范化或过滤 (例如转为小写，并去掉 `ERROR` 或 `INFO` 之类的严重级别词) 。
3. **词干提取**。将每个标记映射为其词干，可以通过匹配共享同一词根的形态变体来提高搜索召回率。
   例如，在英文词干提取中，&quot;running&quot;、&quot;runs&quot; 和 &quot;run&quot; 都会被提取为 &quot;run&quot;，因此查询其中任意一种变体时，都能匹配到全部形式。
   ClickHouse 为多种语言提供了内置的 [stem](/zh/sql-reference/functions/string-functions.md/#stem) 函数。
   示例：`stem(str, 'en')`
4. **大小写规范化**。将标记统一转换为小写或大写，以实现不区分大小写的匹配，例如 [lower](/zh/sql-reference/functions/string-functions.md/#lower)、[lowerUTF8](/zh/sql-reference/functions/string-functions.md/#lowerUTF8)。
   对于大小写转换，我们更建议使用预处理器，而不是后处理器。

后处理器表达式会将 [String](/zh/sql-reference/data-types/string.md) 类型的标记转换为相同类型的标记。
此外，后处理器表达式只能引用定义文本索引所基于的列或表达式。
当该列的类型为 `Array(String)` 时，后处理器仍然会按普通 `String` 值逐个处理各个标记。

禁止使用非确定性函数。

在索引构建期间，后处理器会应用到每个生成的标记上 (对于 `array` 分词器，每个数组元素都是一个标记) 。在查询时，其行为取决于具体函数：

* 对于 `hasToken`、`hasAllTokens`、`hasAnyTokens` 和 `hasPhrase` (使用任意受支持的分词器) ：后处理器会同时应用于 haystack 标记和搜索 needle，从而实现完全归一化的匹配 (例如，不区分大小写的搜索) 。对于 `hasPhrase`，后处理后的标记会被连续定位，因此如果后处理器丢弃了某个标记，也不会留下位置间隙，短语仍然可以跨过它完成匹配——例如，使用会丢弃 `the` 的停用词后处理器时，`hasPhrase(col, 'see cat')` 会匹配文档 `see the cat`。
* 对于所有其他函数 (`=`、`IN`、`has`、`hasAny`、`hasAll`、`mapContains*`) ：只有搜索 needle 会在索引提示查找时经过后处理；行级谓词仍会与原始列值进行比较。

示例：

* 使用后处理器表达式移除停用词：

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

* 使用后处理器表达式移除时间戳：

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

* 使用预处理表达式移除时间戳：

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

* 使用预处理器和后处理器的组合表达式移除时间戳：

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

* 使用后处理器表达式对标记进行词干化：

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

**函数支持**。

对于会查阅文本索引的谓词，在进行粒度级检查之前，会先对搜索值应用预处理器和后处理器，以便索引查找使用与索引构建时存储的相同标记。
对于大多数函数 (`=`、`IN`、`startsWith`、`endsWith`、`LIKE`、`mapContains*`) ，文本索引仅用于跳过无关的数据块；ClickHouse 仍会基于原始列数据，使用原始谓词验证每个保留下来的行。
对于标记搜索函数 (`hasToken`、`hasAllTokens`、`hasAnyTokens`) ，文本索引是主要的求值路径：ClickHouse 会通过与索引构建时相同的预处理器、分词器和后处理器对 needle 进行归一化，并将该规范化结果用于有索引和无索引的表 parts。使用后处理器时，haystack 标记也会在查询时被归一化 (适用于任何分词器，而不仅限于 `array`) ，因此比较两侧都会以一致的方式转换，结果不会依赖于是直接读取索引 (设置 `query_plan_direct_read_from_text_index`) ，还是某个 part 是否具有 materialized 索引——例如，对 `hasAllTokens(col, ['FOO'])` 使用 `lower` 后处理器时，即可启用不区分大小写的匹配。
不使用 `positions` 时，`hasPhrase` 仅将索引用作提示，并使用原始谓词验证每个保留下来的行；后处理器还会以相同方式对短语和 haystack 标记进行归一化，因此结果独立于读取路径，而且被后处理器丢弃的标记不会破坏短语相邻性。使用 `positions = 1` 时，`hasPhrase` 会使用精确的直接读取 (如果有后处理器，仍会应用) 。
被后处理器映射为空字符串的搜索标记会被忽略，也就是说，会被视为在搜索短语中不存在。

| 函数                                                                                          | 支持预处理器                  | 兼容的分词器                                                   | 支持后处理器 |
| ------------------------------------------------------------------------------------------- | ----------------------- | -------------------------------------------------------- | ------ |
| `=`                                                                                         | 是                       | 全部                                                       | 是      |
| `IN`                                                                                        | 是                       | 全部                                                       | 是      |
| [hasToken](/zh/sql-reference/functions/string-search-functions.md/#hasToken)                   | 是                       | 全部 (专为 `splitByNonAlpha` 设计)                             | 是      |
| [hasAnyTokens(col, str)](/zh/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | 是                       | 全部                                                       | 是      |
| [hasAllTokens(col, str)](/zh/sql-reference/functions/string-search-functions.md/#hasAllTokens) | 是                       | 全部                                                       | 是      |
| [hasAnyTokens(col, arr)](/zh/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | 否 (数组元素会直接作为标记使用)       | 全部                                                       | 是      |
| [hasAllTokens(col, arr)](/zh/sql-reference/functions/string-search-functions.md/#hasAllTokens) | 否 (数组元素会直接作为标记使用)       | 全部                                                       | 是      |
| [hasPhrase](/zh/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | 是                       | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | 是      |
| [startsWith](/zh/sql-reference/functions/string-functions.md/#startsWith)                      | 是                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 是      |
| [endsWith](/zh/sql-reference/functions/string-functions.md/#endsWith)                          | 是                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 是      |
| [like](/zh/sql-reference/functions/string-search-functions.md/#like)                           | 是¹                      | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | 是¹     |
| [match](/zh/sql-reference/functions/string-search-functions.md/#match)                         | 是¹                      | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | 是¹     |
| [ilike](/zh/sql-reference/functions/string-search-functions.md/#like)                          | 是² (仅 `lower`/`upper`)  | `splitByNonAlpha`, `array`²                              | 否²     |
| [mapContainsKey](/zh/sql-reference/functions/tuple-map-functions#mapContainsKey)               | 是                       | 全部                                                       | 是      |
| [mapContainsValue](/zh/sql-reference/functions/tuple-map-functions#mapContainsValue)           | 是                       | 全部                                                       | 是      |
| [mapContainsKeyLike](/zh/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | 是                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 是      |
| [mapContainsValueLike](/zh/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | 是                       | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | 是      |
| [has](/zh/sql-reference/functions/array-functions.md/#has)                                     | 是                       | `array`                                                  | 是      |
| [hasAny](/zh/sql-reference/functions/array-functions.md/#hasAny)                               | 是                       | `array`                                                  | 是      |
| [hasAll](/zh/sql-reference/functions/array-functions.md/#hasAll)                               | 是                       | `array`                                                  | 是      |

¹ 对所列分词器，`LIKE` 和 `match` 会将直接读取作为提示使用；否则会回退到穷举扫描。
此外，`LIKE` 还支持 *直接读取 (不使用提示)&#x20;*&#x20;(通过 `use_text_index_like_evaluation_by_dictionary_scan` 启用) ，适用于未使用预处理器或后处理器的 `splitByNonAlpha` 和 `array` 分词器。

² `ILIKE` 仅支持通过直接读取 (不使用提示) 来实现 (`use_text_index_like_evaluation_by_dictionary_scan = 1`，且分词器为 `splitByNonAlpha` 或 `array`) 。
不会回退为将索引用作提示：如果该设置被禁用，或分词器不在支持范围内，则不会对 `ILIKE` 使用索引。
如果存在预处理器，则必须为 `lower` 或 `upper`；不支持后处理器。

**Experimental：位置参数 (可选)&#x20;**。

Experimental 参数 `positions` (默认值：`0`) 用于控制索引是否存储标记位置。
设置为 `1` 时，索引还会额外存储位置数据 (保存在 `.pos` 文件中) ，从而使 [`hasPhrase`](#functions-example-hasphrase) 函数能够通过直接读取执行精确短语匹配。
存储位置信息会增加索引的磁盘占用和写入成本，因此该功能采用显式启用方式。
其磁盘格式目前尚不稳定，因此该参数属于 Experimental，未来版本中可能会发生变化。
因此，创建带有 `positions = 1` 的索引时，需要启用 MergeTree setting [`allow_experimental_text_index_positions`](/zh/operations/settings/merge-tree-settings#allow_experimental_text_index_positions)。
将 `positions = 0` (默认值) 可保持仅存储倒排列表；未指定此参数创建的文本索引将不包含位置信息。

:::warning
该参数属于 Experimental，仅应用于测试。
设置 MergeTree setting [`allow_experimental_text_index_positions`](/zh/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) 以启用位置存储。
:::

<details markdown="1">
  <summary>可选高级参数</summary>

  以下高级参数的默认值几乎在所有情况下都能良好工作。
  我们不建议更改它们。

  可选参数 `dictionary_block_size` (默认值：512) 用于指定字典块的大小，单位为行。

  可选参数 `dictionary_block_frontcoding_compression` (默认值：1) 用于指定字典块是否使用前缀编码压缩。

  可选参数 `posting_list_block_size` (默认值：1048576) 用于指定倒排列表块的大小，单位为行。

  可选参数 `posting_list_codec` (默认值：`none`) 用于指定倒排列表使用的 codec：

  * `none` - 倒排列表存储时不使用额外压缩。
  * `bitpacking` - 应用[差分 (delta) 编码](https://en.wikipedia.org/wiki/Delta_encoding)，然后再进行[位打包](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) (两者都在固定大小的块内进行) 。会降低 SELECT 查询速度，目前不推荐使用。

  以上高级参数也可以通过对应的 MergeTree settings 在表级别进行设置：[`text_index_dictionary_block_size`](/zh/operations/settings/merge-tree-settings#text_index_dictionary_block_size)、[`text_index_dictionary_block_frontcoding_compression`](/zh/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression)、[`text_index_posting_list_block_size`](/zh/operations/settings/merge-tree-settings#text_index_posting_list_block_size) 和 [`text_index_posting_list_codec`](/zh/operations/settings/merge-tree-settings#text_index_posting_list_codec)。
  它们会应用于该表中每个未显式指定该参数的文本索引。

  这些表级设置的主要用例，是在不删除并重新创建所有 table parts 上的文本索引的情况下，更改现有表的索引参数。
  更改表级设置后，新参数只会应用于为新 parts 构建的文本索引；现有 parts 会保留当前 layout。

  在索引定义中给出的参数优先次序高于表设置，例如：

  ```sql
  CREATE TABLE table(
      s String,
      -- 此索引使用 'bitpacking'，覆盖下面的表级默认值：
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- 此索引从表设置继承 'none'：
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*索引粒度。*
在 ClickHouse 中，文本索引被实现为一种[跳过索引](/zh/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types)。
但是，与其他跳过索引不同，文本索引使用无限粒度 (1 亿) 。
这一点可以在文本索引的表定义中看到。

示例：

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

较大的索引粒度可确保为整个分片创建文本索引。
明确指定的索引粒度会被忽略。

<div id="using-a-text-index">
  ## 使用文本索引
</div>

在 SELECT 查询中使用文本索引非常简单，因为常见的字符串搜索函数会自动利用该索引。
如果某一列或 parts 上不存在索引，字符串搜索函数就会退回到较慢的穷举扫描。

:::note
我们建议使用函数 `hasAnyTokens` 和 `hasAllTokens` 来查询文本索引，请参见[下文](#functions-example-hasanytokens-hasalltokens)。
这些函数适用于所有可用的分词器，以及所有可能的预处理器和后处理器表达式。
由于其他受支持的函数在历史上早于文本索引出现，因此它们在很多情况下必须保留原有行为 (例如不支持预处理器或后处理器) 。
:::

<div id="functions-support">
  ### 支持的函数
</div>

如果在 `WHERE` 子句或 `PREWHERE` 子句中使用了文本函数，就可以使用文本索引：

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/zh/sql-reference/functions/comparison-functions.md/#equals)) 完全匹配给定的搜索词。

示例：

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/zh/sql-reference/functions/in-functions)) 与 `equals` 类似，但会匹配所有搜索词。

示例：

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
文本索引不支持 `NOT IN` (`notIn`)。
:::

<div id="functions-example-like-match">
  #### `LIKE` 和 `match`
</div>

:::note
目前，只有当索引分词器为 `splitByNonAlpha`、`ngrams` 或 `sparseGrams` 时，这些函数才会使用文本索引进行过滤。
:::

:::note
文本索引不支持 `NOT LIKE` (`notLike`)。
:::

要将 `LIKE` ([like](/zh/sql-reference/functions/string-search-functions.md/#like)) 和 [match](/zh/sql-reference/functions/string-search-functions.md/#match) 函数与文本索引配合使用，ClickHouse 必须能够从搜索词中提取出完整的标记。
对于使用 `ngrams` 分词器的索引，只要通配符之间搜索字符串的长度等于或大于 ngram 的长度，就满足这一条件。

`splitByNonAlpha` 分词器的文本索引示例：

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

示例中的 `support` 可以匹配 `support`、`supports`、`supporting` 等。
这类查询属于子串查询，无法借助文本索引加速。

要让 LIKE 查询利用文本索引，必须将 LIKE 模式改写为如下形式：

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

`support` 左右的空格可确保该词语能够被提取为一个标记。

不过，在一种特殊情况下，ClickHouse 可以利用倒排索引显著加速 LIKE 查询。

详见 [LIKE/ILIKE 性能调优部分](#like-ilike-queries-perf)。

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` 和 `multiMatchAny`
</div>

[multiSearchAny](/zh/sql-reference/functions/string-search-functions.md/#multiSearchAny) 及其 UTF-8 变体 [multiSearchAnyUTF8](/zh/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) 用于判断多个字面子串中是否有任意一个出现在 haystack 中，而 [multiMatchAny](/zh/sql-reference/functions/string-search-functions.md/#multiMatchAny) 用于判断多个正则表达式中是否有任意一个能够匹配。
这些函数在与 `LIKE` 和 `match` 相同的条件下使用文本索引 (见上文) ：ClickHouse 必须能够从每个 needle 中提取出完整的标记，并且 needles 列表必须是常量。
如果某个粒度中可能包含任意一个 needle，则会读取该粒度。

对于 `multiMatchAny`，如果某个模式无法归约为对标记的要求 (例如 `.*`，它可匹配任意文档) ，则无法使用文本索引，查询会退回到全扫描。

与 `LIKE` 和 `match` 一样，子串搜索和正则表达式搜索最适合搭配 `ngrams` 和 `sparseGrams` 分词器。
这些分词器会为重叠的字符 n-grams 建立索引，因此一个 needle 会被分解为多个 n-grams；只要 needle 作为子串出现，这些 n-grams 就会出现在索引中，无论它是否从单词中间开始或结束。
因此，只要 needle 的长度至少与 n-gram 的大小相同，就可以直接按原样使用。

使用 `ngrams` 分词器的文本索引示例：

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

相比之下，`splitByNonAlpha` 分词器只会为完整的标记 (即整个单词) 建立索引。
由于 needle 可能从单词中间开始或在单词中间结束，ClickHouse 会丢弃每个 needle 的首尾标记，因此索引只能基于完整标记来裁剪粒度。
要让子串和正则表达式搜索在使用 `splitByNonAlpha` 时利用索引，请在每个 needle 两侧加上分隔符字符 (例如空格) ，使其构成一个或多个完整标记。

使用 `splitByNonAlpha` 分词器的文本索引示例：

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` 和 `endsWith`
</div>

与 `LIKE` 类似，函数 [startsWith](/zh/sql-reference/functions/string-functions.md/#startsWith) 和 [endsWith](/zh/sql-reference/functions/string-functions.md/#endsWith) 只有在能从搜索词中提取出完整标记时，才能使用文本索引。
对于使用 `ngrams` 分词器的索引，只要通配符之间待搜索字符串的长度等于或大于 ngram 长度，就满足这一条件。
当文本索引使用后处理器时，如果提取出的提示标记在归一化后仍非空，这些函数仍可在 Hint 模式下使用该索引。如果归一化后所有提示标记都被丢弃，则该索引不会用于该谓词。

使用 `splitByNonAlpha` 分词器的文本索引示例：

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

在该示例中，只有 `clickhouse` 会被视为一个标记。
`support` 不算是一个标记，因为它可以匹配 `support`、`supports`、`supporting` 等。

要查找所有以 `clickhouse supports` 开头的行，请在搜索模式末尾加上一个空格：

```sql
startsWith(comment, 'clickhouse supports ')`
```

类似地，`endsWith` 也应在前面加上空格使用：

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
在使用非 `splitByNonAlpha` 分词器和/或预处理器/后处理器表达式的文本索引中进行查找时，`hasToken` 存在一些使用陷阱。
我们建议改用 `hasAnyTokens` 和 `hasAllTokens`。

不区分大小写的变体 `hasTokenCaseInsensitive` 和 `hasTokenCaseInsensitiveOrNull` 不会利用文本索引——即使在建立了文本索引的列上，它们也始终执行全行扫描。要实现不区分大小写的匹配，请使用 `lower(...)` 预处理器或后处理器，并结合 `hasToken` / `hasAllTokens` / `hasAnyTokens` 一起使用。
:::

函数 [hasToken](/zh/sql-reference/functions/string-search-functions.md/#hasToken) 用于匹配单个指定的标记。

与前面提到的函数不同，这类函数不会对搜索词进行分词 (假定输入就是单个标记) 。

示例：

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` and `hasAllTokens`
</div>

函数 [hasAnyTokens](/zh/sql-reference/functions/string-search-functions.md/#hasAnyTokens) 和 [hasAllTokens](/zh/sql-reference/functions/string-search-functions.md/#hasAllTokens) 用于匹配给定标记中的任意一个或全部标记。

这两个函数接受的搜索标记既可以是字符串 (会使用与索引列相同的分词器进行分词) ，也可以是由已处理标记组成的数组；在搜索前不会对其再进行分词。
更多信息请参见函数文档。

示例：

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

函数 [hasPhrase](/zh/sql-reference/functions/string-search-functions.md/#hasPhrase) 用于短语匹配：所有标记都必须连续出现，且顺序与搜索字符串中的顺序一致。

与 `hasAllTokens` 仅要求所有标记出现在任意位置不同，`hasPhrase` 要求它们必须以连续序列的形式出现。
搜索短语会使用为索引列配置的同一分词器进行分词。
当文本索引使用后处理器时，搜索短语在索引查找前也会先进行归一化。
请注意，该函数要求使用 `splitByNonAlpha`、`splitByString`、`ngrams` 或 `asciiCJK` 分词器之一。

示例：

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

Array 函数 [has](/zh/sql-reference/functions/array-functions#has) 用于匹配字符串数组中的单个标记。

示例：

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` 和 `hasAll`
</div>

数组函数 [hasAny](/zh/sql-reference/functions/array-functions#hasAny) 和 [hasAll](/zh/sql-reference/functions/array-functions#hasAll) 用于检查已建立索引的数组列是否包含某个常量 needle 字符串集合中的任意一个或全部值。

示例：

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

函数 [mapContains](/zh/sql-reference/functions/tuple-map-functions#mapContainsKey) (`mapContainsKey` 的别名) 会在映射的键中，根据从待搜索字符串中提取出的标记进行匹配。
其行为类似于对 `String` 列使用 `equals` 函数。
只有在 `mapKeys(map)` 表达式上创建了文本索引时，才会使用该文本索引。

示例：

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

函数 [mapContainsValue](/zh/sql-reference/functions/tuple-map-functions#mapContainsValue) 会根据从映射的值中待搜索字符串提取出的标记进行匹配。
其行为类似于对 `String` 列使用 `equals` 函数。
只有在 `mapValues(map)` 表达式上创建了文本索引时，才会使用该索引。

示例：

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` 和 `mapContainsValueLike`
</div>

函数 [mapContainsKeyLike](/zh/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) 和 [mapContainsValueLike](/zh/sql-reference/functions/tuple-map-functions#mapContainsValueLike) 分别将模式与映射中的所有键或所有值进行匹配。

示例：

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

访问 [operator[]](/zh/sql-reference/operators#access-operators) 可与文本索引配合使用，用于过滤键和值。只有在 `mapKeys(map)` 或 `mapValues(map)` 表达式上创建了文本索引，或同时在两者上创建了文本索引时，才会使用该文本索引。

示例：

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

请参见以下示例，了解如何对 `Array(T)` 和 `Map(K, V)` 类型的列使用文本索引。

<div id="text-index-example-array">
  ### 为 Array(String) 列建立索引
</div>

设想有一个博客平台，作者使用关键字对博文进行分类。
我们希望用户能够通过搜索或点击 topic 来发现相关内容。

请看下面的表定义：

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

没有文本索引时，要查找包含特定关键词 (例如 `clickhouse`) 的帖子，就需要扫描所有条目：

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

随着平台规模的增长，这种方式会越来越慢，因为查询必须检查每一行中的每个 `keywords` 数组。
为了解决这一性能问题，我们为 `keywords` 列定义一个文本索引：

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### 为 Map 列建立索引
</div>

在许多可观测性用例中，日志消息会被拆分为&quot;各个组成部分&quot;，并以合适的数据类型存储，例如将时间戳存为日期时间、将日志级别存为枚举等。
指标字段最好存储为键值对。
运维团队需要能够高效搜索日志，以便进行调试、处理安全事件和监控。

考虑下面这个日志表：

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

如果没有文本索引，搜索 [Map](/zh/sql-reference/data-types/map.md) 数据时需要进行全表扫描：

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

随着日志量不断增加，这些查询会变慢。

解决方案是为 [Map](/zh/sql-reference/data-types/map.md) 中的键和值创建文本索引。
当你需要按字段名或 attribute 类型查找日志时，可以使用 [mapKeys](/zh/sql-reference/functions/tuple-map-functions.md/#mapKeys) 创建文本索引：

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

当你需要搜索属性中的实际内容时，可使用 [mapValues](/zh/sql-reference/functions/tuple-map-functions.md/#mapValues) 创建文本索引：

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

示例查询：

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### 索引 JSON 列
</div>

文本索引可通过三种方式用于 `JSON` 列：

1. **特定子列上的索引** —— 在已知的 JSON 路径上创建文本索引，就像为普通列创建索引一样。这样会为该路径上的*值*建立索引。
2. **基于路径的索引，使用 [JSONAllPaths](/zh/sql-reference/functions/json-functions.md/#JSONAllPaths)** —— 对每个粒度中存在的*所有路径*建立索引，从而跳过不可能包含查询路径的粒度。这与 `Map` 列类似。
3. **基于值的索引，使用 [JSONAllValues](/zh/sql-reference/functions/json-functions.md#JSONAllValues)** —— 对所有 JSON 路径中的*所有值*建立索引，从而通过单个索引加速对任意 JSON 子列的全文搜索。

<div id="json-indexes-on-subcolumns">
  #### 特定子列上的索引
</div>

你可以在任何 JSON 子列上创建跳过索引，其语法与普通列相同。

在索引表达式中引用 JSON 子列有两种方式：

* 在 JSON type hint 中声明的 **类型化路径** —— 直接按名称访问：`json.a`。
* 带显式类型转换的 **动态路径** —— 使用 `::` 转换语法：`json.b::String`。

示例索引定义：

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

示例查询：

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

查询示例：

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
  #### 使用 JSONAllPaths 的基于路径的索引
</div>

与 `Map` 列类似，也可以使用 [`JSONAllPaths`](/zh/sql-reference/functions/json-functions.md/#JSONAllPaths) 在 [JSON](/zh/sql-reference/data-types/newjson.md) 列上创建文本索引。
该索引会存储每个粒度中存在的 JSON 路径集合，并利用这些路径跳过不包含所查询路径的粒度。

示例索引定义：

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

你可以使用 `EXPLAIN indexes = 1` 来验证是否使用了跳过索引。
当某个路径只存在于一个分片中时，索引会跳过另一个分片。

示例：

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

如果某个路径在任何一个分片中都不存在，则会跳过所有分片和粒度。

示例：

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

`IS NOT NULL` 也会使用索引——它会跳过缺少该 path 的粒度 (因为其值会是 `NULL`) ：

示例：

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
  #### 使用 JSONAllValues 的基于值的索引
</div>

可以通过函数 [`JSONAllValues`](/zh/sql-reference/functions/json-functions.md#JSONAllValues) 在 [JSON](/zh/sql-reference/data-types/newjson.md) 列上使用文本索引，以加速搜索。

`JSONAllValues` 会将 JSON 列中的所有值作为 `Array(String)` 返回。
非字符串数据类型的值 (例如整数和数组) 会被转换为其文本表示形式。
使用 `JSONAllValues` 构建的文本索引会为每一行中所有 JSON 路径上的这些文本表示建立索引。
随后，该索引可加速按单个 JSON 子列进行过滤的查询。
当查询按特定子列进行过滤时 (例如 `data.user_name = 'alice'`) ，文本索引可以快速跳过那些任意 JSON 值中都不包含搜索标记的行 (以及粒度) 。

:::note
当不同的 JSON 路径包含相同的标记时，该索引可能会产生假阳性。
例如，如果第 1 行为 `{"a": "hello", "b": "world"}`，而查询搜索 `data.a = 'world'`，文本索引无法区分 `world` 属于路径 `b` 而不是 `a`。
在这种情况下，索引不会跳过该行，而是由对实际列数据的过滤来完成最终判断。
这种行为与其他文本索引用例相同，即索引充当快速预过滤器。
:::

<div id="json-all-values-creating-the-index">
  ##### 创建索引
</div>

索引定义示例：

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
  ##### 支持的查询模式
</div>

索引创建后，可使用与 `String` 列相同的函数来加速对 JSON 子列的查询，并可对所有列使用函数 `equals`。

子列访问：

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

使用显式 `CAST` 进行子列访问：

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

`IN` 运算符：

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### 短语搜索
</div>

例如，进行常规的文本索引搜索

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

匹配所有以任意顺序包含给定标记的行。
在此示例中，行 `While she stayed in Tokyo, the weather was great.` 符合该过滤器。

相比之下，短语搜索是指按给定顺序匹配这些标记。
例如，

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

匹配任何包含标记序列 `weather in Tokyo` 的行，例如 `How is the weather in Tokyo?`？

文本索引通过对短语中所有标记的倒排列表求交来识别候选粒度，从而加速短语搜索。
在这些粒度内，ClickHouse 随后会验证标记是否精确相邻。
这一过程开销相对较高，也比常规文本搜索查询更慢。
如需加快短语搜索查询，请在文本索引中启用位置存储 (参见上面的 `Optional parameters`) 。

`hasPhrase` 可与分词器 `splitByNonAlpha`、`splitByString`、`ngrams` 和 `asciiCJK` 搭配使用。
给定的短语字符串会使用该索引的分词器进行分词。
短语中的分隔符字符会被忽略：`hasPhrase(text, 'quick+brown')` 等同于 `hasPhrase(text, 'quick brown')`，前提是使用 `splitByNonAlpha` 作为分词器。

<div id="text-index-phrase-search-example">
  #### 示例
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

第 2 行 (`'New weather in York'`) 不匹配，因为这些标记的顺序不对。
第 3 行 (`'weather in New Orleans'`) 不匹配，因为其中不包含标记 `'York'`。

<div id="performance-tuning">
  ## 性能调优
</div>

<div id="direct-read">
  ### 直接读取
</div>

某些类型的文本查询可通过一种称为“直接读取”的优化显著提速。

示例：

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

直接读取优化仅通过文本索引 (即文本索引查找) 来返回查询结果，无需访问底层文本列。
文本索引查找读取的数据量相对较少，因此比 ClickHouse 中常规的跳过索引快得多 (后者会先执行跳过索引查找，再加载并筛选剩余粒度) 。

直接读取由两个设置控制：

* 设置 [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (默认值为 true) ，用于指定是否全局启用直接读取。
* 在 ClickHouse 版本 &lt; 26.4 中，设置 [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) 是直接读取的前置条件。

**支持的函数**

直接读取优化支持函数 `hasToken`、`hasAllTokens` 和 `hasAnyTokens`。
如果文本索引是使用 `array` 分词器定义的，直接读取还支持 `equals`、`has`、`hasAny`、`hasAll`、`mapContainsKey` 和 `mapContainsValue`。
这些函数也可以通过 `AND`、`OR` 和 `NOT` 运算符组合使用。
`WHERE` 或 `PREWHERE` 子句中也可以包含额外的非文本搜索函数过滤器 (针对文本列或其他列) ；在这种情况下，仍会使用直接读取优化，但效果会稍弱一些 (它只适用于受支持的文本搜索函数) 。

要确认某个查询是否使用了直接读取，请使用 `EXPLAIN PLAN actions = 1` 运行该查询。
例如，以下查询禁用了直接读取

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

返回值

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

而同一查询在 `query_plan_direct_read_from_text_index = 1` 时运行

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

返回值

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

第二个 EXPLAIN PLAN 输出包含一个虚拟列 `__text_index_<index_name>_<function_name>_<id>`。
如果该列存在，则表示使用了直接读取。

如果 WHERE 过滤子句只包含文本搜索函数，则查询可以完全避免读取列数据，并通过直接读取获得最大的性能收益。
不过，即使查询中的其他位置访问了文本列，直接读取仍然能带来性能提升。

**作为提示的直接读取**

作为提示的直接读取与普通直接读取基于相同原理，但它会基于文本索引数据额外添加一个过滤器，而不会移除底层文本列。
它适用于那些仅从文本索引读取会产生误报的函数。

支持的函数包括：`like`、`startsWith`、`endsWith`、`equals`、`has`、`hasPhrase`、`mapContainsKey` 和 `mapContainsValue`。

这个额外过滤器可以与其他过滤器结合，进一步提高选择性、限制结果集，并帮助减少从其他列读取的数据量。

作为提示的直接读取由设置 [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) 控制 (默认启用) 。

不使用提示的查询示例：

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

返回值

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

而在 `query_plan_text_index_add_hint = 1` 时运行同一查询

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

返回

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

在第二个 EXPLAIN PLAN 输出中，你可以看到过滤条件中新增了一个额外的合取项 (`__text_index_...`) 。
借助 [PREWHERE](/zh/sql-reference/statements/select/prewhere) 优化，过滤条件被拆分为三个独立的合取项，并按计算复杂度递增的顺序依次应用。
对于这个查询，应用顺序是先 `__text_index_...`，再 `greaterOrEquals(...)`，最后 `like(...)`。
这种排序方式使得在读取查询中 `WHERE` 子句之后使用的重量级列之前，能够跳过比文本索引和原始过滤器更多的数据粒度，从而进一步减少需要读取的数据量。

<div id="like-ilike-queries-perf">
  ### LIKE/ILIKE 查询
</div>

当 LIKE/ILIKE 查询模式为 `%<alpha-numeric-characters-without-spaces>%`，且文本索引分词器为 `splitByNonAlpha` 或 `array` 时，ClickHouse 会利用倒排索引显著提升 LIKE/ILIKE 查询速度。为此，ClickHouse 会扫描倒排索引字典，而不是执行全表扫描，以查找匹配的模式。

启用该优化后，LIKE/ILIKE 查询通常会比全表扫描快得多。不过，当模式匹配到字典中的大多数标记时，性能反而可能不如全表扫描。幸运的是，可以通过回退机制避免这种情况。

该优化由以下设置控制：

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

回退机制由以下两个设置控制：

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

此优化仅支持函数 `like` 和 `ilike`。

<div id="caching">
  ### 缓存
</div>

存在多种服务器级缓存，用于将文本索引的部分内容缓存在内存中 (请参见 [实现细节](#implementation) 一节) ：
目前，文本索引的反序列化请求头、标记和倒排列表都有对应的缓存，以减少 I/O。
使用设置 [use&#95;text&#95;index&#95;header&#95;cache](/zh/operations/settings/settings#use_text_index_header_cache)、[use&#95;text&#95;index&#95;tokens&#95;cache](/zh/operations/settings/settings#use_text_index_tokens_cache) 和 [use&#95;text&#95;index&#95;postings&#95;cache](/zh/operations/settings/settings#use_text_index_postings_cache)，可禁止查询对各个缓存进行读写。

要清除缓存，请使用语句 [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches)

请参考以下服务器设置来配置这些缓存。

<div id="caching-tokens">
  #### 标记缓存设置
</div>

| 设置                                                                                                                                                  | 描述                         |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/zh/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | 文本索引标记缓存策略的名称。             |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/zh/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | 缓存最大大小 (以字节为单位) 。          |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/zh/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | 缓存中已反序列化标记的最大数量。           |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/zh/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | 文本索引标记缓存中受保护队列大小占缓存总大小的比例。 |

<div id="caching-header">
  #### 文本索引请求头缓存设置
</div>

| 设置                                                                                                                                                  | 说明                          |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/zh/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | 文本索引请求头缓存策略的名称。              |
| [text&#95;index&#95;header&#95;cache&#95;size](/zh/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | 缓存的最大大小 (以字节为单位) 。          |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/zh/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | 缓存中已反序列化请求头的最大数量。            |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/zh/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | 文本索引请求头缓存中受保护队列的大小占缓存总大小的比例。 |

<div id="caching-posting-lists">
  #### 倒排列表缓存设置
</div>

| 设置                                                                                                                                                      | 说明                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/zh/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | 文本索引倒排列表缓存策略的名称。              |
| [text&#95;index&#95;postings&#95;cache&#95;size](/zh/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | 缓存的最大大小 (以字节为单位) 。            |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/zh/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | 缓存中已反序列化倒排列表的最大数量。            |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/zh/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | 文本索引倒排列表缓存中受保护队列的大小占缓存总大小的比例。 |

<div id="limitations">
  ## 局限性
</div>

文本索引目前有以下局限性：

* 对包含大量标记的文本索引进行物化 (例如 100 亿个标记) 可能会消耗大量内存。文本
  索引的物化既可能直接发生 (`ALTER TABLE <table> MATERIALIZE INDEX <index>`) ，也可能在分片合并过程中间接发生。
* 无法在超过 4,294,967,296 (= 2^32 = 约 42 亿) 行的分片上物化文本索引。如果没有 materialized 文本索引，查询会回退到在该分片内执行缓慢的穷举搜索。作为最坏情况的估算，假设一个分片只包含一个 String 类型的列，并且 MergeTree setting `max_bytes_to_merge_at_max_space_in_pool` 未被修改 (默认值：150 GB) 。在这种情况下，如果该列平均每行包含的字符数少于 29.5，就会出现这种情况。实际中，表通常还包含其他列，因此该阈值通常会小得多 (取决于其他列的数量、类型和大小) 。

<div id="text-index-vs-bloom-filter-indexes">
  ## 文本索引与基于布隆过滤器的索引
</div>

字符串谓词既可以通过文本索引加速，也可以通过基于布隆过滤器的索引 (索引类型 `bloom_filter`、`ngrambf_v1`、`tokenbf_v1`、`sparse_grams`) 加速，但两者在设计和预期用例上有本质区别：

**布隆过滤器索引**

* 基于概率型数据结构，可能会产生误报。
* 只能回答集合成员关系问题，即某列可能包含标记 X，或确定不包含 X。
* 存储粒度级信息，以便在查询执行期间跳过较大范围的数据。
* 很难正确调优 (示例参见[此处](mergetree#n-gram-bloom-filter)) 。
* 体积相对较小 (每个分片仅几 KB 到几 MB) 。

**文本索引**

* 基于标记构建确定性的倒排索引。索引本身不会产生误报。
* 专门针对文本搜索工作负载进行了优化。
* 存储行级信息，因此可以高效进行术语查找。
* 体积相对较大 (每个分片通常为数十到数百 MB) 。

基于布隆过滤器的索引对全文搜索的支持更多只是一种“副作用”：

* 它们不支持高级分词和预处理。
* 它们不支持多标记搜索。
* 它们无法提供倒排索引应有的性能特征。

相比之下，文本索引则是专为全文搜索打造的：

* 它们提供分词和预处理。
* 它们可为 `hasAllTokens`、`LIKE`、`match` 等文本搜索函数提供高效支持。
* 对于大型文本语料，它们具有明显更好的可扩展性。

<div id="implementation">
  ## 实现细节
</div>

每个文本索引由两个 (抽象的) 数据结构组成：

* 一个字典，用于将每个标记映射到一个倒排列表；以及
* 一组倒排列表，每个列表表示一组行号。

文本索引是针对整个分片构建的。
与其他跳过索引不同，文本索引可以在数据分区片段合并时直接合并，而不是在合并时重新构建 (见下文) 。

在创建索引期间，会创建三个文件 (每个分片 一组) ：

**字典块文件 (.dct)**

文本索引中的标记会先排序，然后存储到字典块中，每个字典块包含 512 个标记 (块大小可通过参数 `dictionary_block_size` 配置) 。
字典块文件 (.dct) 由一个分片中所有索引粒度的全部字典块组成。

**索引请求头文件 (.idx)**

索引请求头文件为每个字典块记录该块的第一个标记，以及它在字典块文件中的相对偏移量。

这种稀疏索引结构类似于 ClickHouse 的[稀疏主键索引](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes))。

**倒排列表文件 (.pst)**

所有标记的倒排列表都会按顺序存放在倒排列表文件中。
为了节省空间，同时仍能快速执行交集和并集操作，倒排列表以 [roaring bitmaps](https://roaringbitmap.org/) 的形式存储。
如果倒排列表大于 `posting_list_block_size`，则会将其拆分为多个块，并按顺序存储到倒排列表文件中。

**位置文件 (.pos)**

可选，仅当索引参数 `positions = 1` 时才会创建。
用于存储标记在匹配行中的位置。

**文本索引的合并**

合并数据分区片段时，文本索引无需从头重建；相反，它可以在合并过程中的独立步骤里高效完成合并。
在此步骤中，会读取每个输入分片的文本索引的已排序字典，并将它们合并为一个新的统一字典。
倒排列表中的行号也会重新计算，以反映它们在合并后数据分区片段中的新位置；这一过程会使用在初始合并阶段创建的旧行号到新行号的映射。
这种合并文本索引的方法类似于带有 `_part_offset` 列的 [projections](/zh/docs/sql-reference/statements/alter/projection#projection-indexes) 的合并方式。
如果索引在源分片中尚未 materialized，则会先构建该索引，将其写入临时文件，然后再与其他分片中的索引以及其他临时索引文件中的索引一起合并。

**调试**

表函数 [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) 可用于检查文本索引的内部结构。

<div id="hacker-news-dataset">
  ## 示例：Hackernews 数据集
</div>

我们来看一下，在包含大量文本的大型数据集上，文本索引带来的性能提升。
我们将使用热门网站 Hacker News 上的 2870 万行评论数据。
下面是未使用文本索引的表：

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

这 2870 万行数据位于 S3 中的一个 Parquet 文件中——让我们将它们插入 `hackernews` 表中：

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

我们将使用 `ALTER TABLE` 在 comment 列上添加文本索引，然后将其物化：

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

现在，让我们使用 `hasToken`、`hasAnyTokens` 和 `hasAllTokens` 函数来执行查询。
以下示例将展示标准索引扫描与直接读取优化之间巨大的性能差异。

<div id="using-hasToken">
  ### 1. 使用 `hasToken`
</div>

`hasToken` 用于检查文本中是否包含某个特定的单个标记。
我们将搜索区分大小写的标记 &#39;ClickHouse&#39;。

**禁用直接读取 (标准扫描)&#x20;**&#xA;默认情况下，ClickHouse 会使用跳过索引来过滤粒度，然后再读取这些粒度的列数据。
我们可以通过禁用直接读取来模拟这种行为。

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

**启用直接读取 (快速索引读取)&#x20;**&#xA;现在我们在启用直接读取 (默认) 的情况下运行相同的查询。

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

直接读取查询仅通过读取索引，速度就快了 45 倍以上 (0.362s 对比 0.008s) ，处理的数据量也显著减少 (9.51 GB 对比 3.15 MB) 。

<div id="using-hasAnyTokens">
  ### 2. 使用 `hasAnyTokens`
</div>

`hasAnyTokens` 用于检查文本是否包含给定标记中的至少一个。
我们将搜索包含 &#39;love&#39; 或 &#39;ClickHouse&#39; 的评论。

**已禁用直接读取 (标准扫描)&#x20;**

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

**已启用直接读取 (快速索引读取)&#x20;**

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

对于这种常见的 &quot;或&quot; 搜索，性能提升更加明显。
通过避免扫描普通列，查询速度几乎提升了 89 倍 (1.329s 对比 0.015s) 。

<div id="using-hasAllTokens">
  ### 3. 使用 `hasAllTokens`
</div>

`hasAllTokens` 用于检查文本是否包含给定的全部标记。
我们将搜索同时包含 &#39;love&#39; 和 &#39;ClickHouse&#39; 的评论。

**禁用直接读取 (标准扫描)&#x20;**&#xA;即使禁用了直接读取，标准跳过索引依然有效。
它将 2870 万行过滤到仅 14.746 万行，但仍需从该列读取 57.03 MB 的数据。

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

**已启用直接读取 (快速索引读取)&#x20;**&#xA;直接读取通过直接处理索引数据来完成查询，仅读取了 147.46 KB。

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

对于这种 &quot;与&quot; 搜索，直接读取优化比标准的跳过索引扫描快 26 倍以上 (0.184 秒 vs 0.007 秒) 。

<div id="compound-search">
  ### 4. 复合搜索：OR、AND、NOT，...
</div>

直接读取优化同样适用于复合布尔表达式。
这里，我们将对 &#39;ClickHouse&#39; OR &#39;clickhouse&#39; 执行一次不区分大小写的搜索。

**已禁用直接读取 (标准扫描)&#x20;**

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

**已启用直接读取 (快速索引读取)&#x20;**

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

通过结合索引结果，直接读取查询快了 34 倍 (0.450s 对比 0.013s) ，并且无需读取 9.58 GB 的列数据。
对于这一特定场景，`hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])` 是更推荐、也更高效的写法。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[宣布 ClickHouse 全文搜索正式可用](https://clickhouse.com/blog/full-text-search-ga-release)
* 博客：[为对象存储打造高性能全文搜索](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* 视频：[ClickHouse 全文搜索简介](https://www.youtube.com/watch?v=9zPmf1a_heU)
* 视频：[揭秘 ClickHouse 在大规模与高性能下的全文搜索](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* 演示文稿：[深入解析 ClickHouse 全文搜索：快速、原生且列式](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* 演示文稿：[倒排数据库索引：为什么、是什么以及如何实现，FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**过期内容**

* 博客：[ClickHouse 倒排索引简介](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* 博客：[深入解析 ClickHouse 全文搜索：快速、原生且列式](https://clickhouse.com/blog/clickhouse-full-text-search)
* 视频：[全文索引：设计与实验](https://www.youtube.com/watch?v=O_MnyUkrIq8)