---
slug: /zh/sql-reference/functions/string-search-functions
---

# 字符串搜索函数

本节中的所有函数默认情况下都区分大小写进行搜索。不区分大小写的搜索通常由单独的函数变体提供。  
请注意，不区分大小写的搜索，遵循英语的小写-大写规则。
例如。英语中大写的`i`是`I`，而在土耳其语中则是`İ`, 对于英语以外的语言，结果可能会不符合预期。

本节中的函数还假设搜索字符串和被搜索字符串是单字节编码文本(例如ASCII)。如果违反此假设，不会抛出异常且结果为undefined。   
UTF-8 编码字符串的搜索通常由单独的函数变体提供。同样，如果使用 UTF-8 函数变体但输入字符串不是 UTF-8 编码文本，不会抛出异常且结果为 undefined。
需要注意，函数不会执行自动 Unicode 规范化，您可以使用[normalizeUTF8*()](https://clickhouse.com/docs/zh/sql-reference/functions/string-functions/) 函数来执行此操作。
在[字符串函数](string-functions.md) 和 [字符串替换函数](string-replace-functions.md) 会分别说明.

## position

返回字符串`haystack`中子字符串`needle`的位置（以字节为单位，从 1 开始）。

**语法**

``` sql
position(haystack, needle[, start_pos])
```

别名：
- `position(needle IN haystack)`

**参数**

- `haystack` — 被检索查询字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — 进行查询的子字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – 在字符串`haystack` 中开始检索的位置(从1开始)，类型为[UInt](../../sql-reference/data-types/int-uint.md)，可选。

**返回值**

- 若子字符串存在，返回位置(以字节为单位，从 1 开始）。
- 如果不存在子字符串，返回 0。

如果子字符串 `needle` 为空，则：
- 如果未指定 `start_pos`，返回 `1`
- 如果 `start_pos = 0`，则返回 `1`
- 如果 `start_pos >= 1` 且 `start_pos <= length(haystack) + 1`，则返回 `start_pos` 
- 否则返回 `0` 

以上规则同样在这些函数中生效: [locate](#locate), [positionCaseInsensitive](#positionCaseInsensitive), [positionUTF8](#positionUTF8), [positionCaseInsensitiveUTF8](#positionCaseInsensitiveUTF8)

数据类型: `Integer`.

**示例**

``` sql
SELECT position('Hello, world!', '!');
```

结果：

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

示例，使用参数 `start_pos` :

``` sql
SELECT
    position('Hello, world!', 'o', 1),
    position('Hello, world!', 'o', 7)
```

结果：

``` text
┌─position('Hello, world!', 'o', 1)─┬─position('Hello, world!', 'o', 7)─┐
│                                 5 │                                 9 │
└───────────────────────────────────┴───────────────────────────────────┘
```

示例，`needle IN haystack`:

```sql
SELECT 6 = position('/' IN s) FROM (SELECT 'Hello/World' AS s);
```

结果：

```text
┌─equals(6, position(s, '/'))─┐
│                           1 │
└─────────────────────────────┘
```

示例，子字符串 `needle` 为空:

``` sql
SELECT
    position('abc', ''),
    position('abc', '', 0),
    position('abc', '', 1),
    position('abc', '', 2),
    position('abc', '', 3),
    position('abc', '', 4),
    position('abc', '', 5)
```
结果：
``` text
┌─position('abc', '')─┬─position('abc', '', 0)─┬─position('abc', '', 1)─┬─position('abc', '', 2)─┬─position('abc', '', 3)─┬─position('abc', '', 4)─┬─position('abc', '', 5)─┐
│                   1 │                      1 │                      1 │                      2 │                      3 │                      4 │                      0 │
└─────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┘
```

## locate

类似于 [position](#position) 但交换了 `haystack` 和 `locate` 参数。

此函数的行为取决于 ClickHouse 版本：
- 在 v24.3 以下的版本中，`locate` 是函数`position`的别名，参数为 `(haystack, needle[, start_pos])`。
- 在 v24.3 及以上的版本中,, `locate` 是独立的函数 (以更好地兼容 MySQL) ,参数为 `(needle, haystack[, start_pos])`。 之前的行为
  可以在设置中恢复 [function_locate_has_mysql_compatible_argument_order = false](../../operations/settings/settings.md#function-locate-has-mysql-compatible-argument-order);

**语法**

``` sql
locate(needle, haystack[, start_pos])
```

## positionCaseInsensitive

类似于 [position](#position) 但是不区分大小写。

## positionUTF8

类似于 [position](#position) 但是假定 `haystack` 和 `needle` 是 UTF-8 编码的字符串。

**示例**

函数 `positionUTF8` 可以正确的将字符 `ö` 计为单个 Unicode 代码点(`ö`由两个点表示):

``` sql
SELECT positionUTF8('Motörhead', 'r');
```

结果：

``` text
┌─position('Motörhead', 'r')─┐
│                          5 │
└────────────────────────────┘
```

## positionCaseInsensitiveUTF8

类似于 [positionUTF8](#positionutf8) 但是不区分大小写。

## multiSearchAllPositions

类似于 [position](#position) 但是返回多个在字符串 `haystack` 中 `needle` 子字符串的位置的数组（以字节为单位，从 1 开始）。


:::note
所有以 `multiSearch*()` 开头的函数仅支持最多 2<sup>8</sup> 个`needle`.
:::

**语法**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needleN])
```

**参数**

- `haystack` — 被检索查询字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — 子字符串数组, 类型为[Array](../../sql-reference/data-types/array.md)

**返回值**

- 位置数组，数组中的每个元素对应于 `needle` 数组中的一个元素。如果在 `haystack` 中找到子字符串，则返回的数组中的元素为子字符串的位置（以字节为单位，从 1 开始）；如果未找到子字符串，则返回的数组中的元素为 0。

**示例**

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world']);
```

结果：

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8

类似于 [multiSearchAllPositions](#multiSearchAllPositions) ,但假定 `haystack` 和 `needle`-s 是 UTF-8 编码的字符串。

## multiSearchFirstPosition

类似于 `position` , 在字符串`haystack`中匹配多个`needle`子字符串，从左开始任一匹配的子串，返回其位置。

函数 `multiSearchFirstPositionCaseInsensitive`, `multiSearchFirstPositionUTF8` 和 `multiSearchFirstPositionCaseInsensitiveUTF8` 提供此函数的不区分大小写 以及/或 UTF-8 变体。

**语法**

```sql
multiSearchFirstPosition(haystack, [needle1, needle2, ..., needleN])
```

## multiSearchFirstIndex

在字符串`haystack`中匹配最左侧的 needle<sub>i</sub> 子字符串，返回其索引 `i` (从1开始)，如无法匹配则返回0。

函数 `multiSearchFirstIndexCaseInsensitive`, `multiSearchFirstIndexUTF8` 和 `multiSearchFirstIndexCaseInsensitiveUTF8` 提供此函数的不区分大小写以及/或 UTF-8 变体。

**语法**

```sql
multiSearchFirstIndex(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>\])
```

## multiSearchAny {#multisearchany}

至少已有一个子字符串`needle`匹配 `haystack` 时返回1，否则返回 0 。

函数 `multiSearchAnyCaseInsensitive`, `multiSearchAnyUTF8` 和 `multiSearchAnyCaseInsensitiveUTF8` 提供此函数的不区分大小写以及/或 UTF-8 变体。


**语法**

```sql
multiSearchAny(haystack, [needle1, needle2, ..., needleN])
```

## match {#match}

返回字符串 `haystack` 是否匹配正则表达式 `pattern` （[re2正则语法参考](https://github.com/google/re2/wiki/Syntax)

匹配基于 UTF-8，例如`.` 匹配 Unicode 代码点 `¥`，它使用两个字节以 UTF-8 表示。T正则表达式不得包含空字节。如果 `haystack` 或`pattern`不是有效的 UTF-8，则此行为为undefined。
与 re2 的默认行为不同，`.` 会匹配换行符。要禁用此功能，请在模式前面添加`(?-s)`。

如果仅希望搜索子字符串，可以使用函数 [like](#like)或 [position](#position) 来替代，这些函数的性能比此函数更高。

**语法**

```sql
match(haystack, pattern)
```

别名： `haystack REGEXP pattern operator`

## multiMatchAny

类似于 `match`，如果至少有一个表达式 `pattern<sub>i</sub>` 匹配字符串 `haystack`，则返回1，否则返回0。

:::note
`multi[Fuzzy]Match*()` 函数家族使用了(Vectorscan)[https://github.com/VectorCamp/vectorscan]库. 因此，只有当 ClickHouse 编译时支持矢量扫描时，它们才会启用。

要关闭所有使用矢量扫描(hyperscan)的功能，请使用设置 `SET allow_hyperscan = 0;`。

由于Vectorscan的限制，`haystack` 字符串的长度必须小于2<sup>32</sup>字节。

Hyperscan 通常容易受到正则表达式拒绝服务 (ReDoS) 攻击。有关更多信息，请参见
[https://www.usenix.org/conference/usenixsecurity22/presentation/turonova](https://www.usenix.org/conference/usenixsecurity22/presentation/turonova)  
[https://doi.org/10.1007/s10664-021-10033-1](https://doi.org/10.1007/s10664-021-10033-1)  
[https://doi.org/10.1145/3236024.3236027](https://doi.org/10.1145/3236024.3236027)  
建议用户谨慎检查提供的表达式。

:::

如果仅希望搜索子字符串，可以使用函数  [multiSearchAny](#multisearchany) 来替代，这些函数的性能比此函数更高。

**语法**

```sql
multiMatchAny(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiMatchAnyIndex

类似于 `multiMatchAny` ，返回任何子串匹配 `haystack` 的索引。

**语法**

```sql
multiMatchAnyIndex(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiMatchAllIndices

类似于 `multiMatchAny`，返回一个数组，包含所有匹配 `haystack` 的子字符串的索引。

**语法**

```sql
multiMatchAllIndices(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAny

类似于 `multiMatchAny` ，如果任一 `pattern` 匹配 `haystack`，则返回1 within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance). 该功能依赖于实验特征 [hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) 库，并且对于某些边缘场景可能会很慢。性能取决于编辑距离`distance`的值和使用的`partten`，但与非模糊搜索相比，它的开销总是更高的。

:::note
由于 hyperscan 的限制，`multiFuzzyMatch*()` 函数族不支持 UTF-8 正则表达式（hyperscan以一串字节来处理）。
:::

**语法**

```sql
multiFuzzyMatchAny(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAnyIndex

类似于 `multiFuzzyMatchAny` 返回在编辑距离内与`haystack`匹配的任何索引

**语法**

```sql
multiFuzzyMatchAnyIndex(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAllIndices

类似于 `multiFuzzyMatchAny` 返回在编辑距离内与`haystack`匹配的所有索引的数组。

**语法**

```sql
multiFuzzyMatchAllIndices(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## extract

使用正则表达式提取字符串。如果字符串 `haystack` 不匹配正则表达式 `pattern` ，则返回空字符串。

对于没有子模式的正则表达式，该函数使用与整个正则表达式匹配的片段。否则，它使用与第一个子模式匹配的片段。

**语法**

```sql
extract(haystack, pattern)
```

## extractAll

使用正则表达式提取字符串的所有片段。如果字符串 `haystack` 不匹配正则表达式 `pattern` ，则返回空字符串。

返回所有匹配项组成的字符串数组。

子模式的行为与函数`extract`中的行为相同。

**语法**

```sql
extractAll(haystack, pattern)
```

## extractAllGroupsHorizontal

使用`pattern`正则表达式匹配`haystack`字符串的所有组。

返回一个元素为数组的数组，其中第一个数组包含与第一组匹配的所有片段，第二个数组包含与第二组匹配的所有片段，依此类推。

这个函数相比 [extractAllGroupsVertical](#extractallgroups-vertical)更慢。

**语法**

``` sql
extractAllGroupsHorizontal(haystack, pattern)
```

**参数**

- `haystack` — 输入的字符串，数据类型为[String](../../sql-reference/data-types/string.md).
- `pattern` — 正则表达式（[re2正则语法参考](https://github.com/google/re2/wiki/Syntax) ，必须包含 group，每个 group 用括号括起来。 如果 `pattern` 不包含 group 则会抛出异常。 数据类型为[String](../../sql-reference/data-types/string.md).

**返回值**

- 数据类型: [Array](../../sql-reference/data-types/array.md).

如果`haystack`不匹配`pattern`正则表达式，则返回一个空数组的数组。

**示例**

``` sql
SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

结果：

``` text
┌─extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','def','ghi'],['111','222','333']]                                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

## extractAllGroupsVertical

使用正则表达式 `pattern`匹配字符串`haystack`中的所有group。返回一个数组，其中每个数组包含每个group的匹配片段。片段按照在`haystack`中出现的顺序进行分组。

**语法**

``` sql
extractAllGroupsVertical(haystack, pattern)
```

**参数**

- `haystack` — 输入的字符串，数据类型为[String](../../sql-reference/data-types/string.md).
- `pattern` — 正则表达式（[re2正则语法参考](https://github.com/google/re2/wiki/Syntax) ，必须包含group，每个group用括号括起来。 如果 `pattern` 不包含group则会抛出异常。 数据类型为[String](../../sql-reference/data-types/string.md).

**返回值**

- 数据类型: [Array](../../sql-reference/data-types/array.md).

如果`haystack`不匹配`pattern`正则表达式，则返回一个空数组。

**示例**

``` sql
SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

结果：

``` text
┌─extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','111'],['def','222'],['ghi','333']]                                            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## like {#like}

返回字符串 `haystack` 是否匹配 LIKE 表达式 `pattern`。

一个 LIKE 表达式可以包含普通字符和以下元字符：

- `%` 表示任意数量的任意字符（包括零个字符）。
- `_` 表示单个任意字符。
- `\` 用于转义文字 `%`, `_` 和 `\`。

匹配基于 UTF-8，例如 `_` 匹配 Unicode 代码点 `¥`，它使用两个字节以 UTF-8 表示。

如果 `haystack` 或 `LIKE` 表达式不是有效的 UTF-8，则行为是未定义的。

不会自动执行 Unicode 规范化，您可以使用[normalizeUTF8*()](https://clickhouse.com/docs/zh/sql-reference/functions/string-functions/) 函数来执行此操作。

如果需要匹配字符 `%`, `_` 和 `/`（这些是 LIKE 元字符），请在其前面加上反斜杠：`\%`, `\_` 和 `\\`。
如果在非 `%`, `_` 或 `\` 字符前使用反斜杠，则反斜杠将失去其特殊含义（即被解释为字面值）。
请注意，ClickHouse 要求字符串中使用反斜杠 [也需要被转义](../syntax.md#string), 因此您实际上需要编写 `\\%`、`\\_` 和 `\\\\`。


对于形式为 `%needle%` 的 LIKE 表达式，函数的性能与 `position` 函数相同。
所有其他 LIKE 表达式都会被内部转换为正则表达式，并以与函数 `match` 相似的性能执行。

**语法**

```sql
like(haystack, pattern)
```

别名： `haystack LIKE pattern` (operator)

## notLike {#notlike}

类似于 `like` 但是返回相反的结果。

别名： `haystack NOT LIKE pattern` (operator)

## ilike

类似于 `like` 但是不区分大小写。

别名： `haystack ILIKE pattern` (operator)

## notILike

类似于 `ilike` 但是返回相反的结果。

别名： `haystack NOT ILIKE pattern` (operator)

## ngramDistance

计算字符串 `haystack`  和子字符串 `needle` 的 4-gram 距离。 为此，它计算两个 4-gram 多重集之间的对称差异，并通过它们的基数之和对其进行标准化。返回 0 到 1 之间的 Float32 浮点数。返回值越小，代表字符串越相似. 如果参数 `needle` or `haystack` 是常数且大小超过 32Kb，则抛出异常。如果参数 `haystack` 或 `needle` 是非常数且大小超过 32Kb ，则返回值恒为 1。

函数 `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8` 提供此函数的不区分大小写以及/或 UTF-8 变体。

**语法**

```sql
ngramDistance(haystack, needle)
```

## ngramSearch

类似于`ngramDistance`，但计算`needle`字符串和 `haystack` 字符串之间的非对称差异，即来自 `needle` 的 n-gram 数量减去由`needle`数量归一化的 n-gram 的公共数量 n-gram。返回 0 到 1 之间的 Float32 浮点数。结果越大，`needle` 越有可能在 `haystack` 中。该函数对于模糊字符串搜索很有用。另请参阅函数 `soundex``。

函数 `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8` 提供此函数的不区分大小写以及/或 UTF-8 变体。

:::note
UTF-8 变体使用了 3-gram 距离。这些并不是完全公平的 n-gram 距离。我们使用 2 字节的哈希函数来哈希 n-gram，然后计算这些哈希表之间的(非)对称差异——可能会发生冲突。在使用 UTF-8 大小写不敏感格式时，我们并不使用公平的 `tolower` 函数——我们将每个码点字节的第 5 位（从零开始）和第零字节的第一个比特位位置为零（如果该串的大小超过一个字节）——这对拉丁字母和大部分西里尔字母都有效。
:::

**语法**

```sql
ngramSearch(haystack, needle)
```

## countSubstrings

返回字符串 `haystack` 中子字符串 `needle` 出现的次数。

函数 `countSubstringsCaseInsensitive` 和 `countSubstringsCaseInsensitiveUTF8` 提供此函数的不区分大小写以及 UTF-8 变体。

**语法**

``` sql
countSubstrings(haystack, needle[, start_pos])
```

**参数**

- `haystack` — 被搜索的字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — 用于搜索的模式子字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – 在字符串`haystack` 中开始检索的位置(从 1 开始)，类型为[UInt](../../sql-reference/data-types/int-uint.md)，可选。

**返回值**

- 子字符串出现的次数。

数据类型: [UInt64](../../sql-reference/data-types/int-uint.md).

**示例**

``` sql
SELECT countSubstrings('aaaa', 'aa');
```

结果：

``` text
┌─countSubstrings('aaaa', 'aa')─┐
│                             2 │
└───────────────────────────────┘
```

示例，使用参数 `start_pos` :

```sql
SELECT countSubstrings('abc___abc', 'abc', 4);
```

结果：

``` text
┌─countSubstrings('abc___abc', 'abc', 4)─┐
│                                      1 │
└────────────────────────────────────────┘
```

## countMatches

返回正则表达式 `pattern` 在 `haystack` 中成功匹配的次数。

**语法**

``` sql
countMatches(haystack, pattern)
```

**参数**

- `haystack` — 输入的字符串，数据类型为[String](../../sql-reference/data-types/string.md).
- `pattern` — 正则表达式（[re2正则语法参考](https://github.com/google/re2/wiki/Syntax)） 数据类型为[String](../../sql-reference/data-types/string.md).

**返回值**

- 匹配次数。

数据类型: [UInt64](../../sql-reference/data-types/int-uint.md).

**示例**

``` sql
SELECT countMatches('foobar.com', 'o+');
```

结果：

``` text
┌─countMatches('foobar.com', 'o+')─┐
│                                2 │
└──────────────────────────────────┘
```

``` sql
SELECT countMatches('aaaa', 'aa');
```

结果：

``` text
┌─countMatches('aaaa', 'aa')────┐
│                             2 │
└───────────────────────────────┘
```

## countMatchesCaseInsensitive

类似于 `countMatches(haystack, pattern)` 但是不区分大小写。

## regexpExtract

提取匹配正则表达式模式的字符串`haystack`中的第一个字符串，并对应于正则表达式组索引。

**语法**

``` sql
regexpExtract(haystack, pattern[, index])
```

别名： `REGEXP_EXTRACT(haystack, pattern[, index])`.

**参数**

- `haystack` — 被匹配字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `pattern` — 正则表达式，必须是常量。类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `index` – 一个大于等于 0 的整数，默认为 1 ，它代表要提取哪个正则表达式组。 [UInt or Int](../../sql-reference/data-types/int-uint.md) 可选。

**返回值**

`pattern`可以包含多个正则组, `index` 代表要提取哪个正则表达式组。如果 `index` 为 0，则返回整个匹配的字符串。

数据类型: `String`.

**示例**

``` sql
SELECT
    regexpExtract('100-200', '(\\d+)-(\\d+)', 1),
    regexpExtract('100-200', '(\\d+)-(\\d+)', 2),
    regexpExtract('100-200', '(\\d+)-(\\d+)', 0),
    regexpExtract('100-200', '(\\d+)-(\\d+)');
```

结果：

``` text
┌─regexpExtract('100-200', '(\\d+)-(\\d+)', 1)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)', 2)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)', 0)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)')─┐
│ 100                                          │ 200                                          │ 100-200                                      │ 100                                       │
└──────────────────────────────────────────────┴──────────────────────────────────────────────┴──────────────────────────────────────────────┴───────────────────────────────────────────┘
```

## hasSubsequence

如果`needle`是`haystack`的子序列，返回1，否则返回0。
子序列是从给定字符串中删除零个或多个元素而不改变剩余元素的顺序得到的序列。

**语法**

``` sql
hasSubsequence(haystack, needle)
```

**参数**

- `haystack` — 被搜索的字符串，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — 搜索子序列，类型为[String](../../sql-reference/syntax.md#syntax-string-literal).

**返回值**

- 1, 如果`needle`是`haystack`的子序列
- 0, 如果`needle`不是`haystack`的子序列

数据类型: `UInt8`.

**示例**

``` sql
SELECT hasSubsequence('garbage', 'arg') ;
```

结果：

``` text
┌─hasSubsequence('garbage', 'arg')─┐
│                                1 │
└──────────────────────────────────┘
```

## hasSubsequenceCaseInsensitive
类似于 [hasSubsequence](#hasSubsequence) 但是不区分大小写。

## hasSubsequenceUTF8

类似于 [hasSubsequence](#hasSubsequence) 但是假定 `haystack` 和 `needle` 是 UTF-8 编码的字符串。

## hasSubsequenceCaseInsensitiveUTF8

类似于 [hasSubsequenceUTF8](#hasSubsequenceUTF8) 但是不区分大小写。
