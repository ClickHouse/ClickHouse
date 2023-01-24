# 字符串搜索函数 {#zi-fu-chuan-sou-suo-han-shu}

下列所有函数在默认的情况下区分大小写。对于不区分大小写的搜索，存在单独的变体。

## 位置（大海捞针），定位（大海捞针) {#positionhaystack-needle-locatehaystack-needle}

在字符串`haystack`中搜索子串`needle`。
返回子串的位置（以字节为单位），从1开始，如果未找到子串，则返回0。

对于不区分大小写的搜索，请使用函数`positionCaseInsensitive`。

## positionUTF8(大海捞针) {#positionutf8haystack-needle}

与`position`相同，但位置以Unicode字符返回。此函数工作在UTF-8编码的文本字符集中。如非此编码的字符集，则返回一些非预期结果（他不会抛出异常）。

对于不区分大小写的搜索，请使用函数`positionCaseInsensitiveUTF8`。

## 多搜索分配（干草堆，\[针<sub>1</sub>，针<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchallpositionshaystack-needle1-needle2-needlen}

与`position`相同，但函数返回一个数组，其中包含所有匹配needle<sub>我</sub>的位置。

对于不区分大小写的搜索或/和UTF-8格式，使用函数`multiSearchAllPositionsCaseInsensitive，multiSearchAllPositionsUTF8，multiSearchAllPositionsCaseInsensitiveUTF8`。

## multiSearchFirstPosition(大海捞针,\[针<sub>1</sub>，针<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstpositionhaystack-needle1-needle2-needlen}

与`position`相同，但返回在`haystack`中与needles字符串匹配的最左偏移。

对于不区分大小写的搜索或/和UTF-8格式，使用函数`multiSearchFirstPositionCaseInsensitive，multiSearchFirstPositionUTF8，multiSearchFirstPositionCaseInsensitiveUTF8`。

## multiSearchFirstIndex(大海捞针,\[针<sub>1</sub>，针<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

返回在字符串`haystack`中最先查找到的needle<sub>我</sub>的索引`i`（从1开始），没有找到任何匹配项则返回0。

对于不区分大小写的搜索或/和UTF-8格式，使用函数`multiSearchFirstIndexCaseInsensitive，multiSearchFirstIndexUTF8，multiSearchFirstIndexCaseInsensitiveUTF8`。

## 多搜索（大海捞针，\[针<sub>1</sub>，针<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchanyhaystack-needle1-needle2-needlen}

如果`haystack`中至少存在一个needle<sub>我</sub>匹配则返回1，否则返回0。

对于不区分大小写的搜索或/和UTF-8格式，使用函数`multiSearchAnyCaseInsensitive，multiSearchAnyUTF8，multiSearchAnyCaseInsensitiveUTF8`。

!!! note "注意"
    在所有`multiSearch*`函数中，由于实现规范，needles的数量应小于2<sup>8</sup>。

## 匹配（大海捞针，模式) {#matchhaystack-pattern}

检查字符串是否与`pattern`正则表达式匹配。`pattern`可以是一个任意的`re2`正则表达式。 `re2`正则表达式的[语法](https://github.com/google/re2/wiki/Syntax)比Perl正则表达式的语法存在更多限制。

如果不匹配返回0，否则返回1。

请注意，反斜杠符号（`\`）用于在正则表达式中转义。由于字符串中采用相同的符号来进行转义。因此，为了在正则表达式中转义符号，必须在字符串文字中写入两个反斜杠（\\）。

正则表达式与字符串一起使用，就像它是一组字节一样。正则表达式中不能包含空字节。
对于在字符串中搜索子字符串的模式，最好使用LIKE或«position»，因为它们更加高效。

## multiMatchAny（大海捞针，\[模式<sub>1</sub>，模式<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

与`match`相同，但如果所有正则表达式都不匹配，则返回0；如果任何模式匹配，则返回1。它使用[超扫描](https://github.com/intel/hyperscan)库。对于在字符串中搜索子字符串的模式，最好使用«multisearchany»，因为它更高效。

!!! note "注意"
    任何`haystack`字符串的长度必须小于2<sup>32\</sup>字节，否则抛出异常。这种限制是因为hyperscan API而产生的。

## multiMatchAnyIndex（大海捞针，\[模式<sub>1</sub>，模式<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

与`multiMatchAny`相同，但返回与haystack匹配的任何内容的索引位置。

## multiFuzzyMatchAny(干草堆,距离,\[模式<sub>1</sub>，模式<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

与`multiMatchAny`相同，但如果在haystack能够查找到任何模式匹配能够在指定的[编辑距离](https://en.wikipedia.org/wiki/Edit_distance)内进行匹配，则返回1。此功能也处于实验模式，可能非常慢。有关更多信息，请参阅[hyperscan文档](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching)。

## multiFuzzyMatchAnyIndex(大海捞针,距离,\[模式<sub>1</sub>，模式<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

与`multiFuzzyMatchAny`相同，但返回匹配项的匹配能容的索引位置。

!!! note "注意"
    `multiFuzzyMatch*`函数不支持UTF-8正则表达式，由于hyperscan限制，这些表达式被按字节解析。

!!! note "注意"
    如要关闭所有hyperscan函数的使用，请设置`SET allow_hyperscan = 0;`。

## 提取（大海捞针，图案) {#extracthaystack-pattern}

使用正则表达式截取字符串。如果’haystack’与’pattern’不匹配，则返回空字符串。如果正则表达式中不包含子模式，它将获取与整个正则表达式匹配的子串。否则，它将获取与第一个子模式匹配的子串。

## extractAll（大海捞针，图案) {#extractallhaystack-pattern}

使用正则表达式提取字符串的所有片段。如果’haystack’与’pattern’正则表达式不匹配，则返回一个空字符串。否则返回所有与正则表达式匹配的字符串数组。通常，行为与’extract’函数相同（它采用第一个子模式，如果没有子模式，则采用整个表达式）。

## 像（干草堆，模式），干草堆像模式运算符 {#likehaystack-pattern-haystack-like-pattern-operator}

检查字符串是否与简单正则表达式匹配。
正则表达式可以包含的元符号有`％`和`_`。

`%` 表示任何字节数（包括零字符）。

`_` 表示任何一个字节。

可以使用反斜杠（`\`）来对元符号进行转义。请参阅«match»函数说明中有关转义的说明。

对于像`％needle％`这样的正则表达式，改函数与`position`函数一样快。
对于其他正则表达式，函数与’match’函数相同。

## 不喜欢（干草堆，模式），干草堆不喜欢模式运算符 {#notlikehaystack-pattern-haystack-not-like-pattern-operator}

与’like’函数返回相反的结果。

## 大海捞针) {#ngramdistancehaystack-needle}

基于4-gram计算`haystack`和`needle`之间的距离：计算两个4-gram集合之间的对称差异，并用它们的基数和对其进行归一化。返回0到1之间的任何浮点数 – 越接近0则表示越多的字符串彼此相似。如果常量的`needle`或`haystack`超过32KB，函数将抛出异常。如果非常量的`haystack`或`needle`字符串超过32Kb，则距离始终为1。

对于不区分大小写的搜索或/和UTF-8格式，使用函数`ngramDistanceCaseInsensitive，ngramDistanceUTF8，ngramDistanceCaseInsensitiveUTF8`。

## ﾂ暗ｪﾂ氾环催ﾂ団ﾂ法ﾂ人) {#ngramsearchhaystack-needle}

与`ngramDistance`相同，但计算`needle`和`haystack`之间的非对称差异——`needle`的n-gram减去`needle`归一化n-gram。可用于模糊字符串搜索。

对于不区分大小写的搜索或/和UTF-8格式，使用函数`ngramSearchCaseInsensitive，ngramSearchUTF8，ngramSearchCaseInsensitiveUTF8`。

!!! note "注意"
    对于UTF-8，我们使用3-gram。所有这些都不是完全公平的n-gram距离。我们使用2字节哈希来散列n-gram，然后计算这些哈希表之间的（非）对称差异 - 可能会发生冲突。对于UTF-8不区分大小写的格式，我们不使用公平的`tolower`函数 - 我们将每个Unicode字符字节的第5位（从零开始）和字节的第一位归零 - 这适用于拉丁语，主要用于所有西里尔字母。

[来源文章](https://clickhouse.com/docs/en/query_language/functions/string_search_functions/) <!--hide-->
