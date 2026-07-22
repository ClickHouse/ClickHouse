---
title: 更新日志条目编写指南
---

<div id="changelog-entry-guidelines">
  # 更新日志条目编写指南
</div>

优秀的更新日志条目能帮助用户快速了解有哪些新内容，以及这些变化会对他们产生什么影响。我们要求贡献者填写面向用户、便于阅读的更新日志条目，这些条目会收录到每个版本的更新日志中。

下面我们将介绍几条编写优质更新日志条目的简单原则。

<div id="write-with-the-user-in-mind-not-the-developer">
  ## 从用户角度出发，而不是从开发者角度出发
</div>

更新日志条目的目的是向&#95;用户&#95;传达变更，而不只是面向&#95;开发者&#95;。
撰写条目时，在合适的情况下，尽量不仅说明变更&#x4E86;***什么***，还要说明这项变&#x66F4;***为什么***&#x5BF9;用户有帮助，或&#x8005;***会如何***&#x5F71;响用户。

例如，不要写：

> 新增 `system.iceberg_history` 表

请写：

> 用户现在可以通过新的 `system.iceberg_history` 表查看 Iceberg 表的历史快照。

不要写：

> 添加 `stringBytesUniq` 和 `stringBytesEntropy` 函数，用于搜索可能为随机或加密的数据。

请写：

> 现在，您可以使用新的 `stringBytesUniq` 和 `stringBytesEntropy` 函数检测字符串中可能经过加密或随机生成的数据，从而帮助识别数据质量问题或安全隐患。

<div id="keep-it-simple">
  ## 保持简洁
</div>

避免使用用户在没有解释的情况下可能看不懂的技术术语。尽量控制在 1–5 句话内，也可以放心使用 LLM 来帮助检查拼写错误、语法问题，或把条目改写得更易于用户理解
 (这不算作弊，我保证！) 

不要这样写：

> 支持将关联子查询用作 `EXISTS` expression 的参数

而要这样写：

> 您现在可以在 `EXISTS` 子句中使用引用外层查询列的子查询。

一个清晰、简洁的条目示例：

> 允许按查询级别调整页缓存设置。这对于更快地进行实验，以及针对高吞吐、低延迟查询进行微调非常必要。

<div id="follow-a-few-simple-formatting-guidelines">
  ## 遵循几条简单的格式规范
</div>

<div id="write-in-full-sentences-and-in-the-present-tense">
  ### 使用完整句子，并使用一般现在时
</div>

不要写成：

> Fixed a crash: if an exception is thrown in an attempt to remove a temporary file

应写为：

> Fixes a crash where an exception is thrown in an attempt to remove a temporary file.

<div id="use-backticks-where-necessary">
  ### 必要时使用反引号
</div>

对配置项、函数名、SQL 语句、格式名称和数据类型等代码元素加上反引号。一般来说，
凡是你会输入到 ClickHouse 客户端中的内容，都应该加上反引号。这样可以让更新日志条目更易读。

不要写成：

> 配置项 use&#95;skip&#95;indexes&#95;if&#95;final 和 use&#95;skip&#95;indexes&#95;if&#95;final&#95;exact&#95;mode 现在默认值为 True

请写成：

> 配置项 `use_skip_indexes_if_final` 和 `use_skip_indexes_if_final_exact_mode` 现在默认值为 `True`

<div id="try-to-follow-a-consistent-format">
  ### 尽量保持统一的格式
</div>

尽量使用相同的结构：它做了什么 → 为什么这对用户很重要 → 如何使用 (如有需要) 。这样读者就能更快浏览内容，也更容易预期每条条目的写法。

例如：

> 你现在可以在向量搜索前或后对结果应用过滤器，从而更好地权衡性能与准确性。使用新的 `vector_search_filter_mode` 设置来选择你偏好的方式。