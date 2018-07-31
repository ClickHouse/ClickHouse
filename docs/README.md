# How to contribute to ClickHouse documentation?

Basically ClickHouse uses "documentation as code" approach, so you can edit Markdown files in this folder from GitHub web interface or fork ClickHouse repository, edit, commit, push and open pull request.

At the moment documentation is bilingual in English and Russian, so it's better to try keeping languages in sync if you can, but it's not strictly required as there are people watching over this. If you add new article, you should also add it to `toc_{en,ru}.yaml` file with pages index.

Master branch is then asynchronously published to ClickHouse official website:

* In English: https://clickhouse.yandex/docs/en/
* In Russian: https://clickhouse.yandex/docs/ru/

Infrastructure to build Markdown to documentation website resides in [tools](tools) folder, it has it's own [README.md](tools/README.md) with more details.

# How to write content for ClickHouse documentation?

## Target audience

When you write pretty much any text, first thing you should think about: who exactly will read it and in which terms it is better to "talk" with them.

ClickHouse can be directly used by all sorts of either analysts and engineers, so you should only basic technical background of reader when writing content for generic parts of documentation, like query language, tutorials or overviews. Though it is ok for articles describing ClickHouse internals, guides for operating ClickHouse clusters, contributing to C++ code and other similar topics.

## Specific recommendations

* Documentation should make sense when you read it roughly from start to end. So when choosing a place for new content try to minimize referring to stuff that will be described later on.
* If documentation section consists of many similar items, like functions or operators, try to order them from more generic (usable by wider audience) to more specific (to some usecases or application types). If several items are intended to be mostly used together, keep them together in documentation too.
* Try to avoid slang, use the most common and specific terms for everythings. If some terms are used as synonyms, state this explicitly.
* All functionality descriptions should be accompanied by examples. At least very basic ones, but real world examples are welcome too.
* Debatable topics like politics, religion, racial and so on are strictly prohibited in either documentation, examples, comments and code.
* People tend to get temporary stuck with some specific words or phrases, usually auxiliary, for a shord period of time. So they get repeated over and over in small part of content, which looks weird when reading. It is easy to fix this by reading your text again before publishing, also you can use this opportunity to fix mistypes and lost punctuation.
* Try to avoid naming the reader in text, it is not strictly prohibited though.

# Quick cheatsheet on used Markdown dialect

* Headers on separate line starting with `# `, `## ` or `### `.
* Bold is in `**asterisks**` or `__underlines__`.
* Links `[anchor](http://...)`, images `![with exclamation sign](http://...jpeg)`.
* Lists are on lines starting with `* unordered` or `1. ordered`, but there should be empty line before first list item. Sub-lists must be indented with 4 spaces.
* Inline piece of code is <code>&#96;in backticks&#96;</code>.
* Multiline code block are <code>&#96;&#96;&#96;in triple backtick quotes &#96;&#96;&#96;</code>.
* Brightly highlighted block of text starts with  `!!! info "Header"`, on next line 4 spaces and content. Instead of `info` can be `warning`.
* Hide block to be opened by click: `<details> <summary>Header</summary> hidden content</details>`.
* Colored text: `<span style="color: red;">text</span>`.
* Additional anchor to be linked to: `<a name="my_anchor"></a>`, for headers fully in English they are created automatically like `"FoO Bar" -> "foo-bar"`.
* Table:
```
| Header    1 | Header    2 | Header    3 |
| ----------- | ----------- | ----------- |
| Cell     A1 | Cell     A2 | Cell     A3 |
| Cell     B1 | Cell     B2 | Cell     B3 |
| Cell     C1 | Cell     C2 | Cell     C3 |
```
