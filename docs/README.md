# How to Contribute to ClickHouse Documentation

ClickHouse uses the "documentation as code" approach, so you can edit Markdown files in this folder from the GitHub web interface. Alternatively, fork the ClickHouse repository, edit, commit, push, and open a pull request.

At the moment documentation is bilingual in English and Russian. Try to keep all languages in sync if you can, but this is not strictly required. There are people who are responsible for monitoring language versions and syncing them. If you add a new article, you should also add it to `toc_{en,ru,zh,fa}.yaml` files with the pages index.

The master branch is then asynchronously published to the ClickHouse official website:

* In English: https://clickhouse.yandex/docs/en/
* In Russian: https://clickhouse.yandex/docs/ru/
* In Chinese: https://clickhouse.yandex/docs/zh/
* In Farsi: https://clickhouse.yandex/docs/fa/

The infrastructure to build Markdown for publishing on the documentation website resides in the [tools](tools) folder. It has its own [README.md](tools/README.md) file with more details.

# How to Write Content for ClickHouse Documentation

## Target Audience

When you write pretty much any text, the first thing you should think about is who will read it and which terms you should use for communicating with them.

ClickHouse can be directly used by all sorts of analysts and engineers. For generic parts of documentation (like the query language, tutorials or overviews), assume that the reader only has a basic technical background. For more technical sections (like articles that describe ClickHouse internals, guides for operating ClickHouse clusters, or rules for contributing to C++ code), you can use technical language and concepts.

## Specific Recommendations

* Documentation should make sense when you read it through from beginning to end. If you add new content, try to place it where the necessary concepts have already been explained.
* If a documentation section consists of many similar items, like functions or operators, try to order them from more generic (usable by a wide audience) to more specific (for specific use cases or application types). If several items are intended to be mostly used together, group them together in the documentation.
* Try to avoid slang. Use the most common and specific terms possible for everything. If some terms are used as synonyms, state this explicitly.
* All descriptions of functionality should be accompanied by examples. Basic examples are acceptable, but real world examples are welcome, too.
* Sensitive topics like politics, religion, race, and so on are strictly prohibited in documentation, examples, comments, and code.
* Proofread your text before publishing. Look for typos, missing punctuation, or repetitions that could be avoided.
* Try to avoid addressing the reader directly, although this is not strictly prohibited.

# How to Add a New Language

1. Create a new docs subfolder named using the [ISO-639-1 language code](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes).
2. Add Markdown files with the translation, mirroring the folder structure of other languages.
3. Commit and open a pull request with the new content.

Some additional configuration has to be done to actually make a new language live on the official website, but it's not automated or documented yet, so we'll do it on our own after the pull request with the content is merged.

# Markdown Dialect Cheatsheet

* Headings are on a separate line starting with `# `, `## ` or `### `.
* Bold is in `**asterisks**` or `__underlines__`.
* Links `[anchor](http://...)`, images `![with exclamation sign](http://...jpeg)`.
* Lists are on lines starting with `* unordered` or `1. ordered`, but there should be an empty line before the first list item. Sub-lists must be indented with 4 spaces.
* Inline code fragments are <code>&#96;in backticks&#96;</code>.
* Multiline code blocks are <code>&#96;&#96;&#96;in triple backtick quotes &#96;&#96;&#96;</code>.
* Brightly highlighted text starts with  `!!! info "Header"`, followed by 4 spaces on the next line and content. For a warning, replace `info` with `warning`.
* Hidden block that opens on click: `<details markdown="1"> <summary>Header</summary> hidden content</details>`.
* Colored text: `<span style="color: red;">text</span>`.
* Heading anchor to be linked to: `Title {#anchor-name}`.
* Table:
```
| Header    1 | Header    2 | Header    3 |
| ----------- | ----------- | ----------- |
| Cell     A1 | Cell     A2 | Cell     A3 |
| Cell     B1 | Cell     B2 | Cell     B3 |
| Cell     C1 | Cell     C2 | Cell     C3 |
```
