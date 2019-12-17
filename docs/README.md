# ClickHouse Docs

## Why Do You Need to Document ClickHouse?

The main reason is that ClickHouse is an open source project and if you don't write the docs, nobody does. "Incomplete or Confusing Documentation" is the top compliant about open source software by the results of a [2017 Github Open Source Survey](http://opensourcesurvey.org/2017/). Documentation is highly valued, but often overlooked. One of the most important contributions someone can make to an open source repository is a documentation update.

Many developers can say that the code is best docs by itself and they are right, but ClickHouse is not a project for C++ developers. The most of it's users don't know C++ and they don't search through the code. If you contribute into the code, your contribution is not complete if you don't add the docs. ClickHouse is large enough to absorb almost any change without a noticeable trace. Nobody will find your very useful function, or an important setting, or a very informative new column in a system table if it is not referenced in docs.

You say: "I don't know how to write.". We prepared some recommendations for you.
You say: "I know what I want to write, but I don't know how to contribute in docs.". Here is some [tips](#how-to-contribute).
You say: "I don't know what to describe." Ask us and we will provide you a choice.

It's easy, extremely useful for ClickHouse users, and grows your carma :-)


## What is the ClickHouse Documentation

The documentation contains information about all the aspects of ClickHouse lifecycle: developing, testing, installing, operating and using. The source language of the documentation is English. English version is the most actual. All other languages are supported by contributors from different countries.

At the moment documentation exists in:

* English: https://clickhouse.yandex/docs/en/
* Russian: https://clickhouse.yandex/docs/ru/
* Chinese: https://clickhouse.yandex/docs/zh/
* Japanese: https://clickhouse.yandex/docs/ja/
* Farsi: https://clickhouse.yandex/docs/fa/

We store the documentation with the ClickHouse source code in the [GitHub repository](https://github.com/ClickHouse/ClickHouse/tree/master/docs). Each language lays in the corresponding folder. Files in language folders that are not translated from English are created as symbolic links from the English ones.

<a name="how-to-contribute"/>

## How to Contribute to ClickHouse Documentation

You can edit the docs in some ways:

- Open a required file in the ClickHouse repository and edit it from the GitHub web interface.

    You can do it on GitHub, or on the [ClickHouse Documentation](https://clickhouse.yandex/docs/en/) site. Each page of ClickHouse Documentation site contains an "Edit this page" (🖋) element in the upper right corner. Clicking this symbol you get to the ClickHouse docs file opened for editing.

    When you saving a file, GitHub opens a pull-request for your contribution. Add the `documentation` label to this pull request for proper checks applying.

- Fork the ClickHouse repository, edit, commit, push, and open a pull request.

    Add the `documentation` label to this pull request for proper checks applying.

- Open GitHub issue and place your docs there with the `documentation` label, if you don't know the proper position for your contribution.


### Markdown Dialect Cheatsheet

- Headings: Place them on a separate line and start with `# `, `## ` or `### `. Use the [Title Case](https://titlecase.com/) for them. Example, `# The First Obligatory Title on a Page`.
- Bold text: `**asterisks**` or `__underlines__`.
- Links: `[link text](uri)`. Examples: 

    - External link: `[ClickHouse repo](https://github.com/ClickHouse/ClickHouse)`
    - Cross link: `[How to build docs](tools/README.md)`

- Images: `![Exclamation sign](uri)`. You can point to local images as well as remote in internet.
- Lists: Lists can be of two types:
    
    - `- unordered`: Each item starts from the `-`.
    - `1. ordered`: Each item starts from the number.
    
    The list must be separated from the text by an empty line. Nested lists must be indented with 4 spaces.

- Inline code: `\`in backticks\``.
- Multiline code blocks: 

<pre lang="no-highlight"><code>```lang_name
in
triple backtick
quotes
```</code></pre>.

- Note:

    ```
    !!! info "Header"
        4 spaces indented text.
    ```

- Warning:

    ```
    !!! warning "Header"
        4 spaces indented text.
    ```

- Text hidden behind a cut (single sting that opens on click): `<details markdown="1"> <summary>Header</summary> hidden content</details>`.
- Colored text: `<span style="color: red;">text</span>`.
- Heading anchor to be linked to: `# Title {#anchor-name}`.
- Table:
    ```
    | Header    1 | Header    2 | Header    3 |
    | ----------- | ----------- | ----------- |
    | Cell     A1 | Cell     A2 | Cell     A3 |
    | Cell     B1 | Cell     B2 | Cell     B3 |
    | Cell     C1 | Cell     C2 | Cell     C3 |
    ```

### Adding a New File

When adding a new file:

- Make symbolic links for all other languages.
- Reference the file from `toc_{en,ru,zh,ja,fa}.yaml` files with the pages index.

### Adding a New Language

1. Create a new docs subfolder named using the [ISO-639-1 language code](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes).
2. Add Markdown files with the translation, mirroring the folder structure of other languages.
3. Commit and open a pull request with the new content.

Some additional configuration has to be done to actually make a new language live on the official website, but it's not automated or documented yet, so we'll do it on our own after the pull request with the content is merged.


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
