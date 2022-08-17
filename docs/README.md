# Contributing to ClickHouse Documentation

## Why Do You Need to Document ClickHouse

The main reason is that ClickHouse is an open source project, and if you don't write the docs, nobody does. "Incomplete or Confusing Documentation" is the top complaint about open source software by the results of a [Github Open Source Survey](http://opensourcesurvey.org/2017/) of 2017. Documentation is highly valued but often overlooked. One of the most important contributions someone can make to an open source repository is a documentation update.

Many developers can say that the code is the best docs by itself, and they are right. But, ClickHouse is not a project for C++ developers. Most of its users don't know C++, and they can't understand the code quickly. ClickHouse is large enough to absorb almost any change without a noticeable trace. Nobody will find your very useful function, or an important setting, or a very informative new column in a system table if it is not referenced in the documentation.

If you want to help ClickHouse with documentation you can face, for example, the following questions:

- "I don't know how to write."
    
    We have prepared some [recommendations](#what-to-write) for you.

- "I know what I want to write, but I don't know how to contribute to docs."

    Here are some [tips](#how-to-contribute).

Writing the docs is extremely useful for project's users and developers, and grows your karma.

**Contents**

- [What is the ClickHouse Documentation](#clickhouse-docs)
- [How to Contribute to ClickHouse Documentation](#how-to-contribute)
    - [Markdown Dialect Cheatsheet](#markdown-cheatsheet)
    - [Adding a New File](#adding-a-new-file)
    - [Adding a New Language](#adding-a-new-language)
-  [How to Write Content for ClickHouse Documentation](#what-to-write)
    - [Documentation for Different Audience](#target-audience)
    - [Common Recommendations](#common-recommendations)
    - [Description Templates](#templates)
- [How to Build Documentation](#how-to-build-docs)


<a name="clickhouse-docs"/>

## What is the ClickHouse Documentation

The documentation contains information about all the aspects of the ClickHouse lifecycle: developing, testing, installing, operating, and using. The base language of the documentation is English. The English version is the most actual. All other languages are supported as much as they can by contributors from different countries.

At the moment, [documentation](https://clickhouse.tech/docs) exists in English, Russian, Chinese, Japanese, and Farsi. We store the documentation besides the ClickHouse source code in the [GitHub repository](https://github.com/ClickHouse/ClickHouse/tree/master/docs).

Each language lays in the corresponding folder. Files that are not translated from English are the symbolic links to the English ones.

<a name="how-to-contribute"/>

## How to Contribute to ClickHouse Documentation

You can contribute to the documentation in many ways, for example:

- Fork the ClickHouse repository, edit, commit, push, and open a pull request.

    Add the `documentation` label to this pull request for proper automatic checks applying. If you have no permissions for adding labels, the reviewer of your PR adds it.

- Open a required file in the ClickHouse repository and edit it from the GitHub web interface.

    You can do it on GitHub, or on the [ClickHouse Documentation](https://clickhouse.tech/docs/en/) site. Each page of ClickHouse Documentation site contains an "Edit this page" (🖋) element in the upper right corner. Clicking this symbol, you get to the ClickHouse docs file opened for editing.

    When you are saving a file, GitHub opens a pull-request for your contribution. Add the `documentation` label to this pull request for proper automatic checks applying. If you have no permissions for adding labels, the reviewer of your PR adds it.

Contribute all new information in English language. Other languages are translations from English.

<a name="markdown-cheatsheet"/>

### Markdown Dialect Cheatsheet

- Headings: Place them on a separate line and start with `# `, `## ` or `### `. Use the [Title Case](https://titlecase.com/) for them. Example:

    ```text
    # The First Obligatory Title on a Page.
    ```

- Bold text: `**asterisks**` or `__underlines__`.
- Links: `[link text](uri)`. Examples: 

    - External link: `[ClickHouse repo](https://github.com/ClickHouse/ClickHouse)`
    - Cross link: `[How to build docs](tools/README.md)`

- Images: `![Exclamation sign](uri)`. You can refer to local images as well as remote in internet.
- Lists: Lists can be of two types:
    
    - `- unordered`: Each item starts from the `-`.
    - `1. ordered`: Each item starts from the number.
    
    A list must be separated from the text by an empty line. Nested lists must be indented with 4 spaces.

- Inline code: `` `in backticks` ``.
- Multiline code blocks:
    <pre lang="no-highlight"><code>```lang_name
    code
    lines
    ```</code></pre>
- Note:

    ```text
    !!! info "Header"
        4 spaces indented text.
    ```

- Warning:

    ```text
    !!! warning "Header"
        4 spaces indented text.
    ```

- Text hidden behind a cut (single sting that opens on click):

    ```text
    <details markdown="1"> <summary>Visible text</summary> 
        Hidden content.
    </details>`.
    ```
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

<a name="adding-a-new-file"/>

### Adding a New File

When you add a new file, it should end with a link like:

`[Original article](https://clickhouse.tech/docs/<path-to-the-page>) <!--hide-->`

and there should be **a new empty line** after it.

{## When adding a new file:

- Make symbolic links for all other languages. You can use the following commands:

    ```bash
    $ cd /ClickHouse/clone/directory/docs
    $ ln -sr en/new/file.md lang/new/file.md
    ```
##}
<a name="adding-a-new-language"/>

### Adding a New Language

1. Create a new docs subfolder named using the [ISO-639-1 language code](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes).
2. Add Markdown files with the translation, mirroring the folder structure of other languages.
3. Commit and open a pull-request with the new content.

When everything is ready, we will add the new language to the website.

<a name="what-to-write"/>

## How to Write Content for ClickHouse Documentation


<a name="target-audience"/>

### Documentation for Different Audience

When writing documentation, think about people who read it. Each audience has specific requirements for terms they use in communications.

ClickHouse documentation can be divided by the audience for the following parts:

- Conceptual topics in [Introduction](https://clickhouse.tech/docs/en/), tutorials and overviews, changelog.

    These topics are for the most common auditory. When editing text in them, use the most common terms that are comfortable for the audience with basic technical skills.

- Query language reference and related topics.

    These parts of the documentation are dedicated to those who use ClickHouse for data analysis. Carefully describe syntax, input, and output data for expressions. Don't forget the examples.

- Description of table engines and operation details.

    Operation engineers who help data analysts to solve their tasks should know how to install/update a ClickHouse server, maintain the ClickHouse cluster, how to integrate it with other tools and systems, how to get the maximum performance of their entire environment.

- Developer's guides.

    The documentation provides code writers with information about how to write code for ClickHouse and how to build it in different environments.

<a name="common-recommendations"/>

### Common Recommendations

- When searching for a position for your text, try to place it in the most anticipated place.
- Group entities. For example, if several functions solve similar tasks or belong to a specific group by use case or an application type, place them together.
- Try to avoid slang. Use the most common and specific terms possible. If some terms are used as synonyms, state this explicitly.
- Add examples for all the functionality. Add basic examples to show how the function works by itself. Add use case examples to show how the function participates in solving specific tasks.
- Any text concerning politics, religion, or other social related themes are strictly prohibited in all the ClickHouse texts.
- Proofread your text before publishing. Look for typos, missing punctuation, or repetitions that could be avoided.

<a name="templates"/>

### Description Templates

When writing docs, you can use prepared templates. Copy the code of a template and use it in your contribution. Sometimes you just need to change level of headers.

Templates:

- [Function](_description_templates/template-function.md)
- [Setting](_description_templates/template-setting.md)
- [Server Setting](_description_templates/template-server-setting.md)
- [Database or Table engine](_description_templates/template-engine.md)
- [System table](_description_templates/template-system-table.md)
- [Data type](_description_templates/data-type.md)
- [Statement](_description_templates/statement.md)


<a name="how-to-build-docs"/>

## How to Build Documentation

You can build your documentation manually by following the instructions in [docs/tools/README.md](../docs/tools/README.md). Also, our CI runs the documentation build after the `documentation` label is added to PR. You can see the results of a build in the GitHub interface. If you have no permissions to add labels, a reviewer of your PR will add it.
