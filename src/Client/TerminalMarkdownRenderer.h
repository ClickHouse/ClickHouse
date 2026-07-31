#pragma once

#include <base/types.h>

#include <functional>


namespace DB
{

/// Renders a Markdown document (such as the `description` column of `system.documentation`) into a
/// string suitable for printing in a terminal. It is used by the interactive `help` command of the
/// client to display the embedded documentation.
///
/// The following block constructs are recognized: ATX headers, paragraphs (word-wrapped to `width`),
/// ordered and unordered lists, blockquotes, GitHub-style tables, and fenced code blocks. Inside text,
/// the inline constructs bold (`**...**`), italic (`*...*`/`_..._`), inline code (`` `...` ``) and
/// links (`[text](url)`) are rendered. SQL code blocks are passed through `highlight_sql` for syntax
/// highlighting, exactly as the line editor highlights queries.
struct TerminalMarkdownRenderer
{
    /// The target line width for word wrapping. Code blocks and tables are not wrapped.
    size_t width = 80;

    /// Whether to emit ANSI escape sequences (colors, bold, italic, ...). When false, a plain-text
    /// rendering is produced (used when the output is not a terminal, e.g. when piped to a file).
    bool ansi = true;

    /// Highlights a fragment of SQL, returning a string that may contain ANSI escape sequences. When
    /// not set, SQL code blocks are rendered verbatim.
    std::function<String(const String & sql)> highlight_sql;

    /// Renders the documentation of a single entity: a banner with its `name` and `type`, followed by
    /// the rendered Markdown `description`.
    String renderEntry(const String & name, const String & type, const String & description) const;

    /// Renders a standalone Markdown document.
    String render(const String & markdown) const;
};

}
