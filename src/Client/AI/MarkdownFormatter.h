#pragma once

#include <string>

namespace DB
{

/// Render a Markdown string to text with ANSI escape codes for a terminal: headers and
/// `**bold**` become bold, `*italic*` italic, `` `code` `` and fenced code blocks are
/// colored, list markers are normalized to a bullet, and `[text](url)` links are shown as
/// underlined text followed by the URL. Only the subset of Markdown that the model commonly
/// emits is handled; anything unrecognized is passed through unchanged.
///
/// The result has no trailing newline. Intended for use only when the output is a terminal
/// (colors enabled); for non-terminal output, print the raw Markdown instead.
std::string renderMarkdownToANSI(const std::string & markdown);

}
