#include <Client/AI/MarkdownFormatter.h>

#include <cctype>
#include <string>
#include <string_view>

namespace DB
{

namespace
{

/// ANSI escape sequences.
constexpr std::string_view ansi_reset = "\033[0m";
constexpr std::string_view ansi_bold = "\033[1m";
constexpr std::string_view ansi_dim = "\033[2m";
constexpr std::string_view ansi_italic = "\033[3m";
constexpr std::string_view ansi_underline = "\033[4m";
constexpr std::string_view ansi_code = "\033[36m"; /// cyan for inline code and code blocks
constexpr std::string_view ansi_header = "\033[1;4m"; /// bold + underline for headers

bool startsWith(const std::string & s, size_t pos, std::string_view prefix)
{
    return pos + prefix.size() <= s.size() && std::string_view(s).substr(pos, prefix.size()) == prefix;
}

/// A "word" character for the purpose of emphasis boundaries: an ASCII alphanumeric, or any byte
/// of a UTF-8 multibyte sequence (high bit set), so that letters of non-Latin scripts also count.
bool isWordChar(char c)
{
    const auto byte = static_cast<unsigned char>(c);
    return (byte & 0x80) != 0 || std::isalnum(byte) != 0;
}

/// `*`/`_` emphasis is applied only at word boundaries, never inside a word, so that identifiers
/// like `max_execution_time` and expressions like `2*n` are left untouched. The opening delimiter
/// (at `open`) must not be preceded by a word character, and the closing delimiter (whose run ends
/// just before `after_close`) must not be followed by one.
bool emphasisAtWordBoundary(const std::string & line, size_t open, size_t after_close)
{
    const bool open_ok = open == 0 || !isWordChar(line[open - 1]);
    const bool close_ok = after_close >= line.size() || !isWordChar(line[after_close]);
    return open_ok && close_ok;
}

/// Render inline spans (`code`, **bold**, *italic*, [text](url)) of a single line.
/// Unmatched or malformed markers are emitted verbatim, so arbitrary text is never corrupted.
std::string renderInline(const std::string & line)
{
    std::string out;
    size_t i = 0;
    const size_t n = line.size();

    while (i < n)
    {
        const char c = line[i];

        /// Inline code: `...` - contents are emitted verbatim, without further interpretation.
        if (c == '`')
        {
            const size_t close = line.find('`', i + 1);
            if (close != std::string::npos && close > i + 1)
            {
                out += ansi_code;
                out += line.substr(i + 1, close - i - 1);
                out += ansi_reset;
                i = close + 1;
                continue;
            }
        }

        /// Bold: **...** or __...__ (checked before italic so `*` is not consumed as italic).
        if (startsWith(line, i, "**") || startsWith(line, i, "__"))
        {
            const std::string_view delimiter = std::string_view(line).substr(i, 2);
            const size_t close = line.find(delimiter, i + 2);
            if (close != std::string::npos && close > i + 2 && emphasisAtWordBoundary(line, i, close + 2))
            {
                out += ansi_bold;
                out += renderInline(line.substr(i + 2, close - i - 2));
                out += ansi_reset;
                i = close + 2;
                continue;
            }
        }

        /// Italic: *...* or _..._ (a single delimiter, not the start of a bold run). Applied only
        /// at word boundaries, so underscores/asterisks inside a word (`max_execution_time`, `2*n`)
        /// are left as-is.
        if (c == '*' || c == '_')
        {
            const size_t close = line.find(c, i + 1);
            if (close != std::string::npos && close > i + 1 && emphasisAtWordBoundary(line, i, close + 1))
            {
                out += ansi_italic;
                out += line.substr(i + 1, close - i - 1);
                out += ansi_reset;
                i = close + 1;
                continue;
            }
        }

        /// Link: [text](url) - shown as underlined text followed by the URL in parentheses.
        if (c == '[')
        {
            const size_t close_bracket = line.find(']', i + 1);
            if (close_bracket != std::string::npos && close_bracket + 1 < n && line[close_bracket + 1] == '(')
            {
                const size_t close_paren = line.find(')', close_bracket + 2);
                if (close_paren != std::string::npos)
                {
                    const std::string text = line.substr(i + 1, close_bracket - i - 1);
                    const std::string url = line.substr(close_bracket + 2, close_paren - close_bracket - 2);
                    out += ansi_underline;
                    out += renderInline(text);
                    out += ansi_reset;
                    out += " (";
                    out += ansi_dim;
                    out += url;
                    out += ansi_reset;
                    out += ')';
                    i = close_paren + 1;
                    continue;
                }
            }
        }

        out += c;
        ++i;
    }

    return out;
}

/// The count of leading `#` characters if `line` is an ATX header (`#` .. `######` followed by
/// a space), else 0.
size_t headerLevel(const std::string & line)
{
    size_t level = 0;
    while (level < line.size() && line[level] == '#')
        ++level;
    if (level >= 1 && level <= 6 && level < line.size() && line[level] == ' ')
        return level;
    return 0;
}

/// A fence line is ``` or ~~~ (optionally followed by an info string), possibly indented.
bool isCodeFence(const std::string & line, size_t indent)
{
    return startsWith(line, indent, "```") || startsWith(line, indent, "~~~");
}

/// A horizontal rule: at least three `-`, `*` or `_`, nothing else but spaces.
bool isHorizontalRule(const std::string & line)
{
    char marker = 0;
    size_t count = 0;
    for (const char c : line)
    {
        if (c == ' ')
            continue;
        if (c != '-' && c != '*' && c != '_')
            return false;
        if (marker == 0)
            marker = c;
        else if (c != marker)
            return false;
        ++count;
    }
    return count >= 3;
}

}

std::string renderMarkdownToANSI(const std::string & markdown)
{
    std::string out;
    bool in_code_fence = false;

    size_t pos = 0;
    const size_t size = markdown.size();
    bool first_line = true;

    while (pos <= size)
    {
        size_t end = markdown.find('\n', pos);
        const bool last_line = end == std::string::npos;
        if (last_line)
            end = size;

        std::string line = markdown.substr(pos, end - pos);
        /// Tolerate CRLF input.
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        /// A trailing newline in the source produces one empty final segment; do not emit a
        /// blank line for it (the caller adds the final newline).
        if (last_line && line.empty() && !first_line)
            break;

        if (!first_line)
            out += '\n';
        first_line = false;

        size_t indent = 0;
        while (indent < line.size() && line[indent] == ' ')
            ++indent;

        if (isCodeFence(line, indent))
        {
            /// The fence markers themselves are not shown; contents are colored as code.
            in_code_fence = !in_code_fence;
            /// Emit nothing for the fence line, but we already added a '\n' separator above;
            /// undo it so fences do not leave blank lines.
            if (!out.empty() && out.back() == '\n')
                out.pop_back();
            first_line = out.empty();
            pos = end + 1;
            continue;
        }

        if (in_code_fence)
        {
            out += ansi_code;
            out += line;
            out += ansi_reset;
            pos = end + 1;
            continue;
        }

        if (const size_t level = headerLevel(line))
        {
            out += ansi_header;
            out += renderInline(line.substr(level + 1));
            out += ansi_reset;
            pos = end + 1;
            continue;
        }

        if (isHorizontalRule(line))
        {
            out += ansi_dim;
            out += "────────────────────";
            out += ansi_reset;
            pos = end + 1;
            continue;
        }

        /// Unordered list item: normalize the `-`/`*`/`+` marker to a bullet.
        if (indent + 1 < line.size() && (line[indent] == '-' || line[indent] == '*' || line[indent] == '+')
            && line[indent + 1] == ' ')
        {
            out += line.substr(0, indent);
            out += "• ";
            out += renderInline(line.substr(indent + 2));
            pos = end + 1;
            continue;
        }

        out += renderInline(line);
        pos = end + 1;
    }

    return out;
}

}
