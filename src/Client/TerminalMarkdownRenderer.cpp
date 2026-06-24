#include <Client/TerminalMarkdownRenderer.h>

#include <Common/UTF8Helpers.h>

#include <algorithm>
#include <cctype>
#include <functional>
#include <utility>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>


namespace DB
{

namespace
{

/// ANSI SGR sequences. They are emitted only when `ansi` is enabled.
constexpr std::string_view ANSI_RESET = "\033[0m";
constexpr std::string_view ANSI_BOLD = "\033[1m";
constexpr std::string_view ANSI_DIM = "\033[2m";
/// Color of inline `code` spans and of header text. Cyan reads well on both dark and light themes,
/// matching the identifier color of the line-editor highlighter.
constexpr std::string_view ANSI_CODE = "\033[36m";

/// The inline style of a fragment of text. `code` is exclusive: it overrides the other attributes,
/// because an inline code span is rendered with its own color and never combined with bold/italic.
struct Style
{
    bool bold = false;
    bool italic = false;
    bool underline = false;
    bool code = false;

    bool operator==(const Style &) const = default;
};

/// A piece of already-rendered inline text. A word is unbreakable; spaces are the only places a line
/// may be wrapped. Keeping each word self-contained (it opens and closes its own styles) makes word
/// wrapping safe: no escape sequence ever leaks across a line break.
struct Token
{
    String rendered;
    size_t width = 0;
    bool is_space = false;
};

bool isBlank(std::string_view s)
{
    return std::all_of(s.begin(), s.end(), [](char c) { return c == ' ' || c == '\t' || c == '\r'; });
}

std::string_view stripCR(std::string_view s)
{
    if (!s.empty() && s.back() == '\r')
        s.remove_suffix(1);
    return s;
}

size_t leadingSpaces(std::string_view s)
{
    size_t n = 0;
    while (n < s.size() && (s[n] == ' ' || s[n] == '\t'))
        ++n;
    return n;
}

bool isInlineSpace(char c)
{
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

std::string_view trimView(std::string_view s)
{
    while (!s.empty() && isInlineSpace(s.front()))
        s.remove_prefix(1);
    while (!s.empty() && isInlineSpace(s.back()))
        s.remove_suffix(1);
    return s;
}

/// Whether a line is an MDX import statement (e.g. `import X from '@theme/badges/ExperimentalBadge';`).
/// These appear in documentation sources but are website-only and should not be shown in the terminal.
bool isMdxImport(std::string_view line)
{
    line = trimView(line);
    if (!line.starts_with("import "))
        return false;
    return line.find(" from '") != std::string_view::npos || line.find(" from \"") != std::string_view::npos || line.starts_with("import '")
        || line.starts_with("import \"");
}

/// Human-readable text for an MDX badge component such as `<ExperimentalBadge/>`. Known badges get a
/// descriptive label; any other `*Badge` component falls back to its name with the camel case split.
String badgeLabel(std::string_view name)
{
    if (name == "ExperimentalBadge")
        return "Experimental";
    if (name == "BetaBadge")
        return "Beta";
    if (name == "CloudNotSupportedBadge")
        return "Not supported in ClickHouse Cloud";
    if (name == "CloudAvailableBadge")
        return "Available in ClickHouse Cloud";
    if (name == "CloudOnlyBadge")
        return "ClickHouse Cloud only";
    if (name == "PrivatePreviewBadge")
        return "Private Preview";

    std::string_view stem = name;
    if (stem.ends_with("Badge"))
        stem.remove_suffix(5);
    String result;
    for (size_t i = 0; i < stem.size(); ++i)
    {
        if (i > 0 && stem[i] >= 'A' && stem[i] <= 'Z' && !(stem[i - 1] >= 'A' && stem[i - 1] <= 'Z'))
            result += ' ';
        result += stem[i];
    }
    return result;
}

/// Removes a trailing explicit anchor such as `{#projections}` from a header's text. Documentation
/// headers carry these anchors for the website; they are noise in the terminal.
std::string_view stripHeaderAnchor(std::string_view text)
{
    text = trimView(text);
    if (!text.empty() && text.back() == '}')
    {
        const size_t open = text.rfind("{#");
        if (open != std::string_view::npos)
            text = trimView(text.substr(0, open));
    }
    return text;
}

/// The ANSI color of an admonition (`:::note`, `:::warning`, ...) label, chosen by its kind.
std::string_view admonitionColor(std::string_view type)
{
    if (type == "tip" || type == "success")
        return "\033[32m"; /// green
    if (type == "warning" || type == "caution")
        return "\033[33m"; /// yellow
    if (type == "danger" || type == "important" || type == "error")
        return "\033[31m"; /// red
    return "\033[36m"; /// note / info and anything else: cyan
}

/// Whether a closing emphasis delimiter for `c` exists at or after `from`: a run of `c` of length at
/// least `need` that is immediately preceded by a non-space character (i.e. that can close emphasis).
/// Used so that a lone `*`/`_` with no matching partner — e.g. the `*` in `SELECT * FROM t` — is rendered
/// literally instead of silently turning the rest of the text into emphasis.
bool hasEmphasisCloser(std::string_view s, size_t from, char c, size_t need)
{
    for (size_t k = from; k < s.size();)
    {
        if (s[k] != c)
        {
            ++k;
            continue;
        }
        size_t run = 0;
        while (k + run < s.size() && s[k + run] == c)
            ++run;
        if (run >= need && k > 0 && !isInlineSpace(s[k - 1]))
            return true;
        k += run;
    }
    return false;
}


/// Performs the actual rendering. One instance is used per document; it accumulates the result in `out`.
class Renderer
{
public:
    explicit Renderer(const TerminalMarkdownRenderer & config_)
        : config(config_)
        , width(std::max<size_t>(config_.width, 20))
        , ansi(config_.ansi)
    {
    }

    String renderEntry(const String & name, const String & type, const String & description)
    {
        out += '\n';
        if (ansi)
        {
            out += ANSI_BOLD;
            out += ANSI_CODE;
            out += name;
            out += ANSI_RESET;
            out += "  ";
            out += ANSI_DIM;
            out += "(" + type + ")";
            out += ANSI_RESET;
            out += '\n';
            out += ANSI_DIM;
            appendRepeated(horizontalRule(), width);
            out += ANSI_RESET;
        }
        else
        {
            out += name + " (" + type + ")";
            out += '\n';
            appendRepeated("=", width);
        }
        out += "\n\n";

        renderDocument(description);
        return finish();
    }

    String render(const String & markdown)
    {
        renderDocument(markdown);
        return finish();
    }

private:
    const TerminalMarkdownRenderer & config;
    const size_t width;
    const bool ansi;
    String out;

    std::string_view horizontalRule() const { return "─"; }

    void appendRepeated(std::string_view unit, size_t count)
    {
        for (size_t i = 0; i < count; ++i)
            out += unit;
    }

    /// Collapse runs of three or more newlines into a single blank line and trim leading/trailing blank lines.
    String finish()
    {
        String result;
        result.reserve(out.size());
        size_t newlines = 0;
        for (char c : out)
        {
            if (c == '\n')
            {
                ++newlines;
                continue;
            }
            if (!result.empty())
                result.append(std::min<size_t>(newlines, 2), '\n'); /// collapse 3+ newlines into a blank line
            newlines = 0; /// leading blank lines are dropped because nothing is appended while `result` is empty
            result += c;
        }
        result += '\n';
        return result;
    }

    String sgr(Style style) const
    {
        if (!ansi)
            return {};
        if (style.code)
            return String(ANSI_CODE);
        String codes;
        if (style.bold)
            codes += "1;";
        if (style.italic)
            codes += "3;";
        if (style.underline)
            codes += "4;";
        if (codes.empty())
            return {};
        codes.pop_back();
        return "\033[" + codes + "m";
    }

    /// Splits a Markdown string into inline tokens, applying inline styling on top of `base`.
    std::vector<Token> renderInline(std::string_view text, Style base = {}) const
    {
        std::vector<Token> tokens;

        String word;
        size_t word_width = 0;
        Style word_style; /// the style currently emitted into `word`

        int bold = base.bold ? 1 : 0;
        int italic = base.italic ? 1 : 0;
        int underline = base.underline ? 1 : 0;

        auto current_style
            = [&]() -> Style { return Style{.bold = bold > 0, .italic = italic > 0, .underline = underline > 0, .code = false}; };

        auto append = [&](std::string_view plain, Style style)
        {
            if (ansi && !(style == word_style))
            {
                word += ANSI_RESET;
                word += sgr(style);
                word_style = style;
            }
            size_t i = 0;
            while (i < plain.size())
            {
                size_t len = UTF8::seqLength(static_cast<UInt8>(plain[i]));
                if (len == 0 || i + len > plain.size())
                    len = 1;
                word.append(plain.data() + i, len);
                ++word_width;
                i += len;
            }
        };

        auto flush_word = [&]()
        {
            if (word.empty())
                return;
            if (ansi && !(word_style == Style{}))
                word += ANSI_RESET;
            tokens.push_back({std::move(word), word_width, false});
            word.clear();
            word_width = 0;
            word_style = Style{};
        };

        auto push_space = [&]()
        {
            if (!tokens.empty() && !tokens.back().is_space)
                tokens.push_back({" ", 1, true});
        };

        std::function<void(std::string_view)> scan = [&](std::string_view s)
        {
            size_t i = 0;
            while (i < s.size())
            {
                char c = s[i];

                if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
                {
                    flush_word();
                    push_space();
                    ++i;
                    continue;
                }

                if (c == '\\' && i + 1 < s.size())
                {
                    append(s.substr(i + 1, 1), current_style());
                    i += 2;
                    continue;
                }

                if (c == '`')
                {
                    size_t n = 0;
                    while (i + n < s.size() && s[i + n] == '`')
                        ++n;
                    size_t content_begin = i + n;
                    size_t close = std::string_view::npos;
                    for (size_t k = content_begin; k + n <= s.size(); ++k)
                    {
                        if (s[k] == '`')
                        {
                            size_t m = 0;
                            while (k + m < s.size() && s[k + m] == '`')
                                ++m;
                            if (m == n)
                            {
                                close = k;
                                break;
                            }
                            k += m - 1;
                        }
                    }
                    if (close == std::string_view::npos)
                    {
                        append(s.substr(i, n), current_style());
                        i = content_begin;
                        continue;
                    }
                    append(s.substr(content_begin, close - content_begin), Style{.code = true});
                    i = close + n;
                    continue;
                }

                if (c == '*' || c == '_')
                {
                    size_t n = 0;
                    while (i + n < s.size() && s[i + n] == c)
                        ++n;

                    /// An intraword run of `*`/`_` (alphanumeric on both sides) is a literal character,
                    /// not an emphasis delimiter. This keeps identifiers such as `max_threads` and simple
                    /// arithmetic such as `2*2` intact, which matters a lot for technical documentation.
                    auto is_word_char = [](char ch)
                    {
                        return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
                            || static_cast<UInt8>(ch) >= 0x80;
                    };
                    const char before = i > 0 ? s[i - 1] : '\0';
                    const char after = i + n < s.size() ? s[i + n] : '\0';
                    if (is_word_char(before) && is_word_char(after))
                    {
                        append(s.substr(i, n), current_style());
                        i += n;
                        continue;
                    }

                    /// A run of two or more markers is bold; a single marker is italic.
                    const size_t need = n >= 2 ? 2 : 1;
                    int & flag = n >= 2 ? bold : italic;

                    /// A delimiter can close emphasis only if it is not preceded by whitespace, and can open
                    /// it only if it is not followed by whitespace (a relaxation of the CommonMark flanking
                    /// rules). An opener is honored only when a matching closer is present, so unbalanced
                    /// markers are left as literal text rather than corrupting the rest of the document.
                    const bool can_close = before != '\0' && !isInlineSpace(before);
                    const bool can_open = after != '\0' && !isInlineSpace(after);

                    if (flag > 0 && can_close)
                        flag = 0;
                    else if (can_open && hasEmphasisCloser(s, i + n, c, need))
                        flag = 1;
                    else
                    {
                        append(s.substr(i, n), current_style());
                        i += n;
                        continue;
                    }

                    flush_word();
                    i += need;
                    continue;
                }

                if (c == '[')
                {
                    size_t close_bracket = s.find(']', i + 1);
                    if (close_bracket != std::string_view::npos && close_bracket + 1 < s.size() && s[close_bracket + 1] == '(')
                    {
                        size_t close_paren = s.find(')', close_bracket + 2);
                        if (close_paren != std::string_view::npos)
                        {
                            std::string_view link_text = s.substr(i + 1, close_bracket - (i + 1));
                            ++underline;
                            scan(link_text);
                            --underline;
                            i = close_paren + 1;
                            continue;
                        }
                    }
                    append(s.substr(i, 1), current_style());
                    ++i;
                    continue;
                }

                /// MDX badge component, e.g. `<ExperimentalBadge/>`: render its human-readable label.
                if (c == '<')
                {
                    const size_t close = s.find('>', i + 1);
                    if (close != std::string_view::npos && close > i + 1 && s[close - 1] == '/')
                    {
                        size_t p = i + 1;
                        const size_t name_begin = p;
                        while (p < close && ((s[p] >= 'A' && s[p] <= 'Z') || (s[p] >= 'a' && s[p] <= 'z') || (s[p] >= '0' && s[p] <= '9')))
                            ++p;
                        const std::string_view name = s.substr(name_begin, p - name_begin);
                        if (name.size() > 5 && name[0] >= 'A' && name[0] <= 'Z' && name.ends_with("Badge"))
                        {
                            append("[" + badgeLabel(name) + "]", Style{.bold = true});
                            i = close + 1;
                            continue;
                        }
                    }
                    append(s.substr(i, 1), current_style());
                    ++i;
                    continue;
                }

                size_t len = UTF8::seqLength(static_cast<UInt8>(c));
                if (len == 0 || i + len > s.size())
                    len = 1;
                append(s.substr(i, len), current_style());
                i += len;
            }
        };

        scan(text);
        flush_word();
        return tokens;
    }

    /// Greedily wraps tokens into lines of at most `width` visible columns. The first line is indented
    /// by `first_indent`, the continuation lines by `hang_indent`.
    void layout(const std::vector<Token> & tokens, size_t first_indent, size_t hang_indent)
    {
        size_t indent = first_indent;
        out.append(indent, ' ');
        size_t col = indent;
        bool has_content = false;
        bool pending_space = false;

        for (const auto & token : tokens)
        {
            if (token.is_space)
            {
                if (has_content)
                    pending_space = true;
                continue;
            }

            size_t need = (pending_space ? 1 : 0) + token.width;
            if (has_content && col + need > width)
            {
                out += '\n';
                indent = hang_indent;
                out.append(indent, ' ');
                col = indent;
                pending_space = false;
                out += token.rendered;
                col += token.width;
            }
            else
            {
                if (pending_space)
                {
                    out += ' ';
                    ++col;
                    pending_space = false;
                }
                out += token.rendered;
                col += token.width;
            }
            has_content = true;
        }
        out += '\n';
    }

    /// Joins tokens into a single line (no wrapping), returning the rendered string and its visible width.
    std::pair<String, size_t> inlineLine(std::string_view text, Style base = {}) const
    {
        auto tokens = renderInline(text, base);
        String result;
        size_t visible = 0;
        bool pending_space = false;
        for (const auto & token : tokens)
        {
            if (token.is_space)
            {
                if (!result.empty())
                    pending_space = true;
                continue;
            }
            if (pending_space)
            {
                result += ' ';
                ++visible;
                pending_space = false;
            }
            result += token.rendered;
            visible += token.width;
        }
        return {result, visible};
    }

    void renderHeader(size_t level, std::string_view text)
    {
        out += '\n';
        if (ansi)
        {
            out += ANSI_CODE;
            out += inlineLine(text, Style{.bold = true}).first;
            out += ANSI_RESET;
            out += '\n';
        }
        else
        {
            auto [line, visible] = inlineLine(text);
            out += line;
            out += '\n';
            appendRepeated(level <= 1 ? "=" : "-", std::min(visible, width));
            out += '\n';
        }
    }

    void renderCodeBlock(const std::vector<std::string_view> & code_lines, std::string_view lang)
    {
        String code;
        for (size_t i = 0; i < code_lines.size(); ++i)
        {
            if (i)
                code += '\n';
            code += code_lines[i];
        }

        const bool is_sql = lang.empty() || boost::iequals(lang, "sql");
        String rendered_code = (is_sql && config.highlight_sql) ? config.highlight_sql(code) : code;

        out += '\n';
        std::string_view rest = rendered_code;
        while (true)
        {
            size_t nl = rest.find('\n');
            std::string_view line = rest.substr(0, nl);
            out += "    "; /// left gutter
            if (ansi && !(is_sql && config.highlight_sql))
            {
                out += ANSI_DIM;
                out += line;
                out += ANSI_RESET;
            }
            else
                out += line;
            out += '\n';
            if (nl == std::string_view::npos)
                break;
            rest = rest.substr(nl + 1);
        }
    }

    void renderBlockquote(const std::vector<std::string_view> & quote_lines)
    {
        out += '\n';
        for (auto line : quote_lines)
        {
            if (ansi)
                out += ANSI_DIM;
            out += "│ ";
            out += inlineLine(line).first;
            if (ansi)
                out += ANSI_RESET;
            out += '\n';
        }
    }

    /// Renders a Docusaurus admonition (`:::note`, `:::tip`, `:::warning`, ...): a colored label followed
    /// by the admonition body rendered as ordinary Markdown.
    void renderAdmonition(std::string_view type, std::string_view title, const std::vector<std::string_view> & body_lines)
    {
        String label;
        if (!title.empty())
            label = String(title);
        else
        {
            label = String(type);
            std::transform(label.begin(), label.end(), label.begin(), [](char c) { return std::toupper(static_cast<unsigned char>(c)); });
        }

        out += '\n';
        if (ansi)
        {
            out += ANSI_BOLD;
            out += admonitionColor(type);
            out += label;
            out += ANSI_RESET;
        }
        else
        {
            out += label;
            out += ':';
        }
        out += '\n';

        String body;
        for (size_t k = 0; k < body_lines.size(); ++k)
        {
            if (k)
                body += '\n';
            body += body_lines[k];
        }
        renderDocument(body);
    }

    static bool isTableSeparator(std::string_view line)
    {
        line = trimView(stripCR(line));
        if (line.empty())
            return false;
        bool has_dash = false;
        for (char c : line)
        {
            if (c == '-')
                has_dash = true;
            else if (c != '|' && c != ':' && c != ' ')
                return false;
        }
        return has_dash;
    }

    static std::vector<String> splitRow(std::string_view line)
    {
        line = trimView(stripCR(line));
        if (!line.empty() && line.front() == '|')
            line.remove_prefix(1);
        if (!line.empty() && line.back() == '|')
            line.remove_suffix(1);

        std::vector<String> cells;
        String cell;
        for (size_t i = 0; i < line.size(); ++i)
        {
            if (line[i] == '\\' && i + 1 < line.size() && line[i + 1] == '|')
            {
                cell += '|';
                ++i;
            }
            else if (line[i] == '|')
            {
                cells.push_back(boost::trim_copy(cell));
                cell.clear();
            }
            else
                cell += line[i];
        }
        cells.push_back(boost::trim_copy(cell));
        return cells;
    }

    void renderTable(const std::vector<std::string_view> & rows)
    {
        std::vector<std::vector<std::pair<String, size_t>>> rendered;
        rendered.reserve(rows.size());
        size_t num_columns = 0;
        for (size_t r = 0; r < rows.size(); ++r)
        {
            std::vector<std::pair<String, size_t>> cells;
            Style base = (r == 0) ? Style{.bold = true} : Style{};
            for (const auto & cell : splitRow(rows[r]))
                cells.push_back(inlineLine(cell, base));
            num_columns = std::max(num_columns, cells.size());
            rendered.push_back(std::move(cells));
        }

        std::vector<size_t> col_width(num_columns, 0);
        for (const auto & cells : rendered)
            for (size_t c = 0; c < cells.size(); ++c)
                col_width[c] = std::max(col_width[c], cells[c].second);

        const std::string_view h = ansi ? "─" : "-";
        const std::string_view v = ansi ? "│" : "|";
        const std::string_view tl = ansi ? "┌" : "+";
        const std::string_view tm = ansi ? "┬" : "+";
        const std::string_view tr = ansi ? "┐" : "+";
        const std::string_view ml = ansi ? "├" : "+";
        const std::string_view mm = ansi ? "┼" : "+";
        const std::string_view mr = ansi ? "┤" : "+";
        const std::string_view bl = ansi ? "└" : "+";
        const std::string_view bm = ansi ? "┴" : "+";
        const std::string_view br = ansi ? "┘" : "+";

        auto border = [&](std::string_view left, std::string_view mid, std::string_view right)
        {
            if (ansi)
                out += ANSI_DIM;
            out += left;
            for (size_t c = 0; c < num_columns; ++c)
            {
                appendRepeated(h, col_width[c] + 2);
                out += (c + 1 == num_columns) ? right : mid;
            }
            if (ansi)
                out += ANSI_RESET;
            out += '\n';
        };

        auto data_row = [&](const std::vector<std::pair<String, size_t>> & cells)
        {
            for (size_t c = 0; c < num_columns; ++c)
            {
                if (ansi)
                    out += ANSI_DIM;
                out += v;
                if (ansi)
                    out += ANSI_RESET;
                out += ' ';
                size_t visible = 0;
                if (c < cells.size())
                {
                    out += cells[c].first;
                    visible = cells[c].second;
                }
                out.append(col_width[c] - visible + 1, ' ');
            }
            if (ansi)
                out += ANSI_DIM;
            out += v;
            if (ansi)
                out += ANSI_RESET;
            out += '\n';
        };

        out += '\n';
        border(tl, tm, tr);
        data_row(rendered[0]);
        border(ml, mm, mr);
        for (size_t r = 1; r < rendered.size(); ++r)
            data_row(rendered[r]);
        border(bl, bm, br);
    }

    static bool listMarker(std::string_view line, size_t indent, size_t & marker_len, String & marker, bool & ordered)
    {
        size_t i = indent;
        if (i < line.size() && (line[i] == '-' || line[i] == '*' || line[i] == '+'))
        {
            if (i + 1 < line.size() && line[i + 1] == ' ')
            {
                ordered = false;
                marker = "•";
                marker_len = 2;
                return true;
            }
            return false;
        }
        size_t digits = 0;
        while (i < line.size() && line[i] >= '0' && line[i] <= '9')
        {
            ++i;
            ++digits;
        }
        if (digits > 0 && i < line.size() && (line[i] == '.' || line[i] == ')') && i + 1 < line.size() && line[i + 1] == ' ')
        {
            ordered = true;
            marker = String(line.substr(indent, i - indent + 1));
            marker_len = i - indent + 2;
            return true;
        }
        return false;
    }

    /// Renders a list whose items are given as raw lines (already determined to start with a marker).
    void renderList(const std::vector<std::string_view> & item_lines)
    {
        out += '\n';
        for (auto raw : item_lines)
        {
            std::string_view line = stripCR(raw);
            size_t indent = leadingSpaces(line);
            size_t marker_len = 0;
            String marker;
            bool ordered = false;
            listMarker(line, indent, marker_len, marker, ordered);

            std::string_view content = line.substr(indent + marker_len);
            size_t base_indent = indent;
            String prefix(base_indent, ' ');
            prefix += marker;
            prefix += ' ';

            out += prefix;
            /// `prefix` may contain a multi-byte bullet; its visible width is base_indent + marker_len.
            size_t first_indent_width = base_indent + marker_len;
            auto tokens = renderInline(content);

            /// Lay out with a manual first prefix already written.
            size_t hang = base_indent + marker_len;
            size_t col = first_indent_width;
            bool has_content = false;
            bool pending_space = false;
            for (const auto & token : tokens)
            {
                if (token.is_space)
                {
                    if (has_content)
                        pending_space = true;
                    continue;
                }
                size_t need = (pending_space ? 1 : 0) + token.width;
                if (has_content && col + need > width)
                {
                    out += '\n';
                    out.append(hang, ' ');
                    col = hang;
                    pending_space = false;
                    out += token.rendered;
                    col += token.width;
                }
                else
                {
                    if (pending_space)
                    {
                        out += ' ';
                        ++col;
                        pending_space = false;
                    }
                    out += token.rendered;
                    col += token.width;
                }
                has_content = true;
            }
            out += '\n';
        }
    }

    bool startsNewBlock(std::string_view line) const
    {
        std::string_view s = stripCR(line);
        if (isBlank(s))
            return true;
        size_t indent = leadingSpaces(s);
        std::string_view stripped = s.substr(indent);
        if (stripped.starts_with("```") || stripped.starts_with("~~~"))
            return true;
        if (stripped.starts_with("#"))
            return true;
        if (stripped.starts_with(">"))
            return true;
        if (stripped.starts_with(":::"))
            return true;
        if (isMdxImport(stripped))
            return true;
        size_t ml = 0;
        String marker;
        bool ordered = false;
        if (listMarker(s, indent, ml, marker, ordered))
            return true;
        return false;
    }

    void renderDocument(const String & markdown)
    {
        std::vector<std::string_view> lines;
        {
            std::string_view rest = markdown;
            while (true)
            {
                size_t nl = rest.find('\n');
                lines.push_back(rest.substr(0, nl));
                if (nl == std::string_view::npos)
                    break;
                rest = rest.substr(nl + 1);
            }
        }

        size_t i = 0;
        while (i < lines.size())
        {
            std::string_view line = stripCR(lines[i]);

            if (isBlank(line))
            {
                out += '\n';
                ++i;
                continue;
            }

            size_t indent = leadingSpaces(line);
            std::string_view stripped = line.substr(indent);

            /// MDX import/export statements (e.g. `import X from '@theme/...';`) are website-only; drop them.
            if (isMdxImport(stripped))
            {
                ++i;
                continue;
            }

            /// Docusaurus admonition: `:::type [title]` ... `:::`. The fences may use more than three
            /// colons (e.g. `::::note` ... `::::`), and the close fence must match the open fence's length.
            if (stripped.starts_with(":::"))
            {
                size_t fence_len = 0;
                while (fence_len < stripped.size() && stripped[fence_len] == ':')
                    ++fence_len;
                const std::string_view spec = trimView(stripped.substr(fence_len));
                ++i; /// always consume the fence line, so a stray close fence cannot stall the loop
                if (!spec.empty()) /// an opening fence carries a type; a bare colon run is just a close fence
                {
                    const size_t space = spec.find_first_of(" \t");
                    const std::string_view type = space == std::string_view::npos ? spec : spec.substr(0, space);
                    const std::string_view title = space == std::string_view::npos ? std::string_view{} : trimView(spec.substr(space));
                    std::vector<std::string_view> body_lines;
                    while (i < lines.size())
                    {
                        const std::string_view bt = trimView(stripCR(lines[i]));
                        if (bt.size() == fence_len && bt.find_first_not_of(':') == std::string_view::npos)
                        {
                            ++i;
                            break;
                        }
                        body_lines.push_back(stripCR(lines[i]));
                        ++i;
                    }
                    renderAdmonition(type, title, body_lines);
                }
                continue;
            }

            /// Fenced code block. The fence may be longer than three characters, and the info string may
            /// carry attributes (e.g. ```sql title=Query) — the language is the first token of the info string.
            if (stripped.starts_with("```") || stripped.starts_with("~~~"))
            {
                const char fence = stripped[0];
                size_t fence_len = 0;
                while (fence_len < stripped.size() && stripped[fence_len] == fence)
                    ++fence_len;

                std::string_view lang = trimView(stripped.substr(fence_len));
                const size_t lang_end = lang.find_first_of(" \t");
                if (lang_end != std::string_view::npos)
                    lang = lang.substr(0, lang_end);

                std::vector<std::string_view> code_lines;
                ++i;
                while (i < lines.size())
                {
                    const std::string_view code_line = stripCR(lines[i]);
                    const std::string_view cs = code_line.substr(leadingSpaces(code_line));
                    size_t close_len = 0;
                    while (close_len < cs.size() && cs[close_len] == fence)
                        ++close_len;
                    if (close_len >= fence_len && trimView(cs.substr(close_len)).empty())
                    {
                        ++i;
                        break;
                    }
                    code_lines.push_back(code_line);
                    ++i;
                }
                renderCodeBlock(code_lines, lang);
                continue;
            }

            /// Table: a row of cells followed by a separator row.
            if (line.find('|') != std::string_view::npos && i + 1 < lines.size() && isTableSeparator(lines[i + 1]))
            {
                std::vector<std::string_view> rows;
                rows.push_back(line);
                i += 2; /// skip the header and the separator
                while (i < lines.size() && !isBlank(lines[i]) && lines[i].find('|') != std::string_view::npos
                       && !stripCR(lines[i]).substr(leadingSpaces(lines[i])).starts_with("```"))
                {
                    rows.push_back(stripCR(lines[i]));
                    ++i;
                }
                renderTable(rows);
                continue;
            }

            /// ATX header.
            if (stripped.starts_with("#"))
            {
                size_t level = 0;
                while (level < stripped.size() && stripped[level] == '#')
                    ++level;
                renderHeader(level, stripHeaderAnchor(stripped.substr(level)));
                ++i;
                continue;
            }

            /// Blockquote.
            if (stripped.starts_with(">"))
            {
                std::vector<std::string_view> quote_lines;
                while (i < lines.size())
                {
                    std::string_view ql = stripCR(lines[i]);
                    std::string_view qs = ql.substr(leadingSpaces(ql));
                    if (!qs.starts_with(">"))
                        break;
                    qs.remove_prefix(1);
                    if (qs.starts_with(" "))
                        qs.remove_prefix(1);
                    quote_lines.push_back(qs);
                    ++i;
                }
                renderBlockquote(quote_lines);
                continue;
            }

            /// List.
            {
                size_t ml = 0;
                String marker;
                bool ordered = false;
                if (listMarker(line, indent, ml, marker, ordered))
                {
                    std::vector<std::string_view> item_lines;
                    while (i < lines.size())
                    {
                        std::string_view il = stripCR(lines[i]);
                        if (isBlank(il))
                            break;
                        size_t li = leadingSpaces(il);
                        size_t m2 = 0;
                        String mk;
                        bool ord = false;
                        if (!listMarker(il, li, m2, mk, ord))
                            break;
                        item_lines.push_back(il);
                        ++i;
                    }
                    renderList(item_lines);
                    continue;
                }
            }

            /// Paragraph: gather consecutive lines until a blank line or the start of another block.
            {
                String paragraph;
                while (i < lines.size())
                {
                    std::string_view pl = stripCR(lines[i]);
                    if (startsNewBlock(pl))
                        break;
                    if (pl.find('|') != std::string_view::npos && i + 1 < lines.size() && isTableSeparator(lines[i + 1]))
                        break;
                    if (!paragraph.empty())
                        paragraph += ' ';
                    paragraph += trimView(pl);
                    ++i;
                }
                layout(renderInline(paragraph), 0, 0);
                continue;
            }
        }
    }
};

}


String TerminalMarkdownRenderer::renderEntry(const String & name, const String & type, const String & description) const
{
    return Renderer(*this).renderEntry(name, type, description);
}

String TerminalMarkdownRenderer::render(const String & markdown) const
{
    return Renderer(*this).render(markdown);
}

}
