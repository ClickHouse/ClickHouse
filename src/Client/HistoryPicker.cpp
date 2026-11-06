#include <Client/HistoryPicker.h>
#include <Common/StringSearcher.h>
#include <Common/StringUtils.h>
#include <Common/TerminalSize.h>
#include <Common/UTF8Helpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <base/find_symbols.h>

#include <algorithm>
#include <string_view>
#include <unordered_set>

#include <sys/ttydefaults.h>

namespace DB
{

namespace
{

constexpr std::string_view PASTE_END = "\x1b[201~";

/// The colors of the `dark256` theme of skim (`src/theme.rs`), the default of the Rust picker this replaced.
constexpr std::string_view COLOR_PROMPT = "\x1b[38;5;110m";
constexpr std::string_view COLOR_INFO = "\x1b[38;5;144m";
constexpr std::string_view COLOR_MATCHED = "\x1b[38;5;108;48;5;0m";
constexpr std::string_view COLOR_MATCHED_END = "\x1b[39;49m";
constexpr std::string_view COLOR_CURRENT = "\x1b[38;5;254;48;5;236m";
constexpr std::string_view COLOR_CURRENT_FOREGROUND = "\x1b[38;5;254m";
constexpr std::string_view COLOR_CURRENT_MATCH = "\x1b[38;5;151m";
constexpr std::string_view COLOR_CURSOR = "\x1b[38;5;161m";
constexpr std::string_view COLOR_RESET = "\x1b[0m";

struct Entry
{
    size_t index; /// Into the original history.
    std::string display;
};

/// Shows the text on a single line, as skim did, and makes control characters inert: history and pasted text
/// must not drive the terminal. Keeps the byte offsets, so the matches found in the result apply to it as is.
std::string toDisplay(std::string_view text)
{
    std::string res(text);
    for (size_t i = 0; i < res.size(); ++i)
    {
        auto c = static_cast<UInt8>(res[i]);
        if (c == '\n' || c == '\r' || c == '\t')
        {
            res[i] = ' ';
        }
        else if (isControlASCII(res[i]) || res[i] == '\x7f') /// DEL
        {
            res[i] = '?';
        }
        else if (c == 0xC2 && i + 1 < res.size() && static_cast<UInt8>(res[i + 1]) >= 0x80 && static_cast<UInt8>(res[i + 1]) <= 0x9F)
        {
            /// U+0080..U+009F, the C1 controls: some terminals execute them like the `ESC` sequences.
            res[i] = '?';
            res[++i] = '?';
        }
    }
    return res;
}

const UInt8 * toBytes(std::string_view s)
{
    return reinterpret_cast<const UInt8 *>(s.data());
}

size_t displayWidth(std::string_view s)
{
    return UTF8::computeWidth(toBytes(s), s.size());
}

std::string_view truncateToWidth(std::string_view s, size_t limit)
{
    return s.substr(0, UTF8::computeBytesBeforeWidth(toBytes(s), s.size(), 0, limit));
}

/// The end of `s` that fits in `limit` columns.
std::string_view tailOfWidth(std::string_view s, size_t limit)
{
    size_t width = displayWidth(s);
    if (width <= limit)
        return s;
    size_t excess = width - limit;
    size_t drop = UTF8::computeBytesBeforeWidth(toBytes(s), s.size(), 0, excess);
    /// The widest prefix within the excess is narrower than it when a wide character crosses the boundary.
    if (drop < s.size() && displayWidth(s.substr(0, drop)) < excess)
        drop += std::min<size_t>(UTF8::seqLength(static_cast<UInt8>(s[drop])), s.size() - drop);
    return s.substr(drop);
}

/// The start of the code point before `pos`.
size_t previousCodePoint(std::string_view s, size_t pos)
{
    if (pos == 0)
        return 0;
    --pos;
    while (pos > 0 && UTF8::isContinuationOctet(static_cast<UInt8>(s[pos])))
        --pos;
    return pos;
}

/// The start of the code point after the one at `pos`.
size_t nextCodePoint(std::string_view s, size_t pos)
{
    return pos < s.size() ? std::min<size_t>(pos + UTF8::seqLength(static_cast<UInt8>(s[pos])), s.size()) : pos;
}

class Picker
{
public:
    Picker(int in_fd_, int out_fd_, const std::string & initial_query, const std::vector<std::string> & history_)
        : in_fd(in_fd_), out_fd(out_fd_), in(in_fd_, /* buf_size */ 1), out(out_fd_), history(history_), query(initial_query)
        , cursor(query.size())
    {
        auto [terminal_cols, terminal_rows] = getTerminalSize(in_fd, out_fd);
        setGeometry(terminal_cols, terminal_rows);

        std::unordered_set<std::string_view> seen;
        entries.reserve(history.size());
        for (size_t i = history.size(); i-- > 0;)
        {
            if (!seen.insert(history[i]).second)
                continue;
            entries.push_back(Entry{i, toDisplay(history[i])});
        }
        matches.reserve(entries.size());
    }

    std::string run()
    {
        write(reserveArea());

        std::string result;
        try
        {
            result = loop();
        }
        catch (...)
        {
            write(clearArea());
            throw;
        }
        write(clearArea());
        out.finalize();
        return result;
    }

private:
    int in_fd;
    int out_fd;
    /// Unbuffered input: bytes typed after the picker closes belong to replxx.
    ReadBufferFromFileDescriptor in;
    WriteBufferFromFileDescriptor out;
    const std::vector<std::string> & history;
    std::string query; /// As typed, it is the result when nothing matches.
    size_t cursor; /// Byte offset in `query`.

    size_t rows = 0;
    size_t cols = 0;
    size_t height = 0;
    size_t list_rows = 0;

    std::vector<Entry> entries;
    std::vector<size_t> matches; /// Indexes into `entries`.
    std::string display_query;
    std::vector<UTF8CaseInsensitiveStringSearcher> searchers; /// For the words of `display_query`, point into it.
    size_t selected = 0;
    size_t offset = 0;

    void setGeometry(size_t terminal_cols, size_t terminal_rows)
    {
        rows = terminal_rows ? terminal_rows : 24;
        cols = terminal_cols ? terminal_cols : 80;
        height = std::min(rows, std::max<size_t>(4, rows * 30 / 100));
        list_rows = height > 2 ? height - 2 : 0;
    }

    /// Returns whether the terminal was resized; then the area is drawn anew in the new size.
    bool handleResize()
    {
        auto [terminal_cols, terminal_rows] = getTerminalSize(in_fd, out_fd);
        if (terminal_cols == cols && terminal_rows == rows)
            return false;
        std::string frame = clearArea();
        setGeometry(terminal_cols, terminal_rows);
        frame += reserveArea();
        write(frame);
        return true;
    }

    void refilter()
    {
        display_query = toDisplay(query);
        std::vector<std::string_view> words;
        splitInto<' '>(words, display_query, /* token_compress */ true);
        searchers.clear();
        searchers.reserve(words.size());
        for (std::string_view word : words)
            searchers.emplace_back(toBytes(word), word.size());

        matches.clear();
        for (size_t i = 0; i < entries.size(); ++i)
        {
            const UInt8 * begin = toBytes(entries[i].display);
            const UInt8 * end = begin + entries[i].display.size();
            if (std::all_of(searchers.begin(), searchers.end(), [&](const auto & searcher) { return searcher.search(begin, end) != end; }))
                matches.push_back(i);
        }
        selected = 0;
        offset = 0;
    }

    void moveSelection(ssize_t delta)
    {
        if (matches.empty())
            return;
        ssize_t pos = static_cast<ssize_t>(selected) + delta;
        selected = static_cast<size_t>(std::clamp<ssize_t>(pos, 0, static_cast<ssize_t>(matches.size()) - 1));
    }

    void redraw()
    {
        offset = std::min(selected, offset);
        if (list_rows && selected >= offset + list_rows)
            offset = selected - list_rows + 1;

        /// Leave the last column empty to avoid the terminal's pending-wrap state.
        size_t content_width = cols > 3 ? cols - 3 : 1;

        /// The part of the query around the cursor, which stays visible.
        std::string_view before_cursor = tailOfWidth(std::string_view(display_query).substr(0, cursor), content_width);
        size_t query_start = before_cursor.data() - display_query.data();
        std::string_view visible_query = truncateToWidth(std::string_view(display_query).substr(query_start), content_width);

        /// Bottom-up like skim and fzf: the best match is the lowest row, right above the count and the prompt.
        std::string frame = moveToTop();
        for (size_t row = list_rows; row-- > 0;)
        {
            frame += "\x1b[2K";
            size_t pos = offset + row;
            if (pos >= matches.size())
            {
                frame += "\r\n";
                continue;
            }
            const Entry & entry = entries[matches[pos]];
            auto [text, elided] = visibleRow(entry.display, content_width);
            if (pos == selected)
            {
                frame += COLOR_CURRENT;
                frame += COLOR_CURSOR;
                frame += ">";
                frame += COLOR_CURRENT_FOREGROUND;
                frame += elided ? " .." : " ";
                appendHighlighted(frame, text, COLOR_CURRENT_MATCH, COLOR_CURRENT_FOREGROUND);
                /// Erasing with the current background extends it to the whole row.
                frame += "\x1b[K";
                frame += COLOR_RESET;
            }
            else
            {
                frame += elided ? "  .." : "  ";
                appendHighlighted(frame, text, COLOR_MATCHED, COLOR_MATCHED_END);
            }
            frame += "\r\n";
        }
        frame += "\x1b[2K";
        frame += COLOR_INFO;
        frame += "  " + std::to_string(matches.size()) + "/" + std::to_string(entries.size());
        frame += COLOR_RESET;
        frame += "\r\n\x1b[2K";
        frame += COLOR_PROMPT;
        frame += "> ";
        frame += COLOR_RESET;
        frame += visible_query;
        frame += "\x1b[" + std::to_string(2 + displayWidth(before_cursor) + 1) + "G";
        write(frame);
    }

    /// The part of an entry to show in `width` columns. Like skim, when the matches do not fit, it is shifted
    /// after a `..` marker (the second result is true then): to the end of the entry if the matches are still
    /// visible, otherwise just enough to show them.
    std::pair<std::string_view, bool> visibleRow(std::string_view display, size_t width) const
    {
        const UInt8 * begin = toBytes(display);
        const UInt8 * end = begin + display.size();
        size_t matches_begin = display.size();
        size_t matches_end = 0;
        for (const auto & searcher : searchers)
        {
            const UInt8 * found = searcher.search(begin, end);
            if (found == end)
                continue;
            matches_begin = std::min<size_t>(matches_begin, found - begin);
            matches_end = std::max<size_t>(matches_end, searcher.matchEnd(found, end) - begin);
        }

        if (width <= 2 || displayWidth(display.substr(0, matches_end)) <= width)
            return {truncateToWidth(display, width), false};

        std::string_view tail = tailOfWidth(display, width - 2);
        size_t start = display.size() - tail.size();
        if (start > matches_begin)
            start = matches_end - tailOfWidth(display.substr(0, matches_end), width - 2).size();
        return {truncateToWidth(display.substr(start), width - 2), true};
    }

    /// From the prompt (the last line of the area) to the start of the area.
    std::string moveToTop() const
    {
        return height > 1 ? "\r\x1b[" + std::to_string(height - 1) + "A" : "\r";
    }

    /// The area is below the input (scrolling the screen if needed), the cursor stays on its last line, the prompt.
    std::string reserveArea() const
    {
        std::string frame;
        for (size_t i = 0; i < height; ++i)
            frame += "\r\n";
        return frame;
    }

    /// Erases the area and returns the cursor to the last line of the input.
    std::string clearArea() const
    {
        return moveToTop() + "\x1b[J\x1b[A";
    }

    void write(std::string_view data)
    {
        out.write(data.data(), data.size());
        out.next();
    }

    /// Returns false on EOF.
    bool readByte(char & c)
    {
        return in.read(c);
    }

    void appendHighlighted(std::string & frame, std::string_view text, std::string_view color, std::string_view color_end) const
    {
        std::vector<bool> highlighted(text.size());
        const UInt8 * text_begin = toBytes(text);
        const UInt8 * text_end = text_begin + text.size();
        for (const auto & searcher : searchers)
        {
            for (const UInt8 * found = searcher.search(text_begin, text_end); found != text_end;
                 found = searcher.search(found + UTF8::seqLength(*found), text_end))
            {
                std::fill(highlighted.begin() + (found - text_begin), highlighted.begin() + (searcher.matchEnd(found, text_end) - text_begin), true);
            }
        }

        for (size_t begin = 0; begin < text.size();)
        {
            size_t end = begin;
            while (end < text.size() && highlighted[end] == highlighted[begin])
                ++end;
            if (highlighted[begin])
                frame += color;
            frame += text.substr(begin, end - begin);
            if (highlighted[begin])
                frame += color_end;
            begin = end;
        }
    }

    /// Returns false on EOF.
    bool readPaste()
    {
        std::string buf;
        char c = 0;
        while (readByte(c))
        {
            buf += c;
            if (buf.ends_with(PASTE_END))
            {
                buf.resize(buf.size() - PASTE_END.size());
                query.insert(cursor, buf);
                cursor += buf.size();
                return true;
            }
        }
        return false;
    }

    enum class Escape : uint8_t
    {
        Abort,
        Up,
        Down,
        PageUp,
        PageDown,
        Left,
        Right,
        Home,
        End,
        Delete,
        QueryChanged,
        Ignored,
        Eof,
    };

    static Escape arrowKey(char c)
    {
        switch (c)
        {
            case 'A': return Escape::Up;
            case 'B': return Escape::Down;
            case 'C': return Escape::Right;
            case 'D': return Escape::Left;
            case 'H': return Escape::Home;
            case 'F': return Escape::End;
            default: return Escape::Ignored;
        }
    }

    void eraseBeforeCursor(size_t from)
    {
        query.erase(from, cursor - from);
        cursor = from;
    }

    Escape readEscape()
    {
        if (!in.poll(25'000))
            return Escape::Abort;
        char c = 0;
        if (!readByte(c))
            return Escape::Eof;
        if (c == 'O') /// SS3, sent by the cursor keys in the application mode of the terminal.
        {
            if (!readByte(c))
                return Escape::Eof;
            return arrowKey(c);
        }
        if (c != '[') /// CSI
            return Escape::Ignored;

        std::string params;
        while (true)
        {
            if (!readByte(c))
                return Escape::Eof;
            /// ECMA-48: parameter bytes `0`..`?`, then intermediate bytes ` `..`/`, then the final byte.
            if (c >= '0' && c <= '?')
                params += c;
            else if (c < ' ' || c > '/')
                break;
        }
        if (c != '~')
            return params.empty() ? arrowKey(c) : Escape::Ignored;
        if (params == "1" || params == "7")
            return Escape::Home;
        if (params == "4" || params == "8")
            return Escape::End;
        if (params == "3")
            return Escape::Delete;
        if (params == "5")
            return Escape::PageUp;
        if (params == "6")
            return Escape::PageDown;
        if (params == "200")
            return readPaste() ? Escape::QueryChanged : Escape::Eof;
        return Escape::Ignored;
    }

    std::string loop()
    {
        refilter();
        while (true)
        {
            redraw();
            /// Wake up periodically to follow the terminal size.
            while (!in.poll(100'000))
            {
                if (handleResize())
                    redraw();
            }

            ssize_t page = static_cast<ssize_t>(std::max<size_t>(list_rows, 1));
            char c = 0;
            if (!readByte(c))
                return {};

            std::string old_query = query;
            switch (c)
            {
                case '\r':
                    return matches.empty() ? query : history[entries[matches[selected]].index];
                case CTRL('C'):
                case CTRL('G'):
                    return {};
                case CTRL('D'):
                    if (query.empty())
                        return {};
                    query.erase(cursor, nextCodePoint(query, cursor) - cursor);
                    break;
                case CERASE:
                case CTRL('H'):
                    eraseBeforeCursor(previousCodePoint(query, cursor));
                    break;
                case CTRL('U'):
                    eraseBeforeCursor(0);
                    break;
                case CTRL('W'):
                {
                    size_t from = cursor;
                    while (from > 0 && isWhitespaceASCII(query[from - 1]))
                        --from;
                    while (from > 0 && !isWhitespaceASCII(query[from - 1]))
                        --from;
                    eraseBeforeCursor(from);
                    break;
                }
                case CTRL('A'):
                    cursor = 0;
                    break;
                case CTRL('E'):
                    cursor = query.size();
                    break;
                case CTRL('B'):
                    cursor = previousCodePoint(query, cursor);
                    break;
                case CTRL('F'):
                    cursor = nextCodePoint(query, cursor);
                    break;
                case CTRL('P'):
                case CTRL('K'):
                    moveSelection(1);
                    break;
                case CTRL('N'):
                case CTRL('J'): /// Enter is '\r' in raw mode.
                    moveSelection(-1);
                    break;
                case CTRL('['): /// ESC
                    switch (readEscape())
                    {
                        case Escape::Abort:
                        case Escape::Eof:
                            return {};
                        /// The list grows upwards, towards older entries.
                        case Escape::Up: moveSelection(1); break;
                        case Escape::Down: moveSelection(-1); break;
                        case Escape::PageUp: moveSelection(page); break;
                        case Escape::PageDown: moveSelection(-page); break;
                        case Escape::Left: cursor = previousCodePoint(query, cursor); break;
                        case Escape::Right: cursor = nextCodePoint(query, cursor); break;
                        case Escape::Home: cursor = 0; break;
                        case Escape::End: cursor = query.size(); break;
                        case Escape::Delete: query.erase(cursor, nextCodePoint(query, cursor) - cursor); break;
                        case Escape::QueryChanged:
                        case Escape::Ignored:
                            break;
                    }
                    break;
                default:
                    if (!isControlASCII(c))
                        query.insert(cursor++, 1, c);
                    break;
            }

            if (query != old_query)
                refilter();
        }
    }
};

}

std::string pickFromHistory(int in_fd, int out_fd, const std::string & initial_query, const std::vector<std::string> & history)
{
    return Picker(in_fd, out_fd, initial_query, history).run();
}

}
