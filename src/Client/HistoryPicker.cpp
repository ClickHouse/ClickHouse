#include <Client/HistoryPicker.h>
#include <Common/StringUtils.h>
#include <Common/TerminalSize.h>
#include <Common/UTF8Helpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <base/find_symbols.h>

#include <algorithm>
#include <string_view>
#include <unordered_set>

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
    std::string lower;
};

/// Everything is shown on a single line, as skim did.
void replaceLineBreaks(std::string & s)
{
    std::replace_if(s.begin(), s.end(), [](char c) { return c == '\n' || c == '\r' || c == '\t'; }, ' ');
}

size_t displayWidth(std::string_view s)
{
    return UTF8::computeWidth(reinterpret_cast<const UInt8 *>(s.data()), s.size());
}

std::string_view truncateToWidth(std::string_view s, size_t limit)
{
    return s.substr(0, UTF8::computeBytesBeforeWidth(reinterpret_cast<const UInt8 *>(s.data()), s.size(), 0, limit));
}

void popCodePoint(std::string & s)
{
    while (!s.empty() && UTF8::isContinuationOctet(static_cast<UInt8>(s.back())))
        s.pop_back();
    if (!s.empty())
        s.pop_back();
}

class Picker
{
public:
    Picker(int in_fd_, int out_fd_, const std::string & initial_query, const std::vector<std::string> & history_)
        : in(in_fd_, /* buf_size */ 1), out(out_fd_), history(history_), query(initial_query)
    {
        auto [terminal_cols, terminal_rows] = getTerminalSize(in_fd_, out_fd_);
        if (terminal_cols && terminal_rows)
        {
            rows = terminal_rows;
            cols = terminal_cols;
        }
        height = std::min(rows, std::max<size_t>(4, rows * 30 / 100));
        list_rows = height > 2 ? height - 2 : 0;
        replaceLineBreaks(query);

        std::unordered_set<std::string_view> seen;
        entries.reserve(history.size());
        for (size_t i = history.size(); i-- > 0;)
        {
            if (!seen.insert(history[i]).second)
                continue;
            Entry entry{i, history[i], {}};
            replaceLineBreaks(entry.display);
            entry.lower = toLowerCopyASCII(entry.display);
            entries.push_back(std::move(entry));
        }
        matches.reserve(entries.size());
    }

    std::string run()
    {
        /// Reserve the area below the input (scrolling the screen if needed); the cursor stays on its last line, the prompt.
        std::string frame;
        for (size_t i = 0; i < height; ++i)
            frame += "\r\n";
        write(frame);

        std::string result;
        try
        {
            result = loop();
        }
        catch (...)
        {
            write(moveToTop() + "\x1b[J\x1b[A");
            throw;
        }
        write(moveToTop() + "\x1b[J\x1b[A");
        out.finalize();
        return result;
    }

private:
    /// Unbuffered input: bytes typed after the picker closes belong to replxx.
    ReadBufferFromFileDescriptor in;
    WriteBufferFromFileDescriptor out;
    const std::vector<std::string> & history;
    std::string query;

    size_t rows = 24;
    size_t cols = 80;
    size_t height = 0;
    size_t list_rows = 0;

    std::vector<Entry> entries;
    std::vector<size_t> matches; /// Indexes into `entries`.
    std::string lower_query;
    std::vector<std::string_view> words; /// Point into `lower_query`.
    size_t selected = 0;
    size_t offset = 0;

    void refilter()
    {
        lower_query = toLowerCopyASCII(query);
        words.clear();
        splitInto<' ', '\t'>(words, lower_query, /* token_compress */ true);

        matches.clear();
        for (size_t i = 0; i < entries.size(); ++i)
        {
            std::string_view lower = entries[i].lower;
            if (std::all_of(words.begin(), words.end(), [&](std::string_view w) { return lower.find(w) != std::string_view::npos; }))
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
        if (selected < offset)
            offset = selected;
        if (list_rows && selected >= offset + list_rows)
            offset = selected - list_rows + 1;

        /// Leave the last column empty to avoid the terminal's pending-wrap state.
        size_t content_width = cols > 3 ? cols - 3 : 1;

        std::string_view visible_query = query;
        while (displayWidth(visible_query) > content_width)
        {
            size_t len = UTF8::seqLength(static_cast<UInt8>(visible_query.front()));
            visible_query.remove_prefix(std::min(len, visible_query.size()));
        }

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
            std::string_view text = truncateToWidth(entry.display, content_width);
            if (pos == selected)
            {
                frame += COLOR_CURRENT;
                frame += COLOR_CURSOR;
                frame += ">";
                frame += COLOR_CURRENT_FOREGROUND;
                frame += " ";
                appendHighlighted(frame, text, entry.lower, COLOR_CURRENT_MATCH, COLOR_CURRENT_FOREGROUND);
                /// Erasing with the current background extends it to the whole row.
                frame += "\x1b[K";
                frame += COLOR_RESET;
            }
            else
            {
                frame += "  ";
                appendHighlighted(frame, text, entry.lower, COLOR_MATCHED, COLOR_MATCHED_END);
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
        frame += "\x1b[" + std::to_string(2 + displayWidth(visible_query) + 1) + "G";
        write(frame);
    }

    /// From the prompt (the last line of the area) to the start of the area.
    std::string moveToTop() const
    {
        return height > 1 ? "\r\x1b[" + std::to_string(height - 1) + "A" : "\r";
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

    /// Lowercasing keeps byte offsets, so the matches found in `lower` apply to `text`.
    void appendHighlighted(std::string & frame, std::string_view text, std::string_view lower, std::string_view color, std::string_view color_end) const
    {
        std::vector<bool> highlighted(text.size());
        for (std::string_view word : words)
        {
            for (size_t found = lower.find(word); found < text.size(); found = lower.find(word, found + 1))
                std::fill_n(highlighted.begin() + found, std::min(word.size(), text.size() - found), true);
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
        char c;
        while (readByte(c))
        {
            buf += c;
            if (buf.ends_with(PASTE_END))
            {
                buf.resize(buf.size() - PASTE_END.size());
                replaceLineBreaks(buf);
                query += buf;
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
        QueryChanged,
        Ignored,
        Eof,
    };

    Escape readEscape()
    {
        if (!in.poll(25'000))
            return Escape::Abort;
        char c;
        if (!readByte(c))
            return Escape::Eof;
        if (c == 'O')
        {
            if (!readByte(c))
                return Escape::Eof;
            return c == 'A' ? Escape::Up : (c == 'B' ? Escape::Down : Escape::Ignored);
        }
        if (c != '[')
            return Escape::Ignored;

        std::string params;
        while (true)
        {
            if (!readByte(c))
                return Escape::Eof;
            if (c >= 0x30 && c <= 0x3F)
                params += c;
            else if (c < 0x20 || c > 0x2F)
                break;
        }
        if (c == 'A' && params.empty())
            return Escape::Up;
        if (c == 'B' && params.empty())
            return Escape::Down;
        if (c == '~' && params == "5")
            return Escape::PageUp;
        if (c == '~' && params == "6")
            return Escape::PageDown;
        if (c == '~' && params == "200")
            return readPaste() ? Escape::QueryChanged : Escape::Eof;
        return Escape::Ignored;
    }

    std::string loop()
    {
        refilter();
        ssize_t page = static_cast<ssize_t>(std::max<size_t>(list_rows, 1));
        while (true)
        {
            redraw();

            char c;
            if (!readByte(c))
                return {};

            std::string old_query = query;
            switch (c)
            {
                case '\r':
                    return matches.empty() ? query : history[entries[matches[selected]].index];
                case 3: /// Ctrl-C
                case 7: /// Ctrl-G
                    return {};
                case 4: /// Ctrl-D
                    if (query.empty())
                        return {};
                    break;
                case 127:
                case 8:
                    popCodePoint(query);
                    break;
                case 21: /// Ctrl-U
                    query.clear();
                    break;
                case 23: /// Ctrl-W
                    while (!query.empty() && isWhitespaceASCII(query.back()))
                        query.pop_back();
                    while (!query.empty() && !isWhitespaceASCII(query.back()))
                        query.pop_back();
                    break;
                case 16: /// Ctrl-P
                case 11: /// Ctrl-K
                    moveSelection(1);
                    break;
                case 14: /// Ctrl-N
                case '\n': /// Ctrl-J; Enter is '\r' in raw mode.
                    moveSelection(-1);
                    break;
                case '\x1b':
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
                        case Escape::QueryChanged:
                        case Escape::Ignored:
                            break;
                    }
                    break;
                default:
                    if (static_cast<UInt8>(c) >= 0x20)
                        query += c;
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
