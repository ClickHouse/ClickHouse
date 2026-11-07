#include <Client/ReplxxLineReader.h>
#include <Common/UTF8Helpers.h>

#include <algorithm>
#include <string_view>
#include <vector>

namespace DB
{
using Replxx = replxx::Replxx;

/// `replxx` measures cursor positions in code points while its state text is UTF-8, so the vim
/// handlers work on decoded code points and the text is converted at the `replxx` boundary.
/// The text comes from `replxx`, which keeps it as code points internally, so it is valid UTF-8.
static std::u32string decodeUTF8(const char * text)
{
    std::u32string result;
    const auto * s = reinterpret_cast<const unsigned char *>(text);
    while (*s)
    {
        char32_t c = *s;
        int extra = 0;
        if (c >= 0xF0)
        {
            c &= 0x07;
            extra = 3;
        }
        else if (c >= 0xE0)
        {
            c &= 0x0F;
            extra = 2;
        }
        else if (c >= 0xC0)
        {
            c &= 0x1F;
            extra = 1;
        }
        ++s;
        for (; extra > 0 && (*s & 0xC0) == 0x80; --extra, ++s)
            c = (c << 6) | (*s & 0x3F);
        result.push_back(c);
    }
    return result;
}

static std::string encodeUTF8(const std::u32string & text)
{
    std::string result;
    result.reserve(text.size());
    for (char32_t c : text)
    {
        if (c < 0x80)
            result.push_back(static_cast<char>(c));
        else if (c < 0x800)
        {
            result.push_back(static_cast<char>(0xC0 | (c >> 6)));
            result.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        }
        else if (c < 0x10000)
        {
            result.push_back(static_cast<char>(0xE0 | (c >> 12)));
            result.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        }
        else
        {
            result.push_back(static_cast<char>(0xF0 | (c >> 18)));
            result.push_back(static_cast<char>(0x80 | ((c >> 12) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        }
    }
    return result;
}

void ReplxxLineReader::withVimState(const std::function<void(int &, std::u32string &)> & func)
{
    auto state = rx.get_state();
    int pos = state.cursor_position();
    std::u32string text = decodeUTF8(state.text());

    func(pos, text);

    rx.set_state(Replxx::State(encodeUTF8(text).c_str(), pos));
}

void ReplxxLineReader::resetVimForNewInput()
{
    resetVim();
    rx.set_editing_mode(MODE_INSERT);
}

int ReplxxLineReader::vimReps() const
{
    uint64_t reps = std::max<uint64_t>(vimbuffer, 1) * std::max<uint64_t>(vimbufferinner, 1);
    return static_cast<int>(std::min<uint64_t>(reps, 1000000));
}

void ReplxxLineReader::fixTrailingNewline(int * pos, std::u32string * text)
{
    if (pos)
    {
        int length = static_cast<int>(text->length());
        /* In normal mode the cursor is on a character, not after the end of a nonempty line, also
         * when that line is followed by the empty last line. Only a cursor that is already after the
         * trailing newline stays on the empty last line. */
        if (*pos > 0 && *pos == length && (*text)[*pos - 1] != '\n')
            --*pos;
        else if (*pos > 0 && *pos < length && (*text)[*pos] == '\n' && (*text)[*pos - 1] != '\n')
            --*pos;
    }
}

void ReplxxLineReader::resetVim(int * pos, std::u32string * text)
{
    vimbuffer = vimbufferinner = flag = op = 0;
    inclusivity_flip = 0;
    find_direction = 0;
    if (pos)
    {
        fixTrailingNewline(pos, text);
        if (text)
            recomputeCurswant(*pos, *text);
    }
}

void ReplxxLineReader::find(std::u32string & text, int & pos, char direction, char32_t c, bool repeat)
{
    int oldpos = pos;
    int length = static_cast<int>(text.length());
    bool forward = direction == 'f' || direction == 't';

    int found = pos >= length || text[pos] == '\n' ? -1 : pos;
    /* When repeating `t` or `T`, the cursor is next to the previous match: skip it, like vim does. */
    if (repeat && found >= 0)
    {
        if (direction == 't' && pos + 1 < length && text[pos + 1] == c)
            found = pos + 1;
        else if (direction == 'T' && pos > 0 && text[pos - 1] == c)
            found = pos - 1;
    }
    for (int reps = vimReps(); reps > 0 && found >= 0; reps--)
    {
        int i = forward ? found + 1 : found - 1;
        for (found = -1; (forward ? i < length : i >= 0) && text[i] != '\n'; i += forward ? 1 : -1)
        {
            if (text[i] == c)
            {
                found = i;
                break;
            }
        }
    }
    if (found >= 0)
    {
        pos = found;
        if (direction == 't')
            pos--;
        if (direction == 'T')
            pos++;
    }
    /* `f` and `t` are inclusive, `F` and `T` are exclusive. The range is not empty even when the
     * cursor stays put, e.g. `dt1` right before a `1` deletes the character under the cursor. */
    if (op && found >= 0)
    {
        if (forward)
        {
            text.erase(oldpos, pos - oldpos + 1 - inclusivity_flip);
            pos = oldpos;
        }
        else
        {
            text.erase(pos, oldpos - pos + inclusivity_flip);
        }
    }
    if (op == OPERATOR_C && found >= 0)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
    {
        rx.set_editing_mode(MODE_NORMAL);
        resetVim(&pos, &text);
    }
}

static int iskeyword(char32_t c)
{
    return ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_' || (192 <= c);
}

static int iswhitespace(char32_t c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

/* Special case if we are at the last word of a line and we are using an operator. */
static int wordForward(const std::u32string & text, int pos, int reps, bool bigword, bool op_pending)
{
    int length = static_cast<int>(text.length());
    int limit = op_pending ? length : length - 1;
    for (int rep = 0; rep < reps; rep++)
    {
        int prev = pos;
        bool stop_at_eol = op_pending && rep == reps - 1;
        int break_on_char = 0;
        for (; pos < limit; pos++)
        {
            if (stop_at_eol && pos > prev && text[pos] == '\n')
                break;
            /* An empty line is a word of its own. */
            if (pos > prev && text[pos] == '\n' && text[pos - 1] == '\n')
                break;
            if (bigword)
            {
                if (break_on_char && !iswhitespace(text[pos]))
                    break;

                if (iswhitespace(text[pos]))
                    break_on_char = 1;
            }
            else
            {
                if (break_on_char & 1 && iskeyword(text[pos]))
                    break;
                if (break_on_char & 2 && !iskeyword(text[pos]) && !iswhitespace(text[pos]))
                    break;

                if (iswhitespace(text[pos]))
                    break_on_char = 3;
                else if (iskeyword(text[pos]))
                    break_on_char |= 2;
                else
                    break_on_char |= 1;
            }
        }
        if (pos == prev)
            break;
    }
    return pos;
}

static bool atWordEnd(const std::u32string & text, int pos, bool bigword)
{
    int length = static_cast<int>(text.length());
    if (pos < 0 || pos >= length || iswhitespace(text[pos]))
        return false;
    if (pos + 1 >= length)
        return true;
    char32_t next = text[pos + 1];
    if (bigword)
        return iswhitespace(next);
    if (iskeyword(text[pos]))
        return !iskeyword(next);
    return iswhitespace(next) || iskeyword(next);
}

static int wordEndForward(const std::u32string & text, int pos, int reps, bool bigword, bool stop)
{
    int length = static_cast<int>(text.length());
    for (int rep = 0; rep < reps; rep++)
    {
        if (stop)
        {
            stop = false;
            if (atWordEnd(text, pos, bigword))
                continue;
        }
        int prev = pos;
        if (pos < length - 1)
            pos++;
        int break_on_char = 0;
        for (; pos < length - 1; pos++)
        {
            if (bigword)
            {
                if (!iswhitespace(text[pos]))
                    break_on_char = 1;

                if (break_on_char && iswhitespace(text[pos + 1]))
                    break;
            }
            else
            {
                if (iskeyword(text[pos]))
                    break_on_char = 1;
                else if (!iswhitespace(text[pos]))
                    break_on_char = 2;

                if (break_on_char & 1 && !iskeyword(text[pos + 1]))
                    break;
                if (break_on_char & 2 && (iswhitespace(text[pos + 1]) || iskeyword(text[pos + 1])))
                    break;
            }
        }
        if (pos == prev)
            break;
    }
    return pos;
}

static int wordBackward(const std::u32string & text, int pos, int reps, bool bigword)
{
    for (int rep = 0; rep < reps; rep++)
    {
        int prev = pos;
        if (pos > 0)
            pos--;
        int break_on_char = 0;
        for (; pos > 0; pos--)
        {
            /* An empty line is a word of its own. */
            if (text[pos] == '\n' && text[pos - 1] == '\n')
                break;
            if (bigword)
            {
                if (!iswhitespace(text[pos]))
                    break_on_char = 1;

                if (break_on_char && iswhitespace(text[pos - 1]))
                    break;
            }
            else
            {
                if (iskeyword(text[pos]))
                    break_on_char = 1;
                else if (!iswhitespace(text[pos]))
                    break_on_char = 2;

                if (break_on_char & 1 && !iskeyword(text[pos - 1]))
                    break;
                if (break_on_char & 2 && (iswhitespace(text[pos - 1]) || iskeyword(text[pos - 1])))
                    break;
            }
        }
        if (pos == prev)
            break;
    }
    return pos;
}

/* The start of the line with the given 1-based number, or of the last line if there are fewer lines. */
static int lineStart(const std::u32string & text, int line)
{
    int length = static_cast<int>(text.length());
    int start = 0;
    for (int pos = 0; line > 1 && pos < length; pos++)
    {
        if (text[pos] == '\n')
        {
            line--;
            start = pos + 1;
        }
    }
    return start;
}

/* The first non-blank character of the line that starts at `line_start`, or the end of the line. */
static int firstNonBlank(const std::u32string & text, int line_start)
{
    int length = static_cast<int>(text.length());
    int pos = line_start;
    for (; pos < length && (text[pos] == ' ' || text[pos] == '\t'); pos++)
        ;
    return pos;
}

static int charClass(const std::u32string & text, int i, bool bigword)
{
    char32_t c = text[i];
    if (c == '\n')
        return -1;
    if (iswhitespace(c))
        return 0;
    if (bigword || iskeyword(c))
        return 1;
    return 2;
}

/* Like `charClass`, but for the extra chunks of a counted word object, which continue
 * across lines: a line break is whitespace, and an empty line is a word of its own. */
static int chunkClass(const std::u32string & text, int i, bool bigword)
{
    if (text[i] == '\n')
        return i > 0 && text[i - 1] == '\n' ? 3 : 0;
    return charClass(text, i, bigword);
}

/* Extend the position over the same character class. */
static int extendChunk(const std::u32string & text, int & end, bool bigword)
{
    int length = static_cast<int>(text.length());
    if (end + 1 >= length)
        return -1;
    int cls = chunkClass(text, end + 1, bigword);
    end++;
    if (cls == 3)
        return cls;
    /* Like vim, whitespace that starts within a line ends at the end of that line: the line break
     * after it is not part of the chunk, so an edit does not join the following line. */
    bool within_line = cls == 0 && text[end] != '\n';
    while (end + 1 < length
           && (within_line ? charClass(text, end + 1, bigword) == 0 : chunkClass(text, end + 1, bigword) == cls))
        end++;
    return cls;
}

/* For every position, whether it belongs to a quoted string literal or identifier. A quote inside
 * a comment, e.g. the apostrophe in `-- it's`, does not start a literal: the brackets after it stay
 * visible. The brackets inside comments are not quoted, like vim, which does not know about comments. */
static std::vector<bool> quotedPositions(const std::u32string & text)
{
    size_t length = text.length();
    std::vector<bool> quoted(length);
    char32_t quote = 0;
    for (size_t i = 0; i < length; ++i)
    {
        char32_t c = text[i];
        char32_t next = i + 1 < length ? text[i + 1] : 0;
        if (quote)
        {
            quoted[i] = true;
            if (c == '\\' && i + 1 < length)
                quoted[++i] = true;
            else if (c == quote)
                quote = 0;
        }
        else if (c == '\'' || c == '"' || c == '`')
        {
            quoted[i] = true;
            quote = c;
        }
        else if ((c == '-' && next == '-') || (c == '/' && next == '/') || (c == '#' && (next == ' ' || next == '!')))
        {
            /* A line comment, up to the end of the line, with the same rules as the `Lexer`. */
            while (i + 1 < length && text[i + 1] != '\n')
                ++i;
        }
        else if (c == '/' && next == '*')
        {
            /* A block comment, up to its end. Like in the `Lexer`, block comments nest. */
            size_t nesting_level = 1;
            for (i += 2; i + 1 < length; ++i)
            {
                if (text[i] == '/' && text[i + 1] == '*')
                {
                    ++nesting_level;
                    ++i;
                }
                else if (text[i] == '*' && text[i + 1] == '/')
                {
                    ++i;
                    if (--nesting_level == 0)
                        break;
                }
            }
        }
    }
    return quoted;
}

/* Like vim, brackets inside quotes are ignored, unless the cursor is inside quotes itself. */
static std::vector<bool> ignoredBrackets(const std::u32string & text, int pos)
{
    std::vector<bool> quoted = quotedPositions(text);
    if (pos >= 0 && pos < static_cast<int>(text.length()) && quoted[pos])
        return std::vector<bool>(text.length());
    return quoted;
}

static bool findEnclosingPair(
    const std::u32string & text,
    const std::vector<bool> & ignored,
    int from_open,
    int from_close,
    char32_t open_char,
    char32_t close_char,
    int & open,
    int & close)
{
    int length = static_cast<int>(text.length());
    int depth = 0;
    open = -1;
    for (int i = std::min(from_open, length - 1); i >= 0; i--)
    {
        if (ignored[i])
            continue;
        if (text[i] == close_char)
            depth++;
        else if (text[i] == open_char)
        {
            if (depth == 0)
            {
                open = i;
                break;
            }
            depth--;
        }
    }
    if (open < 0)
        return false;
    depth = 0;
    close = -1;
    for (int i = from_close; i < length; i++)
    {
        if (ignored[i])
            continue;
        if (text[i] == open_char)
            depth++;
        else if (text[i] == close_char)
        {
            if (depth == 0)
            {
                close = i;
                break;
            }
            depth--;
        }
    }
    return close >= 0;
}

static bool findBracketPair(
    const std::u32string & text,
    const std::vector<bool> & ignored,
    int pos,
    char32_t open_char,
    char32_t close_char,
    int & open,
    int & close)
{
    int length = static_cast<int>(text.length());

    bool on_bracket = pos < length && !ignored[pos];
    int from_open = on_bracket && text[pos] == close_char ? pos - 1 : pos;
    int from_close = on_bracket && text[pos] == open_char ? pos + 1 : pos;
    if (findEnclosingPair(text, ignored, from_open, from_close, open_char, close_char, open, close))
        return true;

    if (open >= 0)
        return false;

    for (int i = pos; i < length; i++)
    {
        if (ignored[i])
            continue;
        if (text[i] == close_char)
            return false;
        if (text[i] == open_char)
        {
            open = i;
            int depth = 0;
            for (int j = i + 1; j < length; j++)
            {
                if (ignored[j])
                    continue;
                if (text[j] == open_char)
                    depth++;
                else if (text[j] == close_char)
                {
                    if (depth == 0)
                    {
                        close = j;
                        return true;
                    }
                    depth--;
                }
            }
            return false;
        }
    }
    return false;
}

/* A character typed as an argument of a command, e.g. after `f` or `r`, as opposed to a special key. */
static bool isVimCharacter(char32_t c)
{
    return (c >= 32 || c == '\t') && c != 127 && c < Replxx::KEY::BASE;
}

template <typename T>
Replxx::key_press_handler_t ReplxxLineReader::vimHandler(T && f)
{
    using F = std::decay_t<T>;
    return [this, func = std::forward<T>(f)](char32_t c)
    {
        if constexpr (std::is_invocable_v<F &, int &, std::u32string &, char32_t>)
        {
            withVimState([&](int & pos, std::u32string & text) { func(pos, text, c); });
        }
        else if constexpr (std::is_invocable_v<F &, char32_t>)
        {
            (void)this;
            func(c);
        }
        return Replxx::ACTION_RESULT::CONTINUE;
    };
}

template <typename T>
void ReplxxLineReader::bindKey(char32_t key, T && f, int mode)
{
    Replxx::key_press_handler_t handler = vimHandler(std::forward<T>(f));
    /* After `di` or `da` only a text object can complete the command: any other key cancels it
     * instead of running as an ordinary motion, e.g. `di$` must not act like `d$`. */
    if (mode == MODE_NORMAL && !std::u32string_view(U"wWb()[]{}<>").contains(key))
    {
        handler = [this, inner = std::move(handler)](char32_t c)
        {
            if (flag)
            {
                resetVim();
                return Replxx::ACTION_RESULT::CONTINUE;
            }
            return inner(c);
        };
    }
    rx.bind_key(key, std::move(handler), mode);
}

template <typename T>
void ReplxxLineReader::bindDefaultKey(T && f, int mode)
{
    rx.bind_default_key(vimHandler(std::forward<T>(f)), mode);
}

namespace
{

/* The number of terminal columns that replxx uses to display a character of the input. */
int displayWidth(char32_t c)
{
    /* replxx displays a tab as four spaces and other control characters as two, like `^A`. */
    if (c == '\t')
        return 4;
    if (c < ' ' || (c >= 0x7F && c <= 0x9F))
        return 2;
    char utf8[4];
    size_t size = DB::UTF8::convertCodePointToUTF8(static_cast<int>(c), utf8, sizeof(utf8));
    return static_cast<int>(DB::UTF8::computeWidth(reinterpret_cast<const UInt8 *>(utf8), size));
}

/* The position on the line starting at `line_start` of the character displayed at `column`,
 * or of the last character of the line if the line is shorter. */
int displayColumnToPos(const std::u32string & text, int line_start, int column)
{
    int length = static_cast<int>(text.length());
    int line_column = 0;
    int i = line_start;
    for (; i < length && text[i] != '\n'; i++)
    {
        line_column += displayWidth(text[i]);
        if (line_column > column || i + 1 == length || text[i + 1] == '\n')
            break;
    }
    return i;
}

}

void ReplxxLineReader::recomputeCurswant(int pos, std::u32string & text)
{
    /* Like vim, `j` and `k` keep the display column rather than the character offset, so tabs and
     * wide characters are accounted for. The column is relative to the input text: continuation
     * lines are not indented by the prompt width. The buffer can be empty and the cursor can be at
     * its end: never index `text` past its last character. */
    int length = static_cast<int>(text.length());
    int line_start = std::min(pos, length);
    while (line_start > 0 && text[line_start - 1] != '\n')
        --line_start;
    curswant = 0;
    for (int i = line_start; i < std::min(pos, length); i++)
        curswant += displayWidth(text[i]);
}

void ReplxxLineReader::vimWordMotion(int & pos, std::u32string & text, char motion)
{
    if (flag)
    {
        if (motion == 'w' || motion == 'W')
            vimWordObject(pos, text, motion == 'W');
        else if (motion == 'b')
            vimBracketObject(pos, text, '(', ')');
        else
            resetVim();
        return;
    }

    bool bigword = motion == 'W' || motion == 'E' || motion == 'B';
    char kind = bigword ? motion - 'A' + 'a' : motion;
    int oldpos = pos;
    int reps = vimReps();

    /* Special case, cw on a blank line acts like ce (dw acts normally though). */
    if (kind == 'w' && op == OPERATOR_C && pos < static_cast<int>(text.length()) && !iswhitespace(text[pos]))
    {
        pos = wordEndForward(text, pos, reps, bigword, true);
        text.erase(oldpos, pos - oldpos + 1 - inclusivity_flip);
        pos = oldpos;
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
        return;
    }

    /* On an empty line `cw` keeps the line break, like vim: the exclusive motion that ends in the
     * first column of the next line ends at the end of the empty line instead. */
    if (kind == 'w' && op == OPERATOR_C && (pos == 0 || text[pos - 1] == '\n')
        && (pos == static_cast<int>(text.length()) || text[pos] == '\n'))
    {
        pos = wordForward(text, pos, reps, bigword, true);
        if (pos > oldpos && text[pos - 1] == '\n')
            pos--;
        text.erase(oldpos, pos - oldpos);
        pos = oldpos;
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
        return;
    }

    if (kind == 'w')
        pos = wordForward(text, pos, reps, bigword, op != 0);
    else if (kind == 'e')
        pos = wordEndForward(text, pos, reps, bigword, false);
    else
        pos = wordBackward(text, pos, reps, bigword);

    /* `e` is inclusive: it selects the character under the cursor even when the cursor stays put,
     * e.g. on the last character of the input. */
    bool selected = pos != oldpos || (kind == 'e' && oldpos < static_cast<int>(text.length()));
    /* Like vim, `ce` on an empty last line, or in an empty buffer, enters insert mode. */
    if (kind == 'e' && op == OPERATOR_C && !selected && oldpos == static_cast<int>(text.length())
        && (oldpos == 0 || text[oldpos - 1] == '\n'))
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
        return;
    }
    if (op)
    {
        if (kind == 'w' && !inclusivity_flip && pos > oldpos && text[pos - 1] == '\n')
        {
            /* Like vim, an exclusive motion that ends in the first column of a line ends at the end
             * of the previous line instead, so the line break stays. When the motion also starts at
             * or before the first non-blank of its line, it becomes linewise. */
            int line_start = oldpos;
            for (; line_start > 0 && text[line_start - 1] != '\n'; line_start--)
                ;
            if (std::all_of(text.begin() + line_start, text.begin() + oldpos, [](char32_t c) { return c == ' ' || c == '\t'; }))
            {
                text.erase(line_start, pos - line_start - (op == OPERATOR_C));
                pos = op == OPERATOR_C ? line_start : firstNonBlank(text, line_start);
            }
            else
            {
                text.erase(oldpos, pos - 1 - oldpos);
                pos = oldpos;
            }
        }
        else if (pos > oldpos || (kind == 'e' && selected))
        {
            int count = pos - oldpos;
            int length = static_cast<int>(text.length());
            if (kind == 'e')
                count += 1 - inclusivity_flip;
            else if ((pos == length || text[pos] == '\n') && text[pos - 1] != '\n')
                /* Like vim, `w` that stops at the end of a non-empty line or of the input is inclusive
                 * on the last character, so `v` makes it exclusive, e.g. `dvw` on the last character
                 * deletes nothing. */
                count -= inclusivity_flip;
            else
                count += inclusivity_flip;
            if (count > 0)
                text.erase(oldpos, count);
            pos = oldpos;
        }
        else if (pos < oldpos)
        {
            /* Like vim, an exclusive motion that ends in the first column of a line ends at the end
             * of the previous line instead, so the line break stays. When the motion also starts at
             * or before the first non-blank of its line, it becomes linewise. */
            if (!inclusivity_flip && text[oldpos - 1] == '\n')
            {
                int line_start = pos;
                for (; line_start > 0 && text[line_start - 1] != '\n'; line_start--)
                    ;
                if (std::all_of(text.begin() + line_start, text.begin() + pos, [](char32_t c) { return c == ' ' || c == '\t'; }))
                {
                    pos = line_start;
                    text.erase(pos, oldpos - pos - (op == OPERATOR_C));
                }
                else
                    text.erase(pos, oldpos - 1 - pos);
            }
            else
                text.erase(pos, oldpos - pos + inclusivity_flip);
        }
    }
    if (op == OPERATOR_C && selected)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
        resetVim(&pos, &text);
}

void ReplxxLineReader::vimWordObject(int & pos, std::u32string & text, bool bigword)
{
    int length = static_cast<int>(text.length());
    bool around = flag == FLAG_AROUND;
    int reps = vimReps();

    int cls = pos < length ? charClass(text, pos, bigword) : -1;
    /* For `aw` an empty line is whitespace, as in Vim. */
    bool empty_line = (pos == 0 || text[pos - 1] == '\n') && (pos == length || text[pos] == '\n');
    if (cls == -1 && around && pos < length && empty_line)
        cls = 0;
    /* With a count, an empty line is a chunk of its own, and the selection continues with
     * the following chunks, as in Vim. */
    if (cls == -1 && empty_line && !around && reps > 1 && pos < length)
        cls = 3;
    /* `ciw` on an empty line, including an empty last line, keeps the line break and enters insert mode, as in Vim. */
    if (cls == -1 && empty_line && op == OPERATOR_C)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
        return;
    }
    if (cls == -1)
    {
        resetVim(&pos, &text);
        return;
    }

    int start = pos;
    int end = pos;
    while (start > 0 && charClass(text, start - 1, bigword) == cls)
        start--;
    while (end + 1 < length && charClass(text, end + 1, bigword) == cls)
        end++;

    if (around)
    {
        /* On whitespace aw takes the whitespace and the following word. On a word it
         * takes the trailing whitespace, or the leading whitespace if there is none. */
        if (cls == 0)
        {
            /* The whitespace continues across line breaks, up to the following word or empty line. */
            while (end + 1 < length && chunkClass(text, end + 1, bigword) == 0)
                end++;
            /* Like vim, the operation is canceled when no word follows the whitespace. */
            if (extendChunk(text, end, bigword) == -1)
            {
                resetVim(&pos, &text);
                return;
            }
        }
        else
        {
            /* The trailing whitespace stays within the line, as in Vim. */
            while (end + 1 < length && charClass(text, end + 1, bigword) == 0)
                end++;
        }
        for (int rep = 1; rep < reps; rep++)
        {
            /* Like for `iw`, the line break at the end of a non-empty line is not a chunk of its own. */
            if (end + 2 < length && text[end + 1] == '\n' && text[end] != '\n')
                end++;
            int chunk = extendChunk(text, end, bigword);
            /* Like vim, the operation is canceled when there are fewer chunks than requested. */
            if (chunk == -1)
            {
                resetVim(&pos, &text);
                return;
            }
            if (chunk == 0 || chunk == 3)
            {
                /* Whitespace or an empty line is taken together with the following word, and
                 * the whitespace continues across line breaks. */
                while (end + 1 < length && chunkClass(text, end + 1, bigword) == 0)
                    end++;
                if (extendChunk(text, end, bigword) == -1)
                {
                    resetVim(&pos, &text);
                    return;
                }
            }
            else if (end + 1 < length && charClass(text, end + 1, bigword) == 0)
                extendChunk(text, end, bigword);
        }
        /* Like vim, a selection that starts on a word and does not end with whitespace takes the
         * whitespace before the word instead. */
        if (cls != 0 && charClass(text, end, bigword) > 0)
            while (start > 0 && charClass(text, start - 1, bigword) == 0)
                start--;
    }
    else
    {
        /* Each extra count takes one more chunk, alternating word and whitespace. Like vim, the line
         * break at the end of a non-empty line is not a chunk of its own: the next chunk starts on the
         * following line. */
        for (int rep = 1; rep < reps; rep++)
        {
            if (end + 2 < length && text[end + 1] == '\n' && text[end] != '\n')
                end++;
            /* Like vim, the operation is canceled when there are fewer chunks than requested. */
            if (extendChunk(text, end, bigword) == -1)
            {
                resetVim(&pos, &text);
                return;
            }
        }
        /* An empty line at the end of the selection is included as a position, not as its line
         * break: like vim, the selection joins it with the previous line, and its line break stays. */
        if (end > start && text[end] == '\n' && text[end - 1] == '\n')
            end--;
    }

    /* Like vim, `v` after the operator makes the inclusive text object exclusive, e.g. `dviw`. */
    if (inclusivity_flip)
        end--;
    if (end >= start)
        text.erase(start, end - start + 1);
    pos = start;
    if (op == OPERATOR_C)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
        resetVim(&pos, &text);
}

void ReplxxLineReader::vimBracketObject(int & pos, std::u32string & text, char32_t open_char, char32_t close_char)
{
    if (!op || !flag)
    {
        resetVim();
        return;
    }

    bool around = flag == FLAG_AROUND;
    int reps = vimReps();

    int open = -1;
    int close = -1;

    std::vector<bool> ignored = ignoredBrackets(text, pos);
    bool found = findBracketPair(text, ignored, pos, open_char, close_char, open, close);
    for (int rep = 1; rep < reps && found; rep++)
        found = findEnclosingPair(text, ignored, open - 1, close + 1, open_char, close_char, open, close);
    if (!found)
    {
        resetVim(&pos, &text);
        return;
    }

    int start = around ? open : open + 1;
    int count = around ? close - open + 1 : close - open - 1;
    bool linewise = false;
    /* Like vim, `v` after the operator makes the inclusive text object exclusive, e.g. `dvi(`. */
    if (inclusivity_flip)
        count--;
    else if (!around)
    {
        /* Like vim, the inner object of a multiline block starts on the next line when the opening
         * bracket ends its line, and ends at the end of the previous line when only blanks precede
         * the closing bracket on its line, so `ci(` keeps the brackets on their own lines. When both
         * hold, the object is linewise: `di(` deletes the lines between the brackets, and `ci(`
         * replaces them with one empty line. */
        bool after_line_break = start < close && text[start] == '\n';
        if (after_line_break)
            start++;
        int close_line = close;
        for (; close_line > 0 && text[close_line - 1] != '\n'; close_line--)
            ;
        bool close_in_indent = close_line > open
            && std::all_of(text.begin() + close_line, text.begin() + close, [](char32_t c) { return c == ' ' || c == '\t'; });
        if (!close_in_indent)
            count = close - start;
        else if (after_line_break && start < close_line)
        {
            linewise = true;
            count = close_line - start - (op == OPERATOR_C);
        }
        else
            count = close_line - 1 - start;
    }
    if (count > 0)
        text.erase(start, count);
    pos = linewise && op != OPERATOR_C ? firstNonBlank(text, start) : start;
    if (op == OPERATOR_C)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
        resetVim(&pos, &text);
}

void ReplxxLineReader::vimCharwise(int & pos, std::u32string & text, int oldpos, bool allow_empty)
{
    /* Exclusive characterwise range between the old and the new position, e.g. `dh`, `cl`.
     * An empty range means that the motion failed, unless `allow_empty` is set: like vim,
     * `cl` on an empty line still enters insert mode. */
    int start = std::min(oldpos, pos);
    int count = std::abs(pos - oldpos);
    if (op && count > 0)
    {
        if (inclusivity_flip)
            count = std::min(count + 1, static_cast<int>(text.length()) - start);
        text.erase(start, count);
        pos = start;
    }
    if (op == OPERATOR_C && (count > 0 || allow_empty))
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
        resetVim(&pos, &text);
}

void ReplxxLineReader::vimLinewise(int & pos, std::u32string & text, int oldpos)
{
    /* Like vim, `v` after the operator forces a linewise motion to be characterwise exclusive, e.g. `dvj`.
     * Like for an empty exclusive range, `c` still enters insert mode. */
    if (op && inclusivity_flip)
    {
        inclusivity_flip = 0;
        int start = std::min(oldpos, pos);
        int end = std::max(oldpos, pos);
        if (end <= start || text[end - 1] != '\n')
        {
            vimCharwise(pos, text, oldpos, /* allow_empty = */ true);
            return;
        }
        /* Like vim, an exclusive motion that ends in the first column of a line ends at the end
         * of the previous line instead, e.g. `dvG`. It becomes linewise, through the previous
         * line, if it starts at or before the first non-blank of its line. */
        int start_line = start;
        for (; start_line > 0 && text[start_line - 1] != '\n'; start_line--)
            ;
        pos = start;
        oldpos = end - 1;
        if (start > firstNonBlank(text, start_line))
        {
            vimCharwise(pos, text, oldpos, /* allow_empty = */ true);
            return;
        }
    }
    if (op)
    {
        /* Linewise: from the start of the first line of the range
         * through the end of the last one. */
        int length = static_cast<int>(text.length());
        int start = std::min(oldpos, pos);
        int end = std::max(oldpos, pos);
        for (; start > 0 && text[start - 1] != '\n'; start--)
            ;
        for (; end < length && text[end] != '\n'; end++)
            ;
        if (op == OPERATOR_C)
            text.erase(start, end - start);
        else if (end < length)
            text.erase(start, end - start + 1);
        else
        {
            /* The range reaches the end of the input: also remove the preceding newline, and
             * place the cursor at the start of the line that is now the last one, like vim does. */
            if (start > 0)
                start--;
            text.erase(start, end - start);
            for (; start > 0 && text[start - 1] != '\n'; start--)
                ;
        }
        /* Like vim, a linewise delete leaves the cursor on the first non-blank of the line. */
        pos = op == OPERATOR_C ? start : firstNonBlank(text, start);
    }
    if (op == OPERATOR_C)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
    {
        rx.set_editing_mode(MODE_NORMAL);
        resetVim(&pos, &text);
    }
}

void ReplxxLineReader::vimForcedDoubledOperator(int & pos, std::u32string & text)
{
    /* Like vim, `v` forces a doubled operator, e.g. `dvd` or `cvc`, to be characterwise exclusive:
     * from the cursor to the first non-blank of the last line of the count. */
    int oldpos = pos;
    int length = static_cast<int>(text.length());
    int line_start = pos;
    for (; line_start > 0 && text[line_start - 1] != '\n'; line_start--)
        ;
    int reps = vimReps();
    for (int rep = 1; rep < reps; rep++)
    {
        int end = line_start;
        for (; end < length && text[end] != '\n'; end++)
            ;
        if (end >= length)
        {
            /* Like vim, a count fails on the last line, and stops there otherwise. */
            if (rep == 1)
            {
                resetVim(&pos, &text);
                return;
            }
            break;
        }
        line_start = end + 1;
    }
    pos = firstNonBlank(text, line_start);
    /* On a line of blanks, the last blank, like vim. */
    if (pos > line_start && (pos == length || text[pos] == '\n'))
        pos--;
    vimLinewise(pos, text, oldpos);
}

void ReplxxLineReader::vimChangeLines(int & pos, std::u32string & text)
{
    /* Replace count lines, starting from the current one, with an empty line. Like vim, a count
     * fails on the last line, and stops there otherwise. */
    if (vimReps() > 1 && std::find(text.begin() + pos, text.end(), U'\n') == text.end())
    {
        resetVim(&pos, &text);
        return;
    }
    int length = static_cast<int>(text.length());
    for (; pos > 0 && text[pos - 1] != '\n'; pos--)
        ;
    int end = pos;
    for (int reps = vimReps(); reps > 0 && end < length; reps--)
    {
        for (; end < length && text[end] != '\n'; end++)
            ;
        if (reps > 1 && end < length)
            end++;
    }
    text.erase(pos, end - pos);

    rx.set_editing_mode(MODE_INSERT);
    resetVim();
}

void ReplxxLineReader::vimDollar(int & pos, std::u32string & text)
{
    int oldpos = pos;
    int length = static_cast<int>(text.length());
    /* A count moves to the end of a following line, like vim: `2$` is the end of the next line.
     * A count past the last line stops there, and any count fails on the last line. */
    int reps = vimReps();
    for (int rep = 1; rep < reps; rep++)
    {
        int end = pos;
        for (; end < length && text[end] != '\n'; end++)
            ;
        if (end >= length)
        {
            if (rep == 1)
            {
                resetVim();
                return;
            }
            break;
        }
        pos = end + 1;
    }
    for (; pos < length - 1 && text[pos] != '\n' && text[pos + 1] != '\n'; pos++)
        ;
    if (op)
    {
        /* Like vim, `d` over several lines becomes linewise when only blanks precede the cursor. */
        if (op == OPERATOR_D && std::find(text.begin() + oldpos, text.begin() + pos, U'\n') != text.begin() + pos)
        {
            int line_start = oldpos;
            for (; line_start > 0 && text[line_start - 1] != '\n'; line_start--)
                ;
            if (std::all_of(text.begin() + line_start, text.begin() + oldpos, [](char32_t c) { return c == ' ' || c == '\t'; }))
            {
                vimLinewise(pos, text, oldpos);
                return;
            }
        }
        /* The end is inclusive, but an empty line has no character to include. */
        int count = pos - oldpos + (pos < length && text[pos] != '\n') - inclusivity_flip;
        text.erase(oldpos, std::max(count, 0));
        pos = oldpos;
    }
    if (op == OPERATOR_C)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
    {
        bool motion = !op;
        resetVim(&pos, &text);
        /* Like vim, a plain `$` makes the following `j` and `k` go to the end of the line, whatever its length. */
        if (motion)
            curswant = CURSWANT_LINE_END;
    }
}

void ReplxxLineReader::setupVimKeybindings(bool allow_editor)
{
    for (int i = 0; i < MODE_END; i++)
    {
        if (i == MODE_INSERT)
        {
            bindKey(
                Replxx::KEY::ESCAPE,
                [this](int & pos, std::u32string & text, char32_t)
                {
                    if (pos > 0 && text[pos - 1] != '\n')
                        pos--;
                    resetVim(&pos, &text);
                    rx.set_editing_mode(MODE_NORMAL);
                },
                i);
        }
        else
        {
            bindKey(
                Replxx::KEY::ESCAPE,
                [this](char32_t)
                {
                    resetVim();
                    rx.set_editing_mode(MODE_NORMAL);
                },
                i);
        }

        rx.bind_key(
            Replxx::KEY::control('J'),
            [this](char32_t code)
            {
                /* A pending command must not leak into the next input. */
                resetVimForNewInput();
                return rx.invoke(Replxx::ACTION::COMMIT_LINE, code);
            },
            i);

        /* `replxx` dispatches only through the keymap of the current mode, so a paste outside of
         * insert mode would otherwise be interpreted as commands. Insert it as text instead. */
        if (i != MODE_INSERT)
        {
            rx.bind_key(
                Replxx::KEY::PASTE_START,
                [this](char32_t code)
                {
                    resetVim();
                    rx.set_editing_mode(MODE_NORMAL);
                    suppress_hints_once = true;
                    Replxx::ACTION_RESULT ret = rx.invoke(Replxx::ACTION::BRACKETED_PASTE, code);
                    withVimState([this](int & pos, std::u32string & text) { resetVim(&pos, &text); });
                    suppressHintsForDisplayedLine();
                    return ret;
                },
                i);
        }

        /* Go through `historyNavigate`, so the recalled line does not pop hints. In insert mode
         * the recalled line stays editable with the cursor at its end; in normal mode the cursor
         * is normalized to the last character. */
        auto history_navigate = [this, i](Replxx::ACTION action, char32_t code)
        {
            resetVim();
            Replxx::ACTION_RESULT ret = historyNavigate(action, code);
            if (i != MODE_INSERT)
            {
                rx.set_editing_mode(MODE_NORMAL);
                withVimState([this](int & pos, std::u32string & text) { resetVim(&pos, &text); });
            }
            return ret;
        };
        rx.bind_key(
            Replxx::KEY::control('P'),
            [history_navigate](char32_t code) { return history_navigate(Replxx::ACTION::HISTORY_PREVIOUS, code); },
            i);
        rx.bind_key(
            Replxx::KEY::control('N'),
            [history_navigate](char32_t code) { return history_navigate(Replxx::ACTION::HISTORY_NEXT, code); },
            i);
        rx.bind_key(
            Replxx::KEY::control('C'),
            [this](char32_t code)
            {
                resetVim();
                rx.set_editing_mode(MODE_INSERT);
                Replxx::ACTION_RESULT ret = rx.invoke(Replxx::ACTION::ABORT_LINE, code);
                withVimState([this](int & pos, std::u32string & text) { resetVim(&pos, &text); });
                return ret;
            },
            i);
    }

    bindKey(
        Replxx::KEY::BACKSPACE,
        [this](int & pos, std::u32string & text, char32_t)
        {
            /* Like `h`, but wraps to the previous line. */
            int oldpos = pos;
            int reps = vimReps();
            for (int rep = 0; rep < reps && pos > 0; rep++)
            {
                pos--;
                /* Like vim, with an operator a wrap lands after the last character of the previous
                 * line, so the line break is one step of the motion. Without an operator the wrap
                 * lands on the last character, so the line break is not a step. */
                if (text[pos] == '\n' && !op)
                    rep--;
            }
            vimCharwise(pos, text, oldpos);
        },
        MODE_NORMAL);

    bindKey(
        Replxx::KEY::ENTER,
        [this](int &pos, std::u32string & text, char32_t)
        {
            pos++;
            text.insert(pos - 1, 1, '\n');
            resetVim(&pos, &text);
        },
        MODE_INSERT);

    bindKey(
        Replxx::KEY::ENTER,
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            for (int rep = 0; rep < reps; rep++)
            {
                /* To the first non-blank of the next line. A count past the last line stops
                 * there, like vim, and the cursor never leaves the buffer. */
                int end = pos;
                for (; end < length && text[end] != '\n'; end++)
                    ;
                if (end >= length)
                    break;
                for (pos = end + 1; pos < length - 1 && text[pos] != '\n' && iswhitespace(text[pos]); pos++)
                    ;
            }
            if (op)
            {
                /* Linewise, like `j`: there is nothing to operate on without a next line. */
                if (std::find(text.begin() + oldpos, text.begin() + pos, U'\n') != text.begin() + pos)
                    vimLinewise(pos, text, oldpos);
                else
                {
                    pos = oldpos;
                    resetVim(&pos, &text);
                }
                return;
            }
            resetVim(&pos, &text);
        },
        MODE_NORMAL);

    bindKey(
        '~',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            /* Toggle the case of up to count characters until the end of the line, and move past
             * them, but not past the last character of the line. */
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            int end = pos;
            for (; end - pos < reps && end < length && text[end] != '\n'; end++)
            {
                char32_t c = text[end];
                if ('a' <= c && c <= 'z')
                    text[end] = c + 'A' - 'a';
                else if ('A' <= c && c <= 'Z')
                    text[end] = c + 'a' - 'A';
            }
            if (end > pos)
                pos = end < length && text[end] != '\n' ? end : end - 1;
            resetVim(&pos, &text);
        },
        MODE_NORMAL);

    bindKey(
        'i',
        [this](char32_t)
        {
            if (op)
            {
                if (!flag)
                    flag = FLAG_INSIDE;
                else
                    resetVim();
            }
            else
                rx.set_editing_mode(MODE_INSERT);
        },
        MODE_NORMAL);

    bindKey(
        'I',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            int length = static_cast<int>(text.length());
            for (; pos > 0 && text[pos - 1] != '\n'; pos--)
                ;
            for (; pos < length - 1 && iswhitespace(text[pos]) && text[pos] != '\n'; pos++)
                ;
            rx.set_editing_mode(MODE_INSERT);
        },
        MODE_NORMAL);

    bindKey(
        'a',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                if (!flag)
                    flag = FLAG_AROUND;
                else
                    resetVim();
            }
            else
            {
                if (pos < static_cast<int>(text.length()) && text[pos] != '\n')
                    pos++;
                rx.set_editing_mode(MODE_INSERT);
            }
        },
        MODE_NORMAL);

    bindKey(
        'A',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            int length = static_cast<int>(text.length());
            for (; pos < length && text[pos] != '\n'; pos++)
                ;
            rx.set_editing_mode(MODE_INSERT);
        },
        MODE_NORMAL);

    bindKey(
        'v',
        [this, allow_editor](char32_t)
        {
            if (!op)
            {
                /* The embedded client must not run an editor on the server. */
                if (!allow_editor)
                {
                    resetVim();
                    return;
                }
                openEditor(false);
                /* Remove the trailing newline. */
                withVimState(
                    [this](int & pos, std::u32string & text)
                    {
                        int length = static_cast<int>(text.length());
                        if (length > 0 && text[length - 1] == '\n')
                            text.erase(length - 1, 1);

                        pos = std::max(static_cast<int>(text.length()) - 1, 0);
                        resetVim(&pos, &text);
                    });
            }
            else
            {
                inclusivity_flip = 1;
            }
        },
        MODE_NORMAL);

    bindKey(
        'h',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int reps = vimReps();
            for (int rep = 0; rep < reps && pos > 0 && text[pos - 1] != '\n'; rep++)
                pos--;
            vimCharwise(pos, text, oldpos);
        },
        MODE_NORMAL);

    bindKey(
        'l',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            if (op && inclusivity_flip)
            {
                /* Like vim, `l` stops on the last character of the line. When it stops short of the
                 * count, the motion is inclusive, and `v` makes it exclusive, e.g. `dvl` on the last
                 * character deletes nothing. Otherwise `v` makes it inclusive. */
                int line_end = pos;
                for (; line_end < length && text[line_end] != '\n'; line_end++)
                    ;
                int rep = 0;
                for (; rep < reps && pos + 1 < line_end; rep++)
                    pos++;
                if (rep < reps)
                    inclusivity_flip = 0;
                vimCharwise(pos, text, oldpos, /* allow_empty = */ true);
                return;
            }
            /* With an operator, `l` may select up to the end of the line, e.g. `dl` on the last character. */
            int limit = op ? length : length - 1;
            for (int rep = 0; rep < reps && pos < limit && text[pos] != '\n' && (op || text[pos + 1] != '\n'); rep++)
                pos++;
            /* With an operator, `l` does not fail at the end of the line: it selects nothing. */
            vimCharwise(pos, text, oldpos, /* allow_empty = */ true);
        },
        MODE_NORMAL);

    bindKey(
        'j',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int length = static_cast<int>(text.length());

            int reps = vimReps();
            for (int rep = 0; rep < reps; rep++)
            {
                int next_newline = pos;
                for (; next_newline < length && text[next_newline] != '\n'; next_newline++)
                    ;
                if (next_newline >= length)
                    break;
                pos = displayColumnToPos(text, next_newline + 1, curswant);
            }
            if (op)
            {
                if (pos != oldpos)
                    vimLinewise(pos, text, oldpos);
                else
                    resetVim(&pos, &text);
                return;
            }
            /* Only clear the counts: recomputing curswant here would lose the
             * sought-for column that j/k keep. */
            resetVim();
        },
        MODE_NORMAL);

    bindKey(
        'k',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            for (int rep = 0; rep < reps && pos > 0; rep++)
            {
                int line_start = std::min(pos, length);
                for (; line_start > 0 && text[line_start - 1] != '\n'; line_start--)
                    ;
                if (line_start == 0)
                    break;
                int prev_line_start = line_start - 1;
                for (; prev_line_start > 0 && text[prev_line_start - 1] != '\n'; prev_line_start--)
                    ;
                pos = displayColumnToPos(text, prev_line_start, curswant);
            }
            if (op)
            {
                /* Compare lines rather than positions: `k` can stay on the same line. */
                int first_line_end = 0;
                for (; first_line_end < length && text[first_line_end] != '\n'; first_line_end++)
                    ;
                if (pos != oldpos && oldpos > first_line_end)
                    vimLinewise(pos, text, oldpos);
                else
                    resetVim(&pos, &text);
                return;
            }
            /* Only clear the counts: recomputing curswant here would lose the
             * sought-for column that j/k keep. */
            resetVim();
        },
        MODE_NORMAL);

    bindKey(
        '0',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            if (!op && vimbuffer)
                vimbuffer = std::min<uint64_t>(10 * vimbuffer, 1000000);
            else if (op && vimbufferinner)
                vimbufferinner = std::min<uint64_t>(10 * vimbufferinner, 1000000);
            else
            {
                for (; pos > 0 && text[pos - 1] != '\n'; pos--)
                    ;
                if (op)
                {
                    /* `v` makes `0` inclusive, but an empty line has no character to include. */
                    bool on_character = oldpos < static_cast<int>(text.length()) && text[oldpos] != '\n';
                    text.erase(pos, oldpos - pos + (inclusivity_flip && on_character));
                }
                if (op == OPERATOR_C)
                {
                    rx.set_editing_mode(MODE_INSERT);
                    resetVim();
                }
                else
                    resetVim(&pos, &text);
            }
        },
        MODE_NORMAL);

    for (char c = '1'; c <= '9'; c++)
    {
        bindKey(
            c,
            [this](char32_t code)
            {
                if (op)
                {
                    vimbufferinner = 10 * vimbufferinner + (code - '0');
                }
                else
                {
                    vimbuffer = 10 * vimbuffer + (code - '0');
                }
            },
            MODE_NORMAL);
    }

    bindKey('$', [this](int & pos, std::u32string & text, char32_t) { vimDollar(pos, text); }, MODE_NORMAL);

    bindKey(
        '%',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int length = static_cast<int>(text.length());
            /* Like vim, use the first bracket from the cursor to the end of the line. The operator
             * range still starts at the original cursor position. Angle brackets are not in vim's
             * default `matchpairs`, and in SQL they are comparison operators. */
            for (; pos < length && text[pos] != '\n' && !std::u32string_view(U"(){}[]").contains(text[pos]); pos++)
                ;
            if (pos >= length || text[pos] == '\n')
            {
                pos = oldpos;
                resetVim(&pos, &text);
                return;
            }
            char32_t paren = text[pos];
            char32_t matching = 0;
            bool opening = true;
            switch (paren)
            {
                case '(': matching = ')'; break;
                case ')':
                    opening = false;
                    matching = '(';
                    break;
                case '{': matching = '}'; break;
                case '}':
                    opening = false;
                    matching = '{';
                    break;
                case '[': matching = ']'; break;
                case ']':
                    opening = false;
                    matching = '[';
                    break;
                default:
                    /* Consume the count and any pending operator. */
                    resetVim();
                    return;
            }

            /* The quote context is that of the original cursor, not of the bracket found ahead. */
            std::vector<bool> ignored = ignoredBrackets(text, oldpos);
            int paren_count = 1;
            int found = -1;
            for (int i = opening ? pos + 1 : pos - 1; opening ? i < length : i >= 0; i += opening ? 1 : -1)
            {
                if (ignored[i])
                    continue;
                char32_t c = text[i];
                if (c == paren)
                    paren_count++;
                else if (c == matching)
                    paren_count--;

                if (paren_count == 0)
                {
                    found = i;
                    break;
                }
            }
            /* Without a matching bracket the command fails and the cursor stays. */
            pos = found >= 0 ? found : oldpos;
            bool moved = pos != oldpos;
            if (op)
            {
                if (pos > oldpos)
                {
                    text.erase(oldpos, pos - oldpos + 1 - inclusivity_flip);
                    pos = oldpos;
                }
                else if (pos < oldpos)
                {
                    text.erase(pos, oldpos - pos + 1 - inclusivity_flip);
                }
            }
            if (op == OPERATOR_C && moved)
            {
                rx.set_editing_mode(MODE_INSERT);
                resetVim();
            }
            else
                resetVim(&pos, &text);
        },
        MODE_NORMAL);

    bindKey(
        'f',
        [this](char32_t)
        {
            find_direction = 'f';
            rx.set_editing_mode(MODE_FIND);
        },
        MODE_NORMAL);
    bindKey(
        'F',
        [this](char32_t)
        {
            find_direction = 'F';
            rx.set_editing_mode(MODE_FIND);
        },
        MODE_NORMAL);
    bindKey(
        't',
        [this](char32_t)
        {
            find_direction = 't';
            rx.set_editing_mode(MODE_FIND);
        },
        MODE_NORMAL);
    bindKey(
        'T',
        [this](char32_t)
        {
            find_direction = 'T';
            rx.set_editing_mode(MODE_FIND);
        },
        MODE_NORMAL);

    bindKey(
        ';',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (strchr("fFtT", last_find_direction) && last_find_char)
                find(text, pos, last_find_direction, last_find_char, /* repeat= */ true);
            else
                /* Nothing to repeat: the command fails, and a pending operator is canceled. */
                resetVim();
        },
        MODE_NORMAL);

    /* The character after `f`, `F`, `t`, `T` or `r` can be any code point, not only ASCII, so these
     * modes take every key without its own binding. A special key cancels the command. */
    bindDefaultKey(
        [this](int & pos, std::u32string & text, char32_t c)
        {
            if (!isVimCharacter(c))
            {
                rx.set_editing_mode(MODE_NORMAL);
                resetVim(&pos, &text);
                return;
            }
            last_find_char = c;
            last_find_direction = find_direction;
            find(text, pos, find_direction, c, /* repeat= */ false);
        },
        MODE_FIND);

    bindDefaultKey(
        [this](int & pos, std::u32string & text, char32_t c)
        {
            if (isVimCharacter(c))
            {
                int length = static_cast<int>(text.length());
                int reps = vimReps();
                int count = 0;
                while (count < reps && pos + count < length && text[pos + count] != '\n')
                    count++;

                if (count == reps)
                {
                    for (int i = 0; i < reps; i++)
                        text[pos + i] = c;
                    pos += reps - 1;
                }
            }
            rx.set_editing_mode(MODE_NORMAL);
            resetVim(&pos, &text);
        },
        MODE_REPLACE);

    bindDefaultKey(
        [this](char32_t)
        {
            rx.set_editing_mode(MODE_NORMAL);
            resetVim();
        },
        MODE_G);

    /* Like vim, a key without a binding cancels the pending operator, count and text object,
     * so that a later motion does not complete a stale command. */
    bindDefaultKey([this](char32_t) { resetVim(); }, MODE_NORMAL);

    for (char motion : {'w', 'e', 'b', 'W', 'E', 'B'})
    {
        bindKey(motion, [this, motion](int & pos, std::u32string & text, char32_t) { vimWordMotion(pos, text, motion); }, MODE_NORMAL);
    }

    /* Both brackets of a pair name the same text object, e.g. `di(` and `di)`. */
    for (const char * pair : {"()", "[]", "{}", "<>"})
    {
        for (int k = 0; k < 2; k++)
        {
            bindKey(
                pair[k],
                [this, open_char = static_cast<char32_t>(pair[0]), close_char = static_cast<char32_t>(pair[1])](int & pos, std::u32string & text, char32_t)
                { vimBracketObject(pos, text, open_char, close_char); },
                MODE_NORMAL);
        }
    }

    bindKey(
        'G',
        [this](int & pos, std::u32string & text, char32_t)
        {
            int oldpos = pos;
            int length = static_cast<int>(text.length());
            if (vimbuffer || vimbufferinner)
            {
                /* A count is an absolute line number, not a repeat. */
                pos = lineStart(text, vimReps());
            }
            else
                for (pos = length; pos > 0 && text[pos - 1] != '\n'; pos--)
                    ;
            pos = firstNonBlank(text, pos);
            vimLinewise(pos, text, oldpos);
        },
        MODE_NORMAL);


    bindKey('g', [this](char32_t) { rx.set_editing_mode(MODE_G); }, MODE_NORMAL);

    bindKey(
        'g',
        [this](int & pos, std::u32string & text, char32_t)
        {
            /* Leave the `g` prefix mode on every path, also when a forced characterwise delete
             * returns early. A change selects insert mode afterwards. */
            rx.set_editing_mode(MODE_NORMAL);
            int oldpos = pos;
            /* A count is an absolute line number, not a repeat. */
            pos = vimbuffer || vimbufferinner ? lineStart(text, vimReps()) : 0;
            pos = firstNonBlank(text, pos);
            vimLinewise(pos, text, oldpos);
        },
        MODE_G);


    bindKey(
        'd',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (!op)
            {
                op = OPERATOR_D;
            }
            else if (op == OPERATOR_D && inclusivity_flip)
                vimForcedDoubledOperator(pos, text);
            else if (op == OPERATOR_D)
            {
                int reps = vimReps();
                /* Like vim, a count fails on the last line, and stops there otherwise. */
                if (reps > 1 && std::find(text.begin() + pos, text.end(), U'\n') == text.end())
                {
                    resetVim(&pos, &text);
                    return;
                }
                for (int rep = 0; rep < reps; rep++)
                {
                    int length = static_cast<int>(text.length());
                    for (; pos > 0 && text[pos - 1] != '\n'; pos--)
                        ;
                    int end = pos;
                    for (; end < length && text[end] != '\n'; end++)
                        ;
                    if (end < length)
                        text.erase(pos, end - pos + 1);
                    else
                    {
                        /* The last line: also remove the preceding newline, and place the
                         * cursor at the start of the line that is now the last one. */
                        if (pos > 0)
                            pos--;
                        text.erase(pos, end - pos);
                        for (; pos > 0 && text[pos - 1] != '\n'; pos--)
                            ;
                        break;
                    }
                }
                /* Like vim, leave the cursor on the first non-blank of the line. */
                pos = firstNonBlank(text, pos);

                resetVim(&pos, &text);
            }
            else
                resetVim();
        },
        MODE_NORMAL);

    bindKey(
        'c',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (!op)
            {
                op = OPERATOR_C;
            }
            else if (op == OPERATOR_C && inclusivity_flip)
                vimForcedDoubledOperator(pos, text);
            else if (op == OPERATOR_C)
                vimChangeLines(pos, text);
            else
                resetVim();
        },
        MODE_NORMAL);


    bindKey(
        's',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            int count = 0;
            while (count < reps && pos + count < length && text[pos + count] != '\n')
                count++;
            if (count > 0)
                text.erase(pos, count);
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        },
        MODE_NORMAL);

    bindKey(
        'r',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            /* Keep the count the replacement character handler will reset it. */
            if (pos < static_cast<int>(text.length()) && text[pos] != '\n')
                rx.set_editing_mode(MODE_REPLACE);
            else
                resetVim();
        },
        MODE_NORMAL);

    bindKey(
        'x',
        [this](int & pos, std::u32string & text, char32_t)
        {
            /* Not a motion: cancel a pending operator, like vim does for e.g. `dx`. */
            if (op)
            {
                resetVim();
                return;
            }
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            int count = 0;
            while (count < reps && pos + count < length && text[pos + count] != '\n')
                count++;
            /* The cursor at the end of the input is moved onto the last character of a nonempty last line,
             * but stays on an empty last line. */
            if (count > 0)
                text.erase(pos, count);
            resetVim(&pos, &text);
        },
        MODE_NORMAL);

    bindKey(
        'D',
        [this](int & pos, std::u32string & text, char32_t)
        {
            /* `D` is `d$` and `C` is `c$`, with the count of the `$` motion. */
            if (op)
            {
                resetVim();
                return;
            }
            op = OPERATOR_D;
            vimDollar(pos, text);
        },
        MODE_NORMAL);

    bindKey(
        'C',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            op = OPERATOR_C;
            vimDollar(pos, text);
        },
        MODE_NORMAL);

    bindKey(
        'S',
        [this](int & pos, std::u32string & text, char32_t)
        {
            /* `S` is `cc`, with its count. */
            if (op)
            {
                resetVim();
                return;
            }
            vimChangeLines(pos, text);
        },
        MODE_NORMAL);

    bindKey(
        'o',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            int length = static_cast<int>(text.length());

            for (; pos < length && text[pos] != '\n'; pos++)
                ;
            text.insert(pos, 1, '\n');
            pos++;
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        },
        MODE_NORMAL);

    bindKey(
        'O',
        [this](int & pos, std::u32string & text, char32_t)
        {
            if (op)
            {
                resetVim();
                return;
            }
            for (; pos > 0 && text[pos - 1] != '\n'; pos--)
                ;
            text.insert(pos, 1, '\n');
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        },
        MODE_NORMAL);
}

}
