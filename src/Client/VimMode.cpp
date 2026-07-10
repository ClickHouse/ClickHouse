#include <Client/ReplxxLineReader.h>

#include <algorithm>

namespace DB
{
using Replxx = replxx::Replxx;

int ReplxxLineReader::vimReps() const {
    uint64_t reps = std::max<uint64_t>(vimbuffer, 1) * std::max<uint64_t>(vimbufferinner, 1);
    return static_cast<int>(std::min<uint64_t>(reps, 1000000));
}

void ReplxxLineReader::fixTrailingNewline(int *pos, std::string *text) {
    if (pos) {
        int length = static_cast<int>(text->length());
        if (*pos > 0 && *pos == length && (*text)[*pos - 1] != '\n')
            --*pos;
        if (*pos > 0 && *pos == length - 1 && (*text)[*pos] == '\n' && (*text)[*pos - 1] != '\n')
            ++*pos;
        else if (*pos > 0 && *pos < length && (*text)[*pos] == '\n' && (*text)[*pos - 1] != '\n')
            --*pos;
    }
}

void ReplxxLineReader::resetVim(int *pos, std::string *text) {
    vimbuffer = vimbufferinner = flag = op = 0;
    inclusivity_flip = 0;
    find_direction = 0;
    if (pos) {
        fixTrailingNewline(pos, text);
        if (text)
            recomputeCurswant(*pos, *text);
    }
}

static int iskeyword(unsigned char c) {
    return ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_' || (192 <= c);
}

static int iswhitespace(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r';
}

/* Special case if we are at the last word of a line and we are using an operator. */
static int wordForward(const std::string &text, int pos, int reps, bool bigword, bool op_pending)
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

static bool atWordEnd(const std::string &text, int pos, bool bigword)
{
    int length = static_cast<int>(text.length());
    if (pos < 0 || pos >= length || iswhitespace(text[pos]))
        return false;
    if (pos + 1 >= length)
        return true;
    unsigned char next = text[pos + 1];
    if (bigword)
        return iswhitespace(next);
    if (iskeyword(text[pos]))
        return !iskeyword(next);
    return iswhitespace(next) || iskeyword(next);
}

static int wordEndForward(const std::string &text, int pos, int reps, bool bigword, bool stop)
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

static int wordBackward(const std::string &text, int pos, int reps, bool bigword)
{
    for (int rep = 0; rep < reps; rep++)
    {
        int prev = pos;
        if (pos > 0)
            pos--;
        int break_on_char = 0;
        for (; pos > 0; pos--)
        {
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

template <typename T>
void ReplxxLineReader::bindKey(char32_t key, T && f, int mode) {
    using F = std::decay_t<T>;
    rx.bind_key(key, [this, func = std::forward<T>(f)](char32_t c) {
        if constexpr (std::is_invocable_v<F &, int &, std::string &, char32_t>) {
            auto state = rx.get_state();
            int pos = state.cursor_position();
            std::string text = state.text();

            func(pos, text, c);

            rx.set_state(Replxx::State(text.c_str(), pos));
        }
        else if constexpr (std::is_invocable_v<F &, char32_t>) {
            (void)this;
            func(c);
        }
        return Replxx::ACTION_RESULT::CONTINUE;
    }, mode);
}

void ReplxxLineReader::recomputeCurswant(int pos, std::string &text) {
    int prev_newline;
    int length = static_cast<int>(text.length());
    for (prev_newline = pos; prev_newline > 0; prev_newline--)
        if (prev_newline <= length && text[prev_newline] == '\n')
            break;
    curswant = std::max(text[prev_newline] != '\n' || pos == 0 ? pos + rx.prompt_indentation() + 1 : pos - prev_newline, 1);
}

void ReplxxLineReader::vimWordMotion(int &pos, std::string &text, char motion)
{
    /* Not implemented yet. */
    if (flag)
    {
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

    if (kind == 'w')
        pos = wordForward(text, pos, reps, bigword, op != 0);
    else if (kind == 'e')
        pos = wordEndForward(text, pos, reps, bigword, false);
    else
        pos = wordBackward(text, pos, reps, bigword);

    bool moved = pos != oldpos;
    if (op)
    {
        if (pos > oldpos)
        {
            text.erase(oldpos, pos - oldpos + (kind == 'e' ? 1 - inclusivity_flip : inclusivity_flip));
            pos = oldpos;
        }
        else if (pos < oldpos)
        {
            text.erase(pos, oldpos - pos + inclusivity_flip);
        }
    }
    if (op == OPERATOR_C && moved)
    {
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }
    else
        resetVim(&pos, &text);
}

void ReplxxLineReader::setupVimKeybindings()
{
    for (int i = 0; i < MODE_END; i++) {
        if (i == MODE_INSERT) {
            bindKey(Replxx::KEY::ESCAPE, [this](int &pos, std::string &text, char32_t) {
                if (pos > 0 && text[pos - 1] != '\n')
                    pos--;
                resetVim(&pos, &text);
                rx.set_editing_mode(MODE_NORMAL);
            }, i);
        }
        else {
            bindKey(Replxx::KEY::ESCAPE, [this](char32_t) {
                resetVim();
                rx.set_editing_mode(MODE_NORMAL);
            }, i);
        }
    }

    for (int i = 0; i < MODE_END; i++) {
        rx.bind_key(Replxx::KEY::control('P'), [this](char32_t code) {
            resetVim();
            rx.set_editing_mode(MODE_NORMAL);
            Replxx::ACTION_RESULT ret = rx.invoke(Replxx::ACTION::HISTORY_PREVIOUS, code);
            auto state = rx.get_state();
            int pos = state.cursor_position();
            std::string text = state.text();
            resetVim(&pos, &text);
            rx.set_state(Replxx::State(text.c_str(), pos));
            return ret;
        }, i);
        rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) {
            resetVim();
            rx.set_editing_mode(MODE_NORMAL);
            Replxx::ACTION_RESULT ret = rx.invoke(Replxx::ACTION::HISTORY_NEXT, code);
            auto state = rx.get_state();
            int pos = state.cursor_position();
            std::string text = state.text();
            resetVim(&pos, &text);
            rx.set_state(Replxx::State(text.c_str(), pos));
            return ret;
        }, i);
        rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) {
            resetVim();
            rx.set_editing_mode(MODE_NORMAL);
            Replxx::ACTION_RESULT ret = rx.invoke(Replxx::ACTION::HISTORY_NEXT, code);
            auto state = rx.get_state();
            int pos = state.cursor_position();
            std::string text = state.text();
            resetVim(&pos, &text);
            rx.set_state(Replxx::State(text.c_str(), pos));
            return ret;
        }, i);
        rx.bind_key(Replxx::KEY::control('C'), [this](char32_t code) {
            resetVim();
            rx.set_editing_mode(MODE_INSERT);
            Replxx::ACTION_RESULT ret = rx.invoke(Replxx::ACTION::ABORT_LINE, code);
            auto state = rx.get_state();
            int pos = state.cursor_position();
            std::string text = state.text();
            resetVim(&pos, &text);
            rx.set_state(Replxx::State(text.c_str(), pos));
            return ret;
        }, i);
    }

    rx.bind_key(Replxx::KEY::ENTER, [this](char32_t code) {
        rx.invoke(Replxx::ACTION::COMMIT_LINE, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);

    bindKey(Replxx::KEY::BACKSPACE, [this](int &pos, std::string &text, char32_t) {
        (void)text;
        int reps = vimReps();
        for (int rep = 0; rep < reps && pos > 0; rep++) {
            pos--;
            if (text[pos] == '\n')
                rep--;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('i', [this](char32_t) {
        if (op) {
            if (!flag)
                flag = FLAG_INSIDE;
            else
                resetVim();
        }
        else
            rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('I', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        for (; pos > 0 && text[pos - 1] != '\n'; pos--);
        for (; pos < length - 1 && iswhitespace(text[pos]) && text[pos] != '\n'; pos++);
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('a', [this](int &pos, std::string &text, char32_t) {
        if (op) {
            if (!flag)
                flag = FLAG_AROUND;
            else
                resetVim();
        }
        else {
            if (pos < static_cast<int>(text.length()) && text[pos] != '\n')
                pos++;
            rx.set_editing_mode(MODE_INSERT);
        }
    }, MODE_NORMAL);

    bindKey('A', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        for (; pos < length && text[pos] != '\n'; pos++);
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('v', [this](char32_t) {
        if (!op) {
            openEditor(false);
            /* Remove the trailing newline. */
            auto state = rx.get_state();
            std::string text = state.text();
            int length = static_cast<int>(text.length());
            if (length > 0 && text[length - 1] == '\n')
                text.erase(length - 1, 1);

            int pos = std::max(static_cast<int>(text.length()) - 1, 0);
            resetVim(&pos, &text);

            rx.set_state(Replxx::State(text.c_str(), pos));
        }
        else {
            inclusivity_flip = 1;
        }
    }, MODE_NORMAL);

    bindKey('h', [this](int &pos, std::string &text, char32_t) {
        int reps = vimReps();
        for (int rep = 0; rep < reps && pos > 0 && text[pos - 1] != '\n'; rep++)
            pos--;
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('l', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        int reps = vimReps();
        for (int rep = 0; rep < reps && pos < length - 1 && text[pos] != '\n' && text[pos + 1] != '\n'; rep++)
            pos++;
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('j', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());

        int reps = vimReps();
        for (int rep = 0; rep < reps; rep++) {
            int prev = pos;
            int offset = curswant;

            for (int i = pos; i < length; i++) {
                if (text[i] == '\n') {
                    for (pos = i + 1; pos < length - 1 && pos < i + offset; pos++)
                        if (text[pos] == '\n' || text[pos + 1] == '\n')
                            break;
                    break;
                }
            }
            if (pos == prev)
                break;
        }
        /* Only clear the counts: recomputing curswant here would lose the
         * sought-for column that j/k keep. */
        resetVim();
    }, MODE_NORMAL);

    bindKey('k', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        int reps = vimReps();
        for (int rep = 0; rep < reps && pos > 0; rep++) {
            int prev_newline;
            for (prev_newline = pos - 1; prev_newline > 0; prev_newline--)
                if (text[prev_newline] == '\n')
                    break;

            int offset = curswant;

            if (prev_newline == 0) {
                if (text[0] == '\n')
                    pos = 0;
                break;
            }

            for (int i = prev_newline - 1; i >= 0; i--) {
                if (text[i] == '\n' || i == 0) {
                    int no_newline = text[i] != '\n';
                    if (no_newline)
                        offset = std::max(offset - (rx.prompt_indentation() + 1), 0);
                    for (pos = no_newline ? 0 : i + 1; pos < length - 1 && pos < i + offset; pos++)
                        if (text[pos] == '\n' || text[pos + 1] == '\n')
                            break;
                    break;
                }
            }
        }
        /* Only clear the counts: recomputing curswant here would lose the
         * sought-for column that j/k keep. */
        resetVim();
    }, MODE_NORMAL);

    bindKey('0', [this](int &pos, std::string &text, char32_t) {
        int oldpos = pos;
        if (!op && vimbuffer)
            vimbuffer = std::min<uint64_t>(10 * vimbuffer, 1000000);
        else if (op && vimbufferinner)
            vimbufferinner = std::min<uint64_t>(10 * vimbufferinner, 1000000);
        else {
            for (; pos > 0 && text[pos - 1] != '\n'; pos--);
            if (op) {
                text.erase(pos, oldpos - pos + inclusivity_flip);
            }
            if (op == OPERATOR_C) {
                rx.set_editing_mode(MODE_INSERT);
                resetVim();
            }
            else
                resetVim(&pos, &text);
        }
    }, MODE_NORMAL);

    for (char c = '1'; c <= '9'; c++) {
        bindKey(c, [this](char32_t code) {
            if (op) {
                vimbufferinner = 10 * vimbufferinner + (code - '0');
            }
            else {
                vimbuffer = 10 * vimbuffer + (code - '0');
            }
        }, MODE_NORMAL);
    }

    bindKey('$', [this](int &pos, std::string &text, char32_t) {
        int oldpos = pos;
        for (; pos < static_cast<int>(text.length()) - 1 && text[pos] != '\n' && text[pos + 1] != '\n'; pos++);
        if (op) {
            text.erase(oldpos, pos - oldpos + 1 - inclusivity_flip);
            pos = oldpos;
        }
        if (op == OPERATOR_C) {
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        }
        else
            resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('%', [this](int &pos, std::string &text, char32_t) {
        int oldpos = pos;
        char paren = text[pos];
        char matching;
        bool opening = true;
        switch (paren) {
        case '(':
            matching = ')';
            break;
        case ')':
            opening = false;
            matching = '(';
            break;
        case '{':
            matching = '}';
            break;
        case '}':
            opening = false;
            matching = '{';
            break;
        case '[':
            matching = ']';
            break;
        case ']':
            opening = false;
            matching = '[';
            break;
        case '<':
            matching = '>';
            break;
        case '>':
            opening = false;
            matching = '<';
            break;
        default:
            /* Consume the count and any pending operator. */
            resetVim();
            return;
        }

        int paren_count = 1;
        for (int i = opening ? pos + 1 : pos - 1; opening ? i < static_cast<int>(text.length()) : i >= 0; i += opening ? 1 : -1) {
            char c = text[i];
            if (c == paren)
                paren_count++;
            else if (c == matching)
                paren_count--;

            if (paren_count == 0) {
                pos = i;
                break;
            }
        }
        bool moved = pos != oldpos;
        if (op) {
            if (pos > oldpos) {
                text.erase(oldpos, pos - oldpos + 1 - inclusivity_flip);
                pos = oldpos;
            }
            else if (pos < oldpos) {
                text.erase(pos, oldpos - pos + 1 - inclusivity_flip);
            }
        }
        if (op == OPERATOR_C && moved) {
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        }
        else
            resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('f', [this](char32_t) {
        find_direction = 'f';
        rx.set_editing_mode(MODE_FIND);
    }, MODE_NORMAL);
    bindKey('F', [this](char32_t) {
        find_direction = 'F';
        rx.set_editing_mode(MODE_FIND);
    }, MODE_NORMAL);
    bindKey('t', [this](char32_t) {
        find_direction = 't';
        rx.set_editing_mode(MODE_FIND);
    }, MODE_NORMAL);
    bindKey('T', [this](char32_t) {
        find_direction = 'T';
        rx.set_editing_mode(MODE_FIND);
    }, MODE_NORMAL);

    for (char c = 32; c < 127; c++) {
        bindKey(c, [this, c](int &pos, std::string &text, char32_t) {
            int oldpos = pos;
            int length = static_cast<int>(text.length());
            bool forward = find_direction == 'f' || find_direction == 't';

            int found = text[pos] == '\n' ? -1 : pos;
            for (int reps = vimReps(); reps > 0 && found >= 0; reps--) {
                int i = forward ? found + 1 : found - 1;
                for (found = -1; (forward ? i < length : i >= 0) && text[i] != '\n'; i += forward ? 1 : -1) {
                    if (text[i] == c) {
                        found = i;
                        break;
                    }
                }
            }
            if (found >= 0) {
                pos = found;
                if (find_direction == 't')
                    pos--;
                if (find_direction == 'T')
                    pos++;
            }
            bool moved = pos != oldpos;
            if (op) {
                if (pos > oldpos) {
                    text.erase(oldpos, pos - oldpos + 1 - inclusivity_flip);
                    pos = oldpos;
                }
                if (oldpos > pos) {
                    text.erase(pos, oldpos - pos + inclusivity_flip);
                }
            }
            if (op == OPERATOR_C && moved) {
                rx.set_editing_mode(MODE_INSERT);
                resetVim();
            }
            else {
                rx.set_editing_mode(MODE_NORMAL);
                resetVim(&pos, &text);
            }
        }, MODE_FIND);

        if (c != 'g') {
            bindKey(c, [this](char32_t) {
                rx.set_editing_mode(MODE_NORMAL);
                resetVim();
            }, MODE_G);
        }

        bindKey(c, [this, c](int &pos, std::string &text, char32_t) {
            int length = static_cast<int>(text.length());
            int reps = vimReps();
            int count = 0;
            while (count < reps && pos + count < length && text[pos + count] != '\n')
                count++;

            if (count == reps) {
                for (int i = 0; i < reps; i++)
                    text[pos + i] = c;
                pos += reps - 1;
            }
            rx.set_editing_mode(MODE_NORMAL);
            resetVim(&pos, &text);
        }, MODE_REPLACE);
    }

    for (char motion : {'w', 'e', 'b', 'W', 'E', 'B'})
    {
        bindKey(motion, [this, motion](int &pos, std::string &text, char32_t)
        {
            vimWordMotion(pos, text, motion);
        }, MODE_NORMAL);
    }

    bindKey('G', [this](int &pos, std::string &text, char32_t) {
        int oldpos = pos;
        int length = static_cast<int>(text.length());
        if (vimbuffer || vimbufferinner) {
            /* A count is an absolute line number, not a repeat. */
            int line = vimReps();
            pos = 0;
            for (; line > 1 && pos < length; pos++)
                if (text[pos] == '\n')
                    line--;
        }
        else
            pos = length;
        if (op) {
            /* Linewise: from the start of the first line of the range
             * through the end of the last one. */
            int start = std::min(oldpos, pos);
            int end = std::max(oldpos, pos);
            for (; start > 0 && text[start - 1] != '\n'; start--);
            for (; end < length && text[end] != '\n'; end++);
            if (op == OPERATOR_C)
                text.erase(start, end - start);
            else if (end < length)
                text.erase(start, end - start + 1);
            else {
                if (start > 0)
                    start--;
                text.erase(start, end - start);
            }
            pos = start;
        }
        if (op == OPERATOR_C) {
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        }
        else
            resetVim(&pos, &text);
    }, MODE_NORMAL);


    bindKey('g', [this](char32_t) {
        rx.set_editing_mode(MODE_G);
    }, MODE_NORMAL);

    bindKey('g', [this](int &pos, std::string &text, char32_t) {
        int oldpos = pos;
        int length = static_cast<int>(text.length());
        pos = 0;
        if (vimbuffer || vimbufferinner) {
            /* A count is an absolute line number, not a repeat. */
            int line = vimReps();
            for (; line > 1 && pos < length; pos++)
                if (text[pos] == '\n')
                    line--;
        }
        if (op) {
            /* Linewise: from the start of the first line of the range
             * through the end of the last one. */
            int start = std::min(oldpos, pos);
            int end = std::max(oldpos, pos);
            for (; start > 0 && text[start - 1] != '\n'; start--);
            for (; end < length && text[end] != '\n'; end++);
            if (op == OPERATOR_C)
                text.erase(start, end - start);
            else if (end < length)
                text.erase(start, end - start + 1);
            else {
                if (start > 0)
                    start--;
                text.erase(start, end - start);
            }
            pos = start;
        }
        if (op == OPERATOR_C) {
            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        }
        else {
            rx.set_editing_mode(MODE_NORMAL);
            resetVim(&pos, &text);
        }
    }, MODE_G);




    /* FIXME: Where should the cursor be placed afterwards? */
    bindKey('d', [this](int &pos, std::string &text, char32_t) {
        if (!op) {
            op = OPERATOR_D;
        }
        else if (op == OPERATOR_D) {
            int reps = vimReps();
            for (int rep = 0; rep < reps; rep++) {
                int length = static_cast<int>(text.length());
                for (; pos > 0 && text[pos - 1] != '\n'; pos--);
                int end;
                for (end = pos; end < length && text[end] != '\n'; end++);
                if (end < length)
                    text.erase(pos, end - pos + 1);
                else {
                    if (pos > 0)
                        pos--;
                    text.erase(pos, end - pos);
                    break;
                }
            }

            resetVim(&pos, &text);
        }
        else
            resetVim();
    }, MODE_NORMAL);

    bindKey('c', [this](int &pos, std::string &text, char32_t) {
        if (!op) {
            op = OPERATOR_C;
        }
        else if (op == OPERATOR_C) {
            int length = static_cast<int>(text.length());
            for (; pos > 0 && text[pos - 1] != '\n'; pos--);
            int end = pos;
            for (int reps = vimReps(); reps > 0 && end < length; reps--) {
                for (; end < length && text[end] != '\n'; end++);
                if (reps > 1 && end < length)
                    end++;
            }
            text.erase(pos, end - pos);

            rx.set_editing_mode(MODE_INSERT);
            resetVim();
        }
        else
            resetVim();
    }, MODE_NORMAL);




    bindKey('s', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        int reps = vimReps();
        int count = 0;
        while (count < reps && pos + count < length && text[pos + count] != '\n')
            count++;
        if (count > 0)
            text.erase(pos, count);
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }, MODE_NORMAL);

    bindKey('r', [this](int &pos, std::string &text, char32_t) {
        /* Keep the count the replacement character handler will reset it. */
        if (pos < static_cast<int>(text.length()) && text[pos] != '\n')
            rx.set_editing_mode(MODE_REPLACE);
        else
            resetVim();
    }, MODE_NORMAL);

    bindKey('x', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        int reps = vimReps();
        int count = 0;
        while (count < reps && pos + count < length && text[pos + count] != '\n')
            count++;
        if (count > 0) {
            text.erase(pos, count);
            length = static_cast<int>(text.length());
            if (pos >= length)
                pos = std::max(length - 1, 0);
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('D', [this](int &pos, std::string &text, char32_t) {
        int end;
        int length = static_cast<int>(text.length());
        for (end = pos; end <= length && text[end] != '\n'; end++);
        if (end > pos) {
            text.erase(pos, end - pos);
            if (pos > 0 && text[pos - 1] != '\n')
                pos--;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('C', [this](int &pos, std::string &text, char32_t) {
        int end;
        int length = static_cast<int>(text.length());
        for (end = pos; end <= length && text[end] != '\n'; end++);
        if (end > pos) {
            text.erase(pos, end - pos);
        }
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }, MODE_NORMAL);

    bindKey('S', [this](int &pos, std::string &text, char32_t) {
        int end;
        int length = static_cast<int>(text.length());
        for (; pos > 0 && text[pos - 1] != '\n'; pos--);
        for (end = pos; end <= length && text[end] != '\n'; end++);
        if (end > pos)
            text.erase(pos, end - pos);
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }, MODE_NORMAL);

    bindKey('o', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());

        for (; pos < length && text[pos] != '\n'; pos++);
        text.insert(pos, 1, '\n');
        pos++;
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }, MODE_NORMAL);

    bindKey('O', [this](int &pos, std::string &text, char32_t) {
        for (; pos > 0 && text[pos - 1] != '\n'; pos--);
        text.insert(pos, 1, '\n');
        rx.set_editing_mode(MODE_INSERT);
        resetVim();
    }, MODE_NORMAL);

#if 0
    for (int mode = MODE_INSERT; mode < MODE_END; mode++) {
        rx.bind_key(Replxx::KEY::ESCAPE, [this](char32_t) { vimbuffer = vimbufferinner = 0; rx.set_editing_mode(MODE_NORMAL); return Replxx::ACTION_RESULT::CONTINUE; }, mode);
    }
    rx.bind_key(Replxx::KEY::ESCAPE, [this](char32_t code) { vimbuffer = vimbufferinner = 0; rx.invoke(Replxx::ACTION::MOVE_CURSOR_LEFT, code); rx.set_editing_mode(MODE_NORMAL); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_INSERT);
    rx.bind_key('i', [this](char32_t) { rx.set_editing_mode(MODE_INSERT); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('I', [this](char32_t code) { rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_BEGINING_OF_LINE, code); rx.set_editing_mode(MODE_INSERT); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('a', [this](char32_t code) { rx.invoke(Replxx::ACTION::MOVE_CURSOR_RIGHT, code); rx.set_editing_mode(MODE_INSERT); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('A', [this](char32_t code) { rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_END_OF_LINE, code); rx.set_editing_mode(MODE_INSERT); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('$', [this](char32_t code) { rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_END_OF_LINE, code); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key(Replxx::KEY::ENTER, [this](char32_t code) { rx.invoke(Replxx::ACTION::COMMIT_LINE, code); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);

    rx.bind_key('h', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
             rx.invoke(Replxx::ACTION::MOVE_CURSOR_LEFT, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key(Replxx::KEY::BACKSPACE, [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
             rx.invoke(Replxx::ACTION::MOVE_CURSOR_LEFT, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('j', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
             rx.invoke(Replxx::ACTION::LINE_NEXT, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('k', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
             rx.invoke(Replxx::ACTION::LINE_PREVIOUS, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('l', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
             rx.invoke(Replxx::ACTION::MOVE_CURSOR_RIGHT, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('w', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_NEXT_SUBWORD, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('b', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_PREVIOUS_SUBWORD, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('W', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_NEXT_WORD, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('B', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_PREVIOUS_WORD, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('x', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::DELETE_CHARACTER_UNDER_CURSOR, code);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('r', [this](char32_t) {
        rx.set_editing_mode(MODE_REPLACE);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('s', [this](char32_t code) {
        int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::DELETE_CHARACTER_UNDER_CURSOR, code);
        rx.set_editing_mode(MODE_INSERT);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('o', [this](char32_t code) {
        vimbuffer = 0;
        rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_END_OF_LINE, code);
        rx.invoke(Replxx::ACTION::NEW_LINE, code);
        rx.set_editing_mode(MODE_INSERT);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('O', [this](char32_t code) {
        vimbuffer = 0;
        rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_BEGINING_OF_LINE, code);
        rx.invoke(Replxx::ACTION::NEW_LINE, code);
        rx.invoke(Replxx::ACTION::LINE_PREVIOUS, code);
        rx.set_editing_mode(MODE_INSERT);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('g', [this](char32_t) { rx.set_editing_mode(MODE_g); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('g', [this](char32_t) {
        char32_t line = vimbuffer == 0 ? 1 : vimbuffer;
        vimbuffer = 0;
        rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_LINE, line);
        rx.set_editing_mode(MODE_NORMAL);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_g);
    rx.bind_key('G', [this](char32_t) {
        int32_t line = vimbuffer;
        vimbuffer = 0;
        rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_LINE, line);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
    rx.bind_key('D', [this](char32_t code) { rx.invoke(Replxx::ACTION::KILL_TO_END_OF_LINE, code); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('d', [this](char32_t) { rx.set_editing_mode(MODE_DELETE); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('c', [this](char32_t) { rx.set_editing_mode(MODE_CHANGE); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('f', [this](char32_t) { rx.set_editing_mode(MODE_f); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('F', [this](char32_t) { rx.set_editing_mode(MODE_F); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('t', [this](char32_t) { rx.set_editing_mode(MODE_t); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
    rx.bind_key('T', [this](char32_t) { rx.set_editing_mode(MODE_T); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);

    rx.bind_key('f', [this](char32_t) { rx.set_editing_mode(MODE_DELETE_f); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_DELETE);
    rx.bind_key('F', [this](char32_t) { rx.set_editing_mode(MODE_DELETE_F); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_DELETE);
    rx.bind_key('t', [this](char32_t) { rx.set_editing_mode(MODE_DELETE_t); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_DELETE);
    rx.bind_key('T', [this](char32_t) { rx.set_editing_mode(MODE_DELETE_T); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_DELETE);

    rx.bind_key('f', [this](char32_t) { rx.set_editing_mode(MODE_CHANGE_f); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_CHANGE);
    rx.bind_key('F', [this](char32_t) { rx.set_editing_mode(MODE_CHANGE_F); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_CHANGE);
    rx.bind_key('t', [this](char32_t) { rx.set_editing_mode(MODE_CHANGE_t); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_CHANGE);
    rx.bind_key('T', [this](char32_t) { rx.set_editing_mode(MODE_CHANGE_T); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_CHANGE);

    rx.bind_key('w', [this](char32_t code) {
        int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
        vimbuffer = vimbufferinner = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::DELETE_UNTIL_NEXT_SUBWORD, code);
        rx.set_editing_mode(MODE_NORMAL);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_DELETE);
    rx.bind_key('W', [this](char32_t code) {
        int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
        vimbuffer = vimbufferinner = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::DELETE_UNTIL_NEXT_WORD, code);
        rx.set_editing_mode(MODE_NORMAL);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_DELETE);
    rx.bind_key('w', [this](char32_t code) {
        int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
        vimbuffer = vimbufferinner = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::DELETE_UNTIL_NEXT_SUBWORD, code);
        rx.set_editing_mode(MODE_INSERT);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_CHANGE);
    rx.bind_key('W', [this](char32_t code) {
        int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
        vimbuffer = vimbufferinner = 0;
        for (int32_t rep = 0; rep < reps; rep++)
            rx.invoke(Replxx::ACTION::DELETE_UNTIL_NEXT_WORD, code);
        rx.set_editing_mode(MODE_INSERT);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_CHANGE);
    rx.bind_key('d', [this](char32_t code) {
        int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
        vimbuffer = vimbufferinner = 0;
        for (int32_t rep = 0; rep < reps; rep++) {
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_BEGINING_OF_LINE, code);
            rx.invoke(Replxx::ACTION::KILL_TO_END_OF_LINE, code);
            /* Weird hack because Replxx is acting weird.
             * rx.invoke(Replxx::ACTION::DELETE_CHARACTER_UNDER_CURSOR, code); */
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_RIGHT, code);
            rx.invoke(Replxx::ACTION::DELETE_CHARACTER_LEFT_OF_CURSOR, code);
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_LEFT, code);
        }
        rx.set_editing_mode(MODE_NORMAL);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_DELETE);
    rx.bind_key('c', [this](char32_t code) {
        int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
        vimbuffer = vimbufferinner = 0;
        for (int32_t rep = 0; rep < reps; rep++) {
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_BEGINING_OF_LINE, code);
            rx.invoke(Replxx::ACTION::KILL_TO_END_OF_LINE, code);
        }
        rx.set_editing_mode(MODE_INSERT);
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_CHANGE);

    for (char c = 32; c < 127; c++) {
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = vimbuffer == 0 ? 1 : vimbuffer;
            vimbuffer = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_CHARACTER_UNDER_CURSOR, code);
                rx.invoke(Replxx::ACTION::INSERT_CHARACTER, code);
            }
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_LEFT, code);
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_REPLACE);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_CHARACTER, Replxx::KEY::meta(code));
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_f);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_CHARACTER_REVERSE, Replxx::KEY::meta(code));
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_F);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_CHARACTER, code);
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_t);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_CHARACTER_REVERSE, code);
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_T);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_UNTIL_CHARACTER, Replxx::KEY::meta(code));
            }
            rx.set_editing_mode(MODE_INSERT);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_CHANGE_f);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_UNTIL_CHARACTER, code);
            }
            rx.set_editing_mode(MODE_INSERT);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_CHANGE_t);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_UNTIL_CHARACTER, Replxx::KEY::meta(code));
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_DELETE_f);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_UNTIL_CHARACTER, code);
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_DELETE_t);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_BACKWARDS_UNTIL_CHARACTER, Replxx::KEY::meta(code));
            }
            rx.set_editing_mode(MODE_INSERT);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_CHANGE_F);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_BACKWARDS_UNTIL_CHARACTER, code);
            }
            rx.set_editing_mode(MODE_INSERT);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_CHANGE_T);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_BACKWARDS_UNTIL_CHARACTER, Replxx::KEY::meta(code));
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_DELETE_F);
        rx.bind_key(c, [this](char32_t code) {
            int32_t reps = (vimbuffer == 0 ? 1 : vimbuffer) * (vimbufferinner == 0 ? 1 : vimbufferinner);
            vimbuffer = vimbufferinner = 0;
            for (int32_t rep = 0; rep < reps; rep++) {
                rx.invoke(Replxx::ACTION::DELETE_BACKWARDS_UNTIL_CHARACTER, code);
            }
            rx.set_editing_mode(MODE_NORMAL);
            return Replxx::ACTION_RESULT::CONTINUE;
        }, MODE_DELETE_T);
    }

    for (char c = '0'; c <= '9'; c++) {
        rx.bind_key(c, [this](char32_t code) { vimbuffer = 10 * vimbuffer + (code - '0'); return Replxx::ACTION_RESULT::CONTINUE; }, MODE_NORMAL);
        for (int mode = MODE_DELETE; mode < MODE_REPLACE; mode++)
            rx.bind_key(c, [this](char32_t code) { vimbufferinner = 10 * vimbufferinner + (code - '0'); return Replxx::ACTION_RESULT::CONTINUE; }, mode);
    }
    rx.bind_key('0', [this](char32_t code) {
        if (vimbuffer == 0)
            rx.invoke(Replxx::ACTION::MOVE_CURSOR_TO_BEGINING_OF_LINE, code);
        else
            vimbuffer = 10 * vimbuffer;
        return Replxx::ACTION_RESULT::CONTINUE;
    }, MODE_NORMAL);
#endif
}

}
