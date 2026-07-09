#include <Client/ReplxxLineReader.h>

#include <algorithm>

namespace DB
{
using Replxx = replxx::Replxx;

void ReplxxLineReader::resetVim(int *pos, std::string *text) {
    vimbuffer = vimbufferinner = flag = op = motion = 0;
    find_direction = 0;
    int length = static_cast<int>(text->length());
    if (pos) {
        if (*pos == length - 1 && length > 0 && (*text)[length - 1] == '\n')
            ++*pos;
        if (text)
            recomputeCurswant(*pos, *text);
    }
}

static int iskeyword(unsigned char c) {
    return ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_' || (192 <= c);
}

static int iswhitespace(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\a' || c == '\b' || c == '\r';
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

    bindKey('i', [this](char32_t) {
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('I', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        for (; pos > 0 && text[pos - 1] != '\n'; pos--);
        for (; pos < length - 1 && iswhitespace(text[pos]); pos++)
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('a', [this](int &pos, std::string &text, char32_t) {
        if (pos < static_cast<int>(text.length()))
            pos++;
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('A', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        for (; pos < length && text[pos] != '\n'; pos++);
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('v', [this](char32_t) {
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
    }, MODE_NORMAL);

    bindKey('h', [this](int &pos, std::string &text, char32_t) {
        if (pos > 0 && text[pos - 1] != '\n')
            pos--;
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('l', [this](int &pos, std::string &text, char32_t) {
        if (pos < static_cast<int>(text.length()) - 1 && text[pos] != '\n' && text[pos + 1] != '\n')
            pos++;
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('j', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());

        int offset = curswant;

        for (int i = pos; i < length; i++) {
            if (text[i] == '\n') {
                for (pos = i + 1; pos < length - 1 && pos < i + offset; pos++)
                    if (text[pos] == '\n' || text[pos + 1] == '\n')
                        break;
                break;
            }
        }
    }, MODE_NORMAL);

    bindKey('k', [this](int &pos, std::string &text, char32_t) {
        if (pos == 0)
            return;

        int length = static_cast<int>(text.length());
        int prev_newline;
        for (prev_newline = pos - 1; prev_newline > 0; prev_newline--)
            if (text[prev_newline] == '\n')
                break;

        int offset = curswant;

        if (prev_newline == 0) {
            if (text[0] == '\n')
                pos = 0;
            return;
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
    }, MODE_NORMAL);

    bindKey('0', [this](int &pos, std::string &text, char32_t) {
        for (; pos > 0 && text[pos - 1] != '\n'; pos--);
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('$', [this](int &pos, std::string &text, char32_t) {
        for (; pos < static_cast<int>(text.length()) - 1 && text[pos] != '\n' && text[pos + 1] != '\n'; pos++);
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('%', [this](int &pos, std::string &text, char32_t) {
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
            int length = static_cast<int>(text.length());
            bool forward = find_direction == 'f' || find_direction == 't';
            for (int i = forward ? pos + 1 : pos - 1; forward ? i < length : i >= 0; i += forward ? 1 : -1) {
                if (text[i] == c) {
                    pos = i;
                    if (find_direction == 't')
                        pos--;
                    if (find_direction == 'T')
                        pos++;
                    break;
                }
            }
            resetVim(&pos, &text);
            rx.set_editing_mode(MODE_NORMAL);
        }, MODE_FIND);


        bindKey(c, [this, c](int &pos, std::string &text, char32_t) {
            if (pos < static_cast<int>(text.length()))
                text[pos] = c;
            resetVim();
        }, MODE_REPLACE);
    }

    bindKey('w', [this](int &pos, std::string &text, char32_t) {
        int break_on_char = 0;
        for (; pos < static_cast<int>(text.length()) - 1; pos++) {
            if (break_on_char & 1 && iskeyword(text[pos]))
                break;
            if (break_on_char & 2 && !iskeyword(text[pos]) && !iswhitespace(text[pos]))
                break;

            if (iswhitespace(text[pos]))
                break_on_char = 3;
            else if (iskeyword(text[pos]))
                break_on_char |= 2;
            else if (!iswhitespace(text[pos]))
                break_on_char |= 1;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('e', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        if (pos < length - 1)
            pos++;
        int break_on_char = 0;
        for (; pos < length - 1; pos++) {
            if (iskeyword(text[pos]))
                break_on_char = 1;
            else if (!iswhitespace(text[pos]))
                break_on_char = 2;

            if (break_on_char & 1 && !iskeyword(text[pos + 1]))
                break;
            if (break_on_char & 2 && (iswhitespace(text[pos + 1]) || iskeyword(text[pos + 1])))
                break;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('b', [this](int &pos, std::string &text, char32_t) {
        if (pos > 0)
            pos--;
        int break_on_char = 0;
        for (; pos > 0; pos--) {
            if (iskeyword(text[pos]))
                break_on_char = 1;
            else if (!iswhitespace(text[pos]))
                break_on_char = 2;

            if (break_on_char & 1 && !iskeyword(text[pos - 1]))
                break;
            if (break_on_char & 2 && (iswhitespace(text[pos - 1]) || iskeyword(text[pos - 1])))
                break;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('W', [this](int &pos, std::string &text, char32_t) {
        int break_on_char = 0;
        for (; pos < static_cast<int>(text.length()) - 1; pos++) {
            if (break_on_char && !iswhitespace(text[pos]))
                break;

            if (iswhitespace(text[pos]))
                break_on_char = 1;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('E', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        if (pos < length - 1)
            pos++;
        int break_on_char = 0;
        for (; pos < length - 1; pos++) {
            if (!iswhitespace(text[pos]))
                break_on_char = 1;

            if (break_on_char && iswhitespace(text[pos + 1]))
                break;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('B', [this](int &pos, std::string &text, char32_t) {
        if (pos > 0)
            pos--;
        int break_on_char = 0;
        for (; pos > 0; pos--) {
            if (!iswhitespace(text[pos]))
                break_on_char = 1;

            if (break_on_char && iswhitespace(text[pos - 1]))
                break;
        }
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('G', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());
        pos = std::max(length - 1, 0);
        resetVim(&pos, &text);
    }, MODE_NORMAL);








    bindKey('s', [this](int &pos, std::string &text, char32_t) {
        if (pos < static_cast<int>(text.length())) {
            text.erase(pos, 1);
            resetVim(&pos, &text);
        }
        rx.set_editing_mode(MODE_INSERT);
    }, MODE_NORMAL);

    bindKey('r', [this](char32_t) {
        rx.set_editing_mode(MODE_REPLACE);
    }, MODE_NORMAL);

    bindKey('x', [this](int &pos, std::string &text, char32_t) {
        if (pos < static_cast<int>(text.length())) {
            text.erase(pos, 1);
            int length = static_cast<int>(text.length());
            if (pos >= length)
                pos = std::max(length - 1, 0);
            resetVim(&pos, &text);
        }
    }, MODE_NORMAL);

    bindKey('D', [this](int &pos, std::string &text, char32_t) {
        int end;
        int length = static_cast<int>(text.length());
        for (end = pos; end <= length && text[end] != '\n'; end++);
        if (end > pos) {
            text.erase(pos, end - pos);
            if (pos > 0)
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
            if (pos > 0)
                pos--;
        }
        rx.set_editing_mode(MODE_INSERT);
        resetVim(&pos, &text);
    }, MODE_NORMAL);

    bindKey('S', [this](int &pos, std::string &text, char32_t) {
        int end;
        int length = static_cast<int>(text.length());
        for (; pos > 0 && text[pos - 1] != '\n'; pos--);
        for (end = pos; end <= length && text[end] != '\n'; end++);
        if (end > pos)
            text.erase(pos, end - pos);
        rx.set_editing_mode(MODE_INSERT);
        resetVim(&pos, &text);
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
