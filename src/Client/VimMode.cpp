#include <Client/ReplxxLineReader.h>

#include <algorithm>

namespace DB
{
using Replxx = replxx::Replxx;

void ReplxxLineReader::resetVim() {
    vimbuffer = vimbufferinner = flag = op = 0;
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
    for (prev_newline = pos; prev_newline > 0; prev_newline--)
        if (text[prev_newline] == '\n')
            break;
    curswant = std::max(prev_newline == 0 ? pos + rx.prompt_indentation() + 1 : pos - prev_newline, 1);
}

void ReplxxLineReader::setupVimKeybindings()
{

    for (int i = 0; i < MODE_END; i++) {
        if (i == MODE_INSERT) {
            bindKey(Replxx::KEY::ESCAPE, [this](int &pos, std::string &text, char32_t) {
                resetVim();
                if (pos > 0 && text[pos - 1] != '\n')
                    pos--;
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

    bindKey('v', [this](char32_t) {
        openEditor(false);
    }, MODE_NORMAL);

    bindKey('h', [this](int &pos, std::string &text, char32_t) {
        if (pos > 0 && text[pos - 1] != '\n')
            pos--;
        recomputeCurswant(pos, text);
    }, MODE_NORMAL);

    bindKey('l', [this](int &pos, std::string &text, char32_t) {
        if (pos < static_cast<int>(text.length()) - 1 && text[pos] != '\n' && text[pos + 1] != '\n')
            pos++;
        recomputeCurswant(pos, text);
    }, MODE_NORMAL);

    bindKey('j', [this](int &pos, std::string &text, char32_t) {
        int length = static_cast<int>(text.length());

        int offset = curswant;

        for (int i = pos; i < length - 1; i++) {
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
