/// `src/windows.cxx` in replxx - the Win32 console backend that translates the ANSI escapes
/// replxx emits into `SetConsoleTextAttribute` calls - reads a process-global `replxx::tty::out`
/// ("is standard output a terminal?"). Upstream replxx defines it in `src/terminal.cxx` as
///
///     namespace tty { bool in( is_a_tty( 0 ) ); bool out( is_a_tty( 1 ) ); }
///
/// ClickHouse's fork replaced those globals with the per-descriptor `tty::is_a_tty( fd )` when it
/// added support for custom descriptors, but did not update `windows.cxx`, which still refers to
/// the removed variable and carries a `FIXME` about passing the descriptor in instead. The
/// Windows backend has therefore not been compilable in the fork since.
///
/// Rather than patch the vendored sources, restore the definition here with exactly the upstream
/// initialiser, so `windows.cxx` behaves as it was written to. Note this reintroduces the
/// limitation the `FIXME` describes: the decision is made once for descriptor 1 instead of for
/// the handle actually being written to. Resolving it properly belongs in
/// https://github.com/ClickHouse/replxx, after which this file should be deleted.

#include "../replxx/src/terminal.hxx"

namespace replxx
{
namespace tty
{

bool out(is_a_tty(1));

}
}

namespace replxx {

/*
 * `escape.cxx` calls this to decode a UTF-8 sequence from the keyboard, but `terminal.cxx` defines
 * it only outside `_WIN32`, where replxx reads input through `windows.cxx` instead. The reference
 * still has to resolve, and the console can answer it directly: `ReadConsoleW` hands over UTF-16,
 * from which a surrogate pair is one code point.
 */
char32_t read_unicode_character(int) {
        HANDLE input(GetStdHandle(STD_INPUT_HANDLE));
        wchar_t units[2] = {};
        DWORD read(0);

        if (!ReadConsoleW(input, &units[0], 1, &read, nullptr) || read != 1) {
                return 0;
        }

        /* A high surrogate is only half of a code point; the low half follows it. */
        if ((units[0] >= 0xD800) && (units[0] <= 0xDBFF)) {
                if (!ReadConsoleW(input, &units[1], 1, &read, nullptr) || read != 1) {
                        return 0;
                }
                return static_cast<char32_t>(0x10000 + ((units[0] - 0xD800) << 10) + (units[1] - 0xDC00));
        }

        return static_cast<char32_t>(units[0]);
}

}
