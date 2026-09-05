/* Windows implementation of `readpassphrase`, whose OpenBSD original in `readpassphrase.c` is
 * built on `termios` and `/dev/tty`. Here the terminal is a console: echo is a bit in the console
 * mode rather than a `termios` flag, and there is no device to open instead of stdin - reading
 * from `CONIN$` is the nearest equivalent and is what `RPP_REQUIRE_TTY` needs. */

#include <ctype.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <windows.h>

#include "readpassphrase.h"

char * readpassphrase(const char * prompt, char * buf, size_t bufsiz, int flags)
{
    HANDLE input;
    HANDLE own_input = INVALID_HANDLE_VALUE;
    DWORD original_mode = 0;
    BOOL is_console;
    size_t length;
    char * p;

    if (bufsiz == 0)
    {
        errno = EINVAL;
        return NULL;
    }

    /* `CONIN$` is the console's input, which - unlike `STD_INPUT_HANDLE` - is still the keyboard
     * when stdin has been redirected. That is what makes a password prompt work in a pipeline. */
    if (!(flags & RPP_STDIN))
    {
        own_input = CreateFileA(
            "CONIN$", GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, 0, NULL);
    }

    if (own_input != INVALID_HANDLE_VALUE)
        input = own_input;
    else if (flags & RPP_REQUIRE_TTY)
    {
        errno = ENOTTY;
        return NULL;
    }
    else
        input = GetStdHandle(STD_INPUT_HANDLE);

    is_console = GetConsoleMode(input, &original_mode);
    if (!is_console && (flags & RPP_REQUIRE_TTY))
    {
        if (own_input != INVALID_HANDLE_VALUE)
            CloseHandle(own_input);
        errno = ENOTTY;
        return NULL;
    }

    if (prompt && *prompt)
    {
        /* To stderr, as the original does, so that a redirected stdout does not swallow it. */
        fputs(prompt, stderr);
        fflush(stderr);
    }

    if (is_console)
    {
        DWORD mode = original_mode | ENABLE_LINE_INPUT | ENABLE_PROCESSED_INPUT;
        if (flags & RPP_ECHO_ON)
            mode |= ENABLE_ECHO_INPUT;
        else
            mode &= ~(DWORD)ENABLE_ECHO_INPUT;
        SetConsoleMode(input, mode);
    }

    {
        /* `ReadConsoleW` rather than a narrow read: the console hands over UTF-16, and going
         * through the active code page would mangle anything outside it. */
        WCHAR wide[1024];
        DWORD read = 0;
        int converted = 0;

        if (is_console)
        {
            if (!ReadConsoleW(input, wide, (DWORD)(sizeof(wide) / sizeof(wide[0])), &read, NULL))
                read = 0;
            converted = WideCharToMultiByte(CP_UTF8, 0, wide, (int)read, buf, (int)bufsiz - 1, NULL, NULL);
            buf[converted > 0 ? converted : 0] = '\0';
        }
        else if (!fgets(buf, (int)bufsiz, stdin))
            buf[0] = '\0';
    }

    if (is_console)
    {
        SetConsoleMode(input, original_mode);
        if (!(flags & RPP_ECHO_ON))
        {
            /* The user's Enter was not echoed, so the cursor is still on the prompt's line. */
            fputs("\n", stderr);
            fflush(stderr);
        }
    }

    if (own_input != INVALID_HANDLE_VALUE)
        CloseHandle(own_input);

    /* Strip the line terminator, which on Windows is two characters. */
    length = strlen(buf);
    while (length > 0 && (buf[length - 1] == '\n' || buf[length - 1] == '\r'))
        buf[--length] = '\0';

    for (p = buf; *p; ++p)
    {
        if (flags & RPP_SEVENBIT)
            *p &= 0x7f;
        if (flags & RPP_FORCELOWER)
            *p = (char)tolower((unsigned char)*p);
        if (flags & RPP_FORCEUPPER)
            *p = (char)toupper((unsigned char)*p);
    }

    return buf;
}
