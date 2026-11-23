/*    $OpenBSD: readpassphrase.c,v 1.26 2016/10/18 12:47:18 millert Exp $    */

/*
 * Copyright (c) 2000-2002, 2007, 2010
 *    Todd C. Miller <Todd.Miller@courtesan.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Sponsored in part by the Defense Advanced Research Projects
 * Agency (DARPA) and Air Force Research Laboratory, Air Force
 * Materiel Command, USAF, under agreement number F39502-99-1-0512.
 */

/* OPENBSD ORIGINAL: lib/libc/gen/readpassphrase.c */

#ifndef _PATH_TTY
#define _PATH_TTY "/dev/tty"
#endif

#pragma clang diagnostic ignored "-Wreserved-identifier"

#include <termios.h>
#include <signal.h>
#include <ctype.h>
#include <fcntl.h>
#include "readpassphrase.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#ifndef TCSASOFT
/* If we don't have TCSASOFT define it so that ORing it it below is a no-op. */
# define TCSASOFT 0
#endif

/* SunOS 4.x which lacks _POSIX_VDISABLE, but has VDISABLE */
#if !defined(_POSIX_VDISABLE) && defined(VDISABLE)
#  define _POSIX_VDISABLE       VDISABLE
#endif

static volatile sig_atomic_t signo[NSIG];

static void handler(int);

static char
convert_char(char ch, int flags)
{
    if ((flags & RPP_SEVENBIT))
        ch &= 0x7f;
    if (isalpha((unsigned char)ch)) {
        if ((flags & RPP_FORCELOWER))
            ch = (char)tolower((unsigned char)ch);
        if ((flags & RPP_FORCEUPPER))
            ch = (char)toupper((unsigned char)ch);
    }
    return ch;
}

/*
 * ClickHouse extension: with `readpassphrase_masked`, every typed character is echoed as a mask,
 * so the user can see how many characters were typed and notice stray control characters.
 * The terminal is switched to the non-canonical mode to receive the characters as they are typed,
 * so the line editing of the canonical mode is done here.
 */

struct masked_input {
    int output;
    const char *prompt;
    size_t prompt_width;           /* The columns taken by the prompt. */
    const struct termios *term;    /* The original terminal settings. */
    const char *mask;
    size_t mask_len;
    int utf8;                      /* Whether a character can take several bytes, as with `IUTF8`. */
    size_t columns;                /* The columns taken by the prompt and the masks in the input line. */
    int below_last_mask;           /* Whether the cursor is at the start of the line below the last mask. */
};

static int
is_control_char(const struct termios *term, int index, char ch)
{
    return term->c_cc[index] != _POSIX_VDISABLE && term->c_cc[index] == (cc_t)ch;
}

static int
is_utf8_continuation(char ch, int utf8)
{
    return utf8 && ((unsigned char)ch & 0xC0) == 0x80;
}

/* The number of characters, each is masked once. */
static size_t
count_chars(const char *begin, const char *end, int utf8)
{
    size_t chars = 0;

    for (; begin < end; begin++) {
        if (!is_utf8_continuation(*begin, utf8))
            chars++;
    }
    return chars;
}

static void
write_string(int output, const char *s)
{
    (void)write(output, s, strlen(s));
}

static void
show_mask(struct masked_input *m)
{
    (void)write(m->output, m->mask, m->mask_len);
    m->columns++;
    m->below_last_mask = 0;
}

static void
erase_mask(struct masked_input *m)
{
    struct winsize size;
    size_t last = m->columns - 1, column = last;
    char sequence[32];

    /*
     * A backspace does not move the cursor to the previous line, so move it to the column of the last mask
     * explicitly. After a character is written in the last column, the cursor stays there until the next
     * character is written, as in VT100. But after the mask in the first column is erased, the cursor is
     * below the mask to erase next.
     */
    if (ioctl(m->output, TIOCGWINSZ, &size) == 0 && size.ws_col != 0) {
        column = last % size.ws_col;
        if (m->below_last_mask)
            write_string(m->output, "\033[A");
        m->below_last_mask = column == 0;
    }
    /* Move the cursor to the column and clear the line from there. */
    (void)snprintf(sequence, sizeof(sequence), "\033[%zuG\033[K", column + 1);
    write_string(m->output, sequence);
    m->columns--;
}

/* Remove the last character after `begin` from the buffer and its mask from the terminal. */
static void
erase_char(struct masked_input *m, char *begin, char **p)
{
    char ch;

    while (*p > begin) {
        ch = *--(*p);
        **p = '\0';
        /* Each character is masked once, at its first byte. */
        if (!is_utf8_continuation(ch, m->utf8)) {
            erase_mask(m);
            return;
        }
    }
}

/* Remove the trailing non-alphanumeric characters after `begin` and the word before them, as the kernel does. */
static void
erase_word(struct masked_input *m, char *begin, char **p)
{
    int seen_alnum = 0;
    unsigned char c;

    while (*p > begin) {
        c = (unsigned char)(*p)[-1];
        if (isalnum(c) || c == '_')
            seen_alnum = 1;
        else if (seen_alnum)
            break;
        erase_char(m, begin, p);
    }
}

static void
reprint(struct masked_input *m, const char *buf, const char *p)
{
    size_t masks = count_chars(buf, p, m->utf8), i;

    write_string(m->output, "\r\n");
    write_string(m->output, m->prompt);
    m->columns = m->prompt_width;
    m->below_last_mask = 0;
    for (i = 0; i < masks; i++)
        show_mask(m);
}

static ssize_t
read_masked(int input, struct masked_input *m, char *buf, char **p, char *end, int flags)
{
    const struct termios *term = m->term;
    const int extended = (term->c_lflag & IEXTEN) != 0;
    int literal = 0;
    ssize_t nr;
    char ch;
    /*
     * In the canonical mode, the end of file character passes the line typed so far to `read`,
     * so it cannot be erased anymore, and it finishes the input if nothing was typed after that.
     */
    char *line_start = buf;

    while ((nr = read(input, &ch, 1)) == 1) {
        if (literal) {
            literal = 0;
        } else if (ch == '\n' || ch == '\r' || is_control_char(term, VEOL, ch)
            || (extended && is_control_char(term, VEOL2, ch))) {
            break;
        } else if (ch == '\b' || ch == 0x7f || is_control_char(term, VERASE, ch)) {
            erase_char(m, line_start, p);
            continue;
        } else if (is_control_char(term, VKILL, ch)) {
            while (*p > line_start)
                erase_char(m, line_start, p);
            continue;
        } else if (extended && is_control_char(term, VWERASE, ch)) {
            erase_word(m, line_start, p);
            continue;
        } else if (extended && is_control_char(term, VLNEXT, ch)) {
            literal = 1;
            continue;
        } else if (extended && is_control_char(term, VREPRINT, ch)) {
            reprint(m, buf, *p);
            continue;
        } else if (is_control_char(term, VEOF, ch)) {
            if (*p == line_start)
                break;
            line_start = *p;
            continue;
        }
        if (*p < end) {
            ch = convert_char(ch, flags);
            *(*p)++ = ch;
            if (!is_utf8_continuation(ch, m->utf8))
                show_mask(m);
        }
    }
    return nr;
}

static char *
readpassphrase_impl(const char *prompt, size_t prompt_width, int echo_mask, char *buf, size_t bufsiz, int flags)
{
    ssize_t nr;
    int input, output, save_errno, i, need_restart, mask;
    struct masked_input masked;
    char ch, *p, *end;
    struct termios term, oterm;
    struct sigaction sa, savealrm, saveint, savehup, savequit, saveterm;
    struct sigaction savetstp, savettin, savettou, savepipe;

    /* I suppose we could alloc on demand in this case (XXX). */
    if (bufsiz == 0) {
        errno = EINVAL;
        return (char *)NULL;
    }

restart:
    for (i = 0; i < NSIG; i++)
        signo[i] = 0;
    nr = -1;
    save_errno = 0;
    need_restart = 0;
    mask = 0;
    /*
     * Read and write to /dev/tty if available.  If not, read from
     * stdin and write to stderr unless a tty is required.
     */
    if ((flags & RPP_STDIN) ||
        (input = output = open(_PATH_TTY, O_RDWR)) == -1) {
        if (flags & RPP_REQUIRE_TTY) {
            errno = ENOTTY;
            return (char *)NULL;
        }
        input = STDIN_FILENO;
        output = STDERR_FILENO;
    }

    /*
     * Turn off echo if possible.
     * If we are using a tty but are not the foreground pgrp this will
     * generate SIGTTOU, so do it *before* installing the signal handlers.
     */
    if (input != STDIN_FILENO && tcgetattr(input, &oterm) == 0) {
        memcpy(&term, &oterm, sizeof(term));
        if (!(flags & RPP_ECHO_ON)) {
            term.c_lflag &= ~((unsigned int) (ECHO | ECHONL));
            if (echo_mask) {
                /* Receive the characters as they are typed to echo the masks. */
                term.c_lflag &= ~((unsigned int) ICANON);
                term.c_cc[VMIN] = 1;
                term.c_cc[VTIME] = 0;
                mask = 1;
            }
        }
#ifdef VSTATUS
        if (term.c_cc[VSTATUS] != _POSIX_VDISABLE)
            term.c_cc[VSTATUS] = _POSIX_VDISABLE;
#endif
        (void)tcsetattr(input, TCSAFLUSH|TCSASOFT, &term);
    } else {
        memset(&term, 0, sizeof(term));
        term.c_lflag |= ECHO;
        memset(&oterm, 0, sizeof(oterm));
        oterm.c_lflag |= ECHO;
    }

    /*
     * Catch signals that would otherwise cause the user to end
     * up with echo turned off in the shell.  Don't worry about
     * things like SIGXCPU and SIGVTALRM for now.
     */
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;        /* don't restart system calls */
    sa.sa_handler = handler;
    (void)sigaction(SIGALRM, &sa, &savealrm);
    (void)sigaction(SIGHUP, &sa, &savehup);
    (void)sigaction(SIGINT, &sa, &saveint);
    (void)sigaction(SIGPIPE, &sa, &savepipe);
    (void)sigaction(SIGQUIT, &sa, &savequit);
    (void)sigaction(SIGTERM, &sa, &saveterm);
    (void)sigaction(SIGTSTP, &sa, &savetstp);
    (void)sigaction(SIGTTIN, &sa, &savettin);
    (void)sigaction(SIGTTOU, &sa, &savettou);

    if (!(flags & RPP_STDIN))
        (void)write(output, prompt, strlen(prompt));
    end = buf + bufsiz - 1;
    p = buf;
    if (mask) {
        masked.output = output;
        masked.prompt = prompt;
        masked.prompt_width = prompt_width;
        masked.term = &oterm;
        if (flags & RPP_MASK_BULLET) {
            masked.mask = "\xE2\x97\x8F"; /* U+25CF BLACK CIRCLE */
            masked.mask_len = 3;
        } else {
            masked.mask = "*";
            masked.mask_len = 1;
        }
        masked.utf8 = 0;
#ifdef IUTF8
        masked.utf8 = (oterm.c_iflag & IUTF8) != 0;
#endif
        /* The prompt is assumed to start at the beginning of the line. */
        masked.columns = prompt_width;
        masked.below_last_mask = 0;
        nr = read_masked(input, &masked, buf, &p, end, flags);
    } else {
        while ((nr = read(input, &ch, 1)) == 1 && ch != '\n' && ch != '\r') {
            if (p < end)
                *p++ = convert_char(ch, flags);
        }
    }
    *p = '\0';
    save_errno = errno;
    if (!(term.c_lflag & ECHO))
        (void)write(output, "\n", 1);

    /* Restore old terminal settings and signals. */
    if (memcmp(&term, &oterm, sizeof(term)) != 0) {
        const int sigttou = (int)signo[SIGTTOU];

        /* Ignore SIGTTOU generated when we are not the fg pgrp. */
        while (tcsetattr(input, TCSAFLUSH|TCSASOFT, &oterm) == -1 &&
            errno == EINTR && !signo[SIGTTOU])
            continue;
        signo[SIGTTOU] = sigttou;
    }
    (void)sigaction(SIGALRM, &savealrm, NULL);
    (void)sigaction(SIGHUP, &savehup, NULL);
    (void)sigaction(SIGINT, &saveint, NULL);
    (void)sigaction(SIGQUIT, &savequit, NULL);
    (void)sigaction(SIGPIPE, &savepipe, NULL);
    (void)sigaction(SIGTERM, &saveterm, NULL);
    (void)sigaction(SIGTSTP, &savetstp, NULL);
    (void)sigaction(SIGTTIN, &savettin, NULL);
    (void)sigaction(SIGTTOU, &savettou, NULL);
    if (input != STDIN_FILENO)
        (void)close(input);

    /*
     * If we were interrupted by a signal, resend it to ourselves
     * now that we have restored the signal handlers.
     */
    for (i = 0; i < NSIG; i++) {
        if (signo[i]) {
            kill(getpid(), i);
            switch (i) {
            case SIGTSTP:
            case SIGTTIN:
            case SIGTTOU:
                need_restart = 1;
            }
        }
    }
    if (need_restart)
        goto restart;

    if (save_errno)
        errno = save_errno;
    return(nr == -1 ? NULL : buf);
}
//DEF_WEAK(readpassphrase);

char *
readpassphrase(const char *prompt, char *buf, size_t bufsiz, int flags)
{
    return readpassphrase_impl(prompt, 0, 0, buf, bufsiz, flags);
}

char *
readpassphrase_masked(const char *prompt, size_t prompt_width, char *buf, size_t bufsiz, int flags)
{
    return readpassphrase_impl(prompt, prompt_width, 1, buf, bufsiz, flags);
}

static void handler(int s)
{
    signo[s] = 1;
}
