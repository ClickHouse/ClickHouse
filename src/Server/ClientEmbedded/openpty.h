#pragma once
#include <termios.h> // IWYU pragma: export
#include <sys/ioctl.h> // IWYU pragma: export

int openpty(int *amaster, int *aslave, char *name,
            const struct termios *termp, const struct winsize *winp);
