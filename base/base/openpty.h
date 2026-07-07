#pragma once

#include <sys/ioctl.h>


/// Portable replacement for libc's `openpty` using POSIX `posix_openpt`/`grantpt`/`unlockpt`.
/// Opens a new pseudo-terminal pair, applies the window size to the slave side,
/// and writes the master/slave file descriptors into the output references.
/// Returns 0 on success, -1 on failure with `errno` set.
int openPty(int & master_fd, int & slave_fd, const winsize & ws);
