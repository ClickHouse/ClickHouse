/* ClickHouse-specific replacement for the `mz_config.h` header that upstream
 * `minizip-ng` generates from `mz_config.h.cmakein` with compiler and system
 * checks. ClickHouse builds are hermetic and cross-compiled, so the CMake
 * `Check*` modules are forbidden and the values are hardcoded instead.
 *
 * All platforms ClickHouse targets (Linux with glibc or musl, macOS, FreeBSD)
 * are POSIX systems providing `dirent.h`, `inttypes.h`, `stdint.h`, `DIR`,
 * `fseeko`, `symlink` and `readlink`.
 */

#ifndef MZ_CONFIG_H
#define MZ_CONFIG_H

// Define to 1 if you have the <dirent.h> header file.
#define HAVE_DIRENT_H 1

// Define to 1 if you have the <sys/dirent.h> header file.
#define HAVE_SYS_DIRENT_H 0

// Define to 1 if you have the <inttypes.h> header file.
#define HAVE_INTTYPES_H 1

// Define to 1 if you have the <stdint.h> header file.
#define HAVE_STDINT_H 1

// Define to 1 if DIR* is defined.
#define HAVE_PDIR 1

// Define to 1 if fseeko() is defined.
#define HAVE_FSEEKO 1

// Define to 1 if symlink() is defined.
#define HAVE_SYMLINK 1

// Define to 1 if readlink() is defined.
#define HAVE_READLINK 1

#endif
