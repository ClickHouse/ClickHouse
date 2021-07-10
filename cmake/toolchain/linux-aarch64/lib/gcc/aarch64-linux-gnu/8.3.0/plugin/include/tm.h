#ifndef GCC_TM_H
#define GCC_TM_H
#ifndef LIBC_GLIBC
# define LIBC_GLIBC 1
#endif
#ifndef LIBC_UCLIBC
# define LIBC_UCLIBC 2
#endif
#ifndef LIBC_BIONIC
# define LIBC_BIONIC 3
#endif
#ifndef LIBC_MUSL
# define LIBC_MUSL 4
#endif
#ifndef DEFAULT_LIBC
# define DEFAULT_LIBC LIBC_GLIBC
#endif
#ifndef ANDROID_DEFAULT
# define ANDROID_DEFAULT 0
#endif
#ifndef TARGET_DEFAULT_ASYNC_UNWIND_TABLES
# define TARGET_DEFAULT_ASYNC_UNWIND_TABLES 1
#endif
#ifndef TARGET_FIX_ERR_A53_843419_DEFAULT
# define TARGET_FIX_ERR_A53_843419_DEFAULT 1
#endif
#ifdef IN_GCC
# include "options.h"
# include "insn-constants.h"
# include "config/aarch64/biarchlp64.h"
# include "config/aarch64/aarch64.h"
# include "config/dbxelf.h"
# include "config/elfos.h"
# include "config/gnu-user.h"
# include "config/linux.h"
# include "config/glibc-stdint.h"
# include "config/aarch64/aarch64-elf.h"
# include "config/aarch64/aarch64-linux.h"
# include "config/initfini-array.h"
#endif
#if defined IN_GCC && !defined GENERATOR_FILE && !defined USED_FOR_TARGET
# include "insn-flags.h"
#endif
#if defined IN_GCC && !defined GENERATOR_FILE
# include "insn-modes.h"
#endif
# include "defaults.h"
#endif /* GCC_TM_H */
