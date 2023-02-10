/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */

/* PCRE is written in Standard C, but there are a few non-standard things it
can cope with, allowing it to run on SunOS4 and other "close to standard"
systems.

In environments that support the GNU autotools, config.h.in is converted into
config.h by the "configure" script. In environments that use CMake,
config-cmake.in is converted into config.h. If you are going to build PCRE "by
hand" without using "configure" or CMake, you should copy the distributed
config.h.generic to config.h, and edit the macro definitions to be the way you
need them. You must then add -DHAVE_CONFIG_H to all of your compile commands,
so that config.h is included at the start of every source.

Alternatively, you can avoid editing by using -D on the compiler command line
to set the macro values. In this case, you do not have to set -DHAVE_CONFIG_H,
but if you do, default values will be taken from config.h for non-boolean
macros that are not defined on the command line.

Boolean macros such as HAVE_STDLIB_H and SUPPORT_PCRE8 should either be defined
(conventionally to 1) for TRUE, and not defined at all for FALSE. All such
macros are listed as a commented #undef in config.h.generic. Macros such as
MATCH_LIMIT, whose actual value is relevant, have defaults defined, but are
surrounded by #ifndef/#endif lines so that the value can be overridden by -D.

PCRE uses memmove() if HAVE_MEMMOVE is defined; otherwise it uses bcopy() if
HAVE_BCOPY is defined. If your system has neither bcopy() nor memmove(), make
sure both macros are undefined; an emulation function will then be used. */

/* By default, the \R escape sequence matches any Unicode line ending
   character or sequence of characters. If BSR_ANYCRLF is defined (to any
   value), this is changed so that backslash-R matches only CR, LF, or CRLF.
   The build-time default can be overridden by the user of PCRE at runtime. */
/* #undef BSR_ANYCRLF */

/* If you are compiling for a system that uses EBCDIC instead of ASCII
   character codes, define this macro to any value. You must also edit the
   NEWLINE macro below to set a suitable EBCDIC newline, commonly 21 (0x15).
   On systems that can use "configure" or CMake to set EBCDIC, NEWLINE is
   automatically adjusted. When EBCDIC is set, PCRE assumes that all input
   strings are in EBCDIC. If you do not define this macro, PCRE will assume
   input strings are ASCII or UTF-8/16/32 Unicode. It is not possible to build
   a version of PCRE that supports both EBCDIC and UTF-8/16/32. */
/* #undef EBCDIC */

/* In an EBCDIC environment, define this macro to any value to arrange for the
   NL character to be 0x25 instead of the default 0x15. NL plays the role that
   LF does in an ASCII/Unicode environment. The value must also be set in the
   NEWLINE macro below. On systems that can use "configure" or CMake to set
   EBCDIC_NL25, the adjustment of NEWLINE is automatic. */
/* #undef EBCDIC_NL25 */

/* Define to 1 if you have the `bcopy' function. */
/* #undef HAVE_BCOPY */

/* Define to 1 if you have the <bits/type_traits.h> header file. */
/* #undef HAVE_BITS_TYPE_TRAITS_H */

/* Define to 1 if you have the <bzlib.h> header file. */
/* #undef HAVE_BZLIB_H */

/* Define to 1 if you have the <dirent.h> header file. */
#ifndef HAVE_DIRENT_H
#define HAVE_DIRENT_H 0
#endif

/* Define to 1 if you have the <dlfcn.h> header file. */
#ifndef HAVE_DLFCN_H
#define HAVE_DLFCN_H 0
#endif

/* Define to 1 if you have the <editline/readline.h> header file. */
#ifndef HAVE_EDITLINE_READLINE_H
#define HAVE_EDITLINE_READLINE_H 0
#endif

/* Define to 1 if you have the <edit/readline/readline.h> header file. */
#ifndef HAVE_EDIT_READLINE_READLINE_H
#define HAVE_EDIT_READLINE_READLINE_H 0
#endif

/* Define to 1 if you have the <inttypes.h> header file. */
#ifndef HAVE_INTTYPES_H
#define HAVE_INTTYPES_H 0
#endif

/* Define to 1 if you have the <limits.h> header file. */
/* #undef HAVE_LIMITS_H */
#ifndef HAVE_LIMITS_H
#define HAVE_LIMITS_H 1
#endif

/* Define to 1 if the system has the type `long long'. */
#ifndef HAVE_LONG_LONG
#define HAVE_LONG_LONG 0
#endif

/* Define to 1 if you have the `memmove' function. */
#ifndef HAVE_MEMMOVE
#define HAVE_MEMMOVE 0
#endif

/* Define to 1 if you have the <memory.h> header file. */
#ifndef HAVE_MEMORY_H
#define HAVE_MEMORY_H 1
#endif

/* Define if you have POSIX threads libraries and header files. */
#ifndef HAVE_PTHREAD
#define HAVE_PTHREAD 0
#endif

/* Have PTHREAD_PRIO_INHERIT. */
#ifndef HAVE_PTHREAD_PRIO_INHERIT
#define HAVE_PTHREAD_PRIO_INHERIT 0
#endif

/* Define to 1 if you have the <readline/history.h> header file. */
#ifndef HAVE_READLINE_HISTORY_H
#define HAVE_READLINE_HISTORY_H 0
#endif

/* Define to 1 if you have the <readline/readline.h> header file. */
#ifndef HAVE_READLINE_READLINE_H
#define HAVE_READLINE_READLINE_H 0
#endif

/* Define to 1 if you have the <stdint.h> header file. */
#ifndef HAVE_STDINT_H
#define HAVE_STDINT_H 0
#endif

/* Define to 1 if you have the <stdlib.h> header file. */
#ifndef HAVE_STDLIB_H
#define HAVE_STDLIB_H 1
#endif

/* Define to 1 if you have the `strerror' function. */
#ifndef HAVE_STRERROR
#define HAVE_STRERROR 0
#endif

/* Define to 1 if you have the <string> header file. */
#ifndef HAVE_STRING
#define HAVE_STRING 0
#endif

/* Define to 1 if you have the <strings.h> header file. */
#ifndef HAVE_STRINGS_H
#define HAVE_STRINGS_H 0
#endif

/* Define to 1 if you have the <string.h> header file. */
#ifndef HAVE_STRING_H
#define HAVE_STRING_H 1
#endif

/* Define to 1 if you have `strtoimax'. */
#ifndef HAVE_STRTOIMAX
#define HAVE_STRTOIMAX 0
#endif

/* Define to 1 if you have `strtoll'. */
#ifndef HAVE_STRTOLL
#define HAVE_STRTOLL 0
#endif

/* Define to 1 if you have `strtoq'. */
#ifndef HAVE_STRTOQ
#define HAVE_STRTOQ 0
#endif

/* Define to 1 if you have the <sys/stat.h> header file. */
#ifndef HAVE_SYS_STAT_H
#define HAVE_SYS_STAT_H 0
#endif

/* Define to 1 if you have the <sys/types.h> header file. */
#ifndef HAVE_SYS_TYPES_H
#define HAVE_SYS_TYPES_H 0
#endif

/* Define to 1 if you have the <type_traits.h> header file. */
/* #undef HAVE_TYPE_TRAITS_H */

/* Define to 1 if you have the <unistd.h> header file. */
#ifndef HAVE_UNISTD_H
#define HAVE_UNISTD_H 0
#endif

/* Define to 1 if the system has the type `unsigned long long'. */
#ifndef HAVE_UNSIGNED_LONG_LONG
#define HAVE_UNSIGNED_LONG_LONG 0
#endif

/* Define to 1 if the compiler supports simple visibility declarations. */
/* #undef HAVE_VISIBILITY */

/* Define to 1 if you have the <windows.h> header file. */
/* #undef HAVE_WINDOWS_H */

/* Define to 1 if you have the <zlib.h> header file. */
/* #undef HAVE_ZLIB_H */

/* Define to 1 if you have `_strtoi64'. */
/* #undef HAVE__STRTOI64 */

/* The value of LINK_SIZE determines the number of bytes used to store links
   as offsets within the compiled regex. The default is 2, which allows for
   compiled patterns up to 64K long. This covers the vast majority of cases.
   However, PCRE can also be compiled to use 3 or 4 bytes instead. This allows
   for longer patterns in extreme cases. */
#ifndef LINK_SIZE
#define LINK_SIZE 2
#endif

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
/* This is ignored unless you are using libtool. */
#ifndef LT_OBJDIR
#define LT_OBJDIR ".libs/"
#endif

/* The value of MATCH_LIMIT determines the default number of times the
   internal match() function can be called during a single execution of
   pcre_exec(). There is a runtime interface for setting a different limit.
   The limit exists in order to catch runaway regular expressions that take
   for ever to determine that they do not match. The default is set very large
   so that it does not accidentally catch legitimate cases. */
#ifndef MATCH_LIMIT
#define MATCH_LIMIT 10000000
#endif

/* The above limit applies to all calls of match(), whether or not they
   increase the recursion depth. In some environments it is desirable to limit
   the depth of recursive calls of match() more strictly, in order to restrict
   the maximum amount of stack (or heap, if NO_RECURSE is defined) that is
   used. The value of MATCH_LIMIT_RECURSION applies only to recursive calls of
   match(). To have any useful effect, it must be less than the value of
   MATCH_LIMIT. The default is to use the same value as MATCH_LIMIT. There is
   a runtime method for setting a different limit. */
#ifndef MATCH_LIMIT_RECURSION
#define MATCH_LIMIT_RECURSION MATCH_LIMIT
#endif

/* This limit is parameterized just in case anybody ever wants to change it.
   Care must be taken if it is increased, because it guards against integer
   overflow caused by enormously large patterns. */
#ifndef MAX_NAME_COUNT
#define MAX_NAME_COUNT 30000
#endif

/* This limit is parameterized just in case anybody ever wants to change it.
   Care must be taken if it is increased, because it guards against integer
   overflow caused by enormously large patterns. */
#ifndef MAX_NAME_SIZE
#define MAX_NAME_SIZE 32
#endif

/* The value of NEWLINE determines the default newline character sequence.
   PCRE client programs can override this by selecting other values at run
   time. In ASCII environments, the value can be 10 (LF), 13 (CR), or 3338
   (CRLF); in EBCDIC environments the value can be 21 or 37 (LF), 13 (CR), or
   3349 or 3365 (CRLF) because there are two alternative codepoints (0x15 and
   0x25) that are used as the NL line terminator that is equivalent to ASCII
   LF. In both ASCII and EBCDIC environments the value can also be -1 (ANY),
   or -2 (ANYCRLF). */
#ifndef NEWLINE
#define NEWLINE 10
#endif

/* PCRE uses recursive function calls to handle backtracking while matching.
   This can sometimes be a problem on systems that have stacks of limited
   size. Define NO_RECURSE to any value to get a version that doesn't use
   recursion in the match() function; instead it creates its own stack by
   steam using pcre_recurse_malloc() to obtain memory from the heap. For more
   detail, see the comments and other stuff just above the match() function.
   */
/* #undef NO_RECURSE */

/* Name of package */
#define PACKAGE "pcre"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT ""

/* Define to the full name of this package. */
#define PACKAGE_NAME "PCRE"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "PCRE 8.43"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "pcre"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "8.43"

/* The value of PARENS_NEST_LIMIT specifies the maximum depth of nested
   parentheses (of any kind) in a pattern. This limits the amount of system
   stack that is used while compiling a pattern. */
#ifndef PARENS_NEST_LIMIT
#define PARENS_NEST_LIMIT 250
#endif

/* The value of PCREGREP_BUFSIZE determines the size of buffer used by
   pcregrep to hold parts of the file it is searching. This is also the
   minimum value. The actual amount of memory used by pcregrep is three times
   this number, because it allows for the buffering of "before" and "after"
   lines. */
#ifndef PCREGREP_BUFSIZE
#define PCREGREP_BUFSIZE 20480
#endif

/* If you are compiling for a system other than a Unix-like system or
   Win32, and it needs some magic to be inserted before the definition
   of a function that is exported by the library, define this macro to
   contain the relevant magic. If you do not define this macro, a suitable
    __declspec value is used for Windows systems; in other environments
   "extern" is used for a C compiler and "extern C" for a C++ compiler.
   This macro apears at the start of every exported function that is part
   of the external API. It does not appear on functions that are "external"
   in the C sense, but which are internal to the library. */
/* #undef PCRE_EXP_DEFN */

/* Define to any value if linking statically (TODO: make nice with Libtool) */
#ifndef PCRE_STATIC
#define PCRE_STATIC 1
#endif

/* When calling PCRE via the POSIX interface, additional working storage is
   required for holding the pointers to capturing substrings because PCRE
   requires three integers per substring, whereas the POSIX interface provides
   only two. If the number of expected substrings is small, the wrapper
   function uses space on the stack, because this is faster than using
   malloc() for each call. The threshold above which the stack is no longer
   used is defined by POSIX_MALLOC_THRESHOLD. */
#ifndef POSIX_MALLOC_THRESHOLD
#define POSIX_MALLOC_THRESHOLD 10
#endif

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* Define to 1 if you have the ANSI C header files. */
#ifndef STDC_HEADERS
#define STDC_HEADERS 1
#endif

/* Define to any value to enable support for Just-In-Time compiling. */
/* #undef SUPPORT_JIT */

/* Define to any value to allow pcregrep to be linked with libbz2, so that it
   is able to handle .bz2 files. */
/* #undef SUPPORT_LIBBZ2 */

/* Define to any value to allow pcretest to be linked with libedit. */
/* #undef SUPPORT_LIBEDIT */

/* Define to any value to allow pcretest to be linked with libreadline. */
/* #undef SUPPORT_LIBREADLINE */

/* Define to any value to allow pcregrep to be linked with libz, so that it is
   able to handle .gz files. */
/* #undef SUPPORT_LIBZ */

/* Define to any value to enable the 16 bit PCRE library. */
/* #undef SUPPORT_PCRE16 */

/* Define to any value to enable the 32 bit PCRE library. */
/* #undef SUPPORT_PCRE32 */

/* Define to any value to enable the 8 bit PCRE library. */
#ifndef SUPPORT_PCRE8
#define SUPPORT_PCRE8
#endif

/* Define to any value to enable JIT support in pcregrep. */
/* #undef SUPPORT_PCREGREP_JIT */

/* Define to any value to enable support for Unicode properties. */
#ifndef SUPPORT_UCP
#define SUPPORT_UCP
#endif

/* Define to any value to enable support for the UTF-8/16/32 Unicode encoding.
   This will work even in an EBCDIC environment, but it is incompatible with
   the EBCDIC macro. That is, PCRE can support *either* EBCDIC code *or*
   ASCII/UTF-8/16/32, but not both at once. */
#ifndef SUPPORT_UTF8
#define SUPPORT_UTF8
#endif

/* Define to any value for valgrind support to find invalid memory reads. */
/* #undef SUPPORT_VALGRIND */

/* Version number of package */
#define VERSION "8.43"

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* Define to the type of a signed integer type of width exactly 64 bits if
   such a type exists and the standard includes do not define it. */
/* #undef int64_t */

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */
