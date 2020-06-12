/*
 * Public domain
 * limits.h compatibility shim
 */

#ifdef _MSC_VER
#include <../include/limits.h>
#if _MSC_VER >= 1900
#include <../ucrt/stdlib.h>
#else
#include <../include/stdlib.h>
#endif
#ifndef PATH_MAX
#define PATH_MAX _MAX_PATH
#endif
#else
#include_next <limits.h>
#endif

#ifdef __hpux
#include <sys/param.h>
#ifndef PATH_MAX
#define PATH_MAX MAXPATHLEN
#endif
#endif
