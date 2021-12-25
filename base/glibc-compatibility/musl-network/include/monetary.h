#ifndef _MONETARY_H
#define _MONETARY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>

#define __NEED_ssize_t
#define __NEED_size_t
#define __NEED_locale_t

#include <bits/alltypes.h>

ssize_t strfmon(char *__restrict, size_t, const char *__restrict, ...);
ssize_t strfmon_l(char *__restrict, size_t, locale_t, const char *__restrict, ...);

#ifdef __cplusplus
}
#endif

#endif
