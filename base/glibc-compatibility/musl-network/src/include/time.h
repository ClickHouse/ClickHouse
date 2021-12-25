#ifndef TIME_H
#define TIME_H

#include "../../include/time.h"

hidden int __clock_gettime(clockid_t, struct timespec *);
hidden int __clock_nanosleep(clockid_t, int, const struct timespec *, struct timespec *);

hidden char *__asctime_r(const struct tm *, char *);
hidden struct tm *__gmtime_r(const time_t *restrict, struct tm *restrict);
hidden struct tm *__localtime_r(const time_t *restrict, struct tm *restrict);

hidden size_t __strftime_l(char *restrict, size_t, const char *restrict, const struct tm *restrict, locale_t);

#endif
