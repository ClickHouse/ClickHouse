#ifndef _ULIMIT_H
#define _ULIMIT_H

#ifdef __cplusplus
extern "C" {
#endif

#define UL_GETFSIZE 1
#define UL_SETFSIZE 2

long ulimit (int, ...);

#ifdef __cplusplus
}
#endif

#endif
