#ifndef _UCHAR_H
#define _UCHAR_H

#ifdef __cplusplus
extern "C" {
#endif

#if __cplusplus < 201103L
typedef unsigned short char16_t;
typedef unsigned char32_t;
#endif

#define __NEED_mbstate_t
#define __NEED_size_t

#include <features.h>
#include <bits/alltypes.h>

size_t c16rtomb(char *__restrict, char16_t, mbstate_t *__restrict);
size_t mbrtoc16(char16_t *__restrict, const char *__restrict, size_t, mbstate_t *__restrict);

size_t c32rtomb(char *__restrict, char32_t, mbstate_t *__restrict);
size_t mbrtoc32(char32_t *__restrict, const char *__restrict, size_t, mbstate_t *__restrict);

#ifdef __cplusplus
}
#endif

#endif
