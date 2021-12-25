#ifndef _STDALIGN_H
#define _STDALIGN_H

#ifndef __cplusplus

/* this whole header only works in C11 or with compiler extensions */
#if __STDC_VERSION__ < 201112L && defined( __GNUC__)
#define _Alignas(t) __attribute__((__aligned__(t)))
#define _Alignof(t) __alignof__(t)
#endif

#define alignas _Alignas
#define alignof _Alignof

#endif

#define __alignas_is_defined 1
#define __alignof_is_defined 1

#endif
