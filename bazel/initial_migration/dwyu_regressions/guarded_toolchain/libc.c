#include <features.h>
#include <features.h>
#include <stdlib.h>
#include <stdio.h>
#include <features.h>

#if !defined(__GLIBC__) || __GLIBC__ != 2
#error Expected the configured glibc header
#endif
#ifdef DWYU_SHADOW_FEATURES
#error A later project system directory must not replace glibc
#endif
int dwyu_glibc_c(void) { return __GLIBC__; }
