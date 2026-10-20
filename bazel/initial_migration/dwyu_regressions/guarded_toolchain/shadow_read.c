#include <features.h>
#include <features.h>
#ifndef DWYU_SHADOW_FEATURES
#error Ordinary -I must precede glibc's -isystem
#endif
int dwyu_read_shadow(void) { return DWYU_SHADOW_FEATURES; }
