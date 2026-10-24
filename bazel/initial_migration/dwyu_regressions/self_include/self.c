#if !defined(DWYU_SELF_FUNCTION)
#define DWYU_SELF_FUNCTION dwyu_ansi
#include "self.c"
#undef DWYU_SELF_FUNCTION
#define DWYU_SELF_FUNCTION dwyu_wide
#include "self.c"
#undef DWYU_SELF_FUNCTION
#else
#include "api/api.h"
int DWYU_SELF_FUNCTION(void)
{
    return DWYU_SELF_API_VALUE;
}
#endif
