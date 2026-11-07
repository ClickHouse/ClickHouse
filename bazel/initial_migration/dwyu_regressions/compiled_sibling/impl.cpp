#ifdef DWYU_SIBLING_READER
#include "api.h"
static int dwyu_sibling_value() { return DWYU_SIBLING_VALUE; }
#else
int dwyu_sibling_impl() { return 29; }
#endif
