#define UNW_LOCAL_ONLY
#include "config.h"
#include <libunwind.h>
#if defined(UNW_LOCAL_ONLY) && !defined(UNW_REMOTE_ONLY)
#include "Gglobal.c"
#endif
