#include <unistd.h>
#ifdef DWYU_RESOLUTION_ONCE_LOCAL
#error "angle lookup must select the real configured sysroot, not the local unistd.h"
#endif
_Static_assert(_POSIX_VERSION >= 200809L, "configured sysroot unistd.h");
#include "../primer/once.h"
_Static_assert(DWYU_RESOLUTION_ONCE_LOCAL == 137, "the bridge must prime the distinct local header");
#include "api.inc"
