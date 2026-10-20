#include <unistd.h>
#ifdef DWYU_RESOLUTION_GUARDED_LOCAL
#error "angle lookup must select the real configured sysroot, not the local unistd.h"
#endif
_Static_assert(_POSIX_VERSION >= 200809L, "configured sysroot unistd.h");
#include "../primer/guarded.h"
_Static_assert(DWYU_RESOLUTION_GUARDED_LOCAL == 131, "the bridge must prime the distinct local header");
#include "api.inc"
