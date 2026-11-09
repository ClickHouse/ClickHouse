#include <unistd.h>
#ifdef DWYU_RESOLUTION_AFTER_SELECTED
#error "-idirafter unistd.h preceded Clang's implicit configured sysroot"
#endif
_Static_assert(_POSIX_VERSION >= 200809L, "configured sysroot unistd.h");
#include "unistd.h"
#include <unistd.h>
#include "unistd.h"
