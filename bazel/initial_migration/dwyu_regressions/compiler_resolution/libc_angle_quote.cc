#include <unistd.h>
static_assert(_POSIX_VERSION >= 200809L, "configured sysroot unistd.h");
#include "unistd.h"
#include <unistd.h>
#include "unistd.h"
static_assert(_POSIX_VERSION >= 200809L, "both spellings use configured sysroot");
