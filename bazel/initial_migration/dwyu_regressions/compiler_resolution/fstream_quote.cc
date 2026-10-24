#include <fstream>
#include "unistd.h"
#include "fcntl.h"
#include "errno.h"
#include "unistd.h"
#include "fcntl.h"
#include "errno.h"

static_assert(_POSIX_VERSION >= 200809L);
static_assert(O_RDONLY == 0);
static_assert(EINVAL != 0);
static_assert(sizeof(std::filebuf) != 0);
