#pragma once

#include "config.h"

#if USE_LIBARCHIVE

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"

#include <archive.h>
#include <archive_entry.h>
#endif
#endif
