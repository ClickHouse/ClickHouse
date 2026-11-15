#pragma once

#include "config.h"

#if USE_LIBURING

#include <Interpreters/Context_fwd.h>
#include <Disks/IO/IOUringReader.h>
#include <memory>

namespace DB
{

std::unique_ptr<IOUringReader> createIOUringReader();

IOUringReader & getIOUringReaderOrThrow(ContextPtr);

IOUringReader & getIOUringReaderOrThrow();

}
#endif
