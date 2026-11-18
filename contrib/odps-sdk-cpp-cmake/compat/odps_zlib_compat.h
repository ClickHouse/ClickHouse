#pragma once

/// `util/gzip_util.cpp` uses raw zlib constants that modern protobuf no longer
/// exposes transitively.
#include <zlib.h>
