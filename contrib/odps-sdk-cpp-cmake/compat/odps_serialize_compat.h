#pragma once

/// Compatibility prelude used only by `tunnel/serialize.cpp`.
///
/// protobuf removed the warning-threshold argument from
/// `CodedInputStream::SetTotalBytesLimit`. Parse protobuf's declaration before
/// rewriting the old SDK call to its supported one-argument form.
#include <google/protobuf/io/coded_stream.h>

#define ODPS_COMPAT_FIRST_ARG(first, ...) first
#define SetTotalBytesLimit(...) SetTotalBytesLimit(ODPS_COMPAT_FIRST_ARG(__VA_ARGS__, 0))

/// `serialize.cpp` also uses raw zlib constants that modern protobuf no longer
/// exposes transitively.
#include <zlib.h>
