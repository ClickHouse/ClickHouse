#pragma once

#include "config.h"

#if USE_ARROW

/// Single place that pulls in the FlatBuffers-generated Arrow IPC metadata headers.
///
/// These headers (`Message_generated.h`, `Schema_generated.h`, `File_generated.h`) are produced
/// from the Arrow `*.fbs` definitions and depend ONLY on the FlatBuffers library, not on the
/// Apache Arrow C++ runtime. That is exactly what lets the native Arrow IPC reader and writer
/// parse and build the IPC metadata without linking the Arrow library: the buffer payloads are
/// decoded and encoded directly to and from ClickHouse columns.
///
/// The include base `contrib/arrow/cpp/src` is provided by the `ch_contrib::arrow_ipc_metadata`
/// CMake interface target, so `<generated/Message_generated.h>` resolves here and the headers'
/// own relative includes (e.g. `"Schema_generated.h"`) resolve next to them.

#include <flatbuffers/flatbuffers.h>
#include <generated/Message_generated.h>
#include <generated/Schema_generated.h>
#include <generated/File_generated.h>

namespace DB::ArrowIPC
{

/// Short alias for the generated FlatBuffers namespace used throughout the native implementation.
namespace flatbuf = org::apache::arrow::flatbuf;

}

#endif
