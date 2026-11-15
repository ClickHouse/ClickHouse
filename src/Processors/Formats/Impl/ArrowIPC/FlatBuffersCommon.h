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

/// The FlatBuffers runtime stays at global scope (`::flatbuffers`): it is included first so the
/// re-includes inside the generated headers below are guarded no-ops, and the generated headers
/// reference it as `::flatbuffers` regardless.
#include <flatbuffers/flatbuffers.h>

#include <cstdint>

/// Apache Arrow's generated metadata headers declare everything in `org::apache::arrow::flatbuf` and
/// define inline accessor/verifier functions as weak symbols. The Apache Arrow C++ library (linked
/// into the same binary for the library-based formats and Arrow Flight) defines the exact same
/// symbols. Compiling them at their canonical scope here would let the linker select our copies for
/// Arrow's own code paths. Wrapping the generated headers in a private namespace gives our copies
/// distinct mangled names, so they cannot interpose on Arrow's. Only the generated layer is wrapped;
/// the `::flatbuffers` runtime it calls is global (and its scalar reads are inlined into each side's
/// own functions, so nothing is shared across the boundary).
namespace DB::ArrowIPC::arrow_fb
{
#include <generated/Message_generated.h>
#include <generated/Schema_generated.h>
#include <generated/File_generated.h>
}

namespace DB::ArrowIPC
{

/// Short alias for the generated FlatBuffers namespace used throughout the native implementation.
namespace flatbuf = arrow_fb::org::apache::arrow::flatbuf;

/// `flatbuffers::Verifier` checks that the length prefix of a vector is 4-byte aligned, but not that its
/// elements are aligned to their own size. A corrupted buffer can place a vector of 8-byte scalars or structs
/// (`RecordBatch.nodes`, `RecordBatch.buffers`, `RecordBatch.variadicBufferCounts`, `Footer.recordBatches`)
/// at an address that is 4 mod 8, and reading its elements would be a misaligned load. Check such vectors
/// after verification. The storage the buffer is read into is aligned by the allocator, so a vector of a
/// well-formed buffer passes. An empty vector has no elements to read, and writers do not align it (e.g. an
/// empty `Footer.dictionaries` written by Apache Arrow C++ 21.0.0), so it is accepted.
template <typename Vector>
bool isFlatBuffersVectorAligned8(const Vector * vector)
{
    return vector == nullptr || vector->size() == 0 || reinterpret_cast<uintptr_t>(vector->Data()) % 8 == 0;
}

}

#endif
