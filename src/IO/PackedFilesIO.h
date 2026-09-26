#pragma once
#include <Common/MapWithMemoryTracking.h>
#include <base/types.h>
#include <map>

namespace DB
{

/// Common parts of PackedFilesReader and PackedFilesWriter.
namespace PackedFilesIO
{

/// Version is chosen per archive by the writer, not globally: skp_idx.packed writes v1 to carry
/// uncompressed_size, statistics.packed keeps writing v0. MAX_VERSION is only the read-side
/// ceiling (readIndex rejects greater); writers must pass one of the explicit versions below.
static constexpr UInt8 MAX_VERSION = 1;
static constexpr UInt8 VERSION_WITHOUT_UNCOMPRESSED_SIZE = 0;
static constexpr UInt8 VERSION_WITH_UNCOMPRESSED_SIZE = 1;
static constexpr auto ARCHIVE_EXTENSION = ".packed";

struct FileOffset
{
    UInt64 offset = 0;
    UInt64 size = 0;
    /// Recorded only in v1+; 0 means unknown.
    UInt64 uncompressed_size = 0;
};

using Index = MapWithMemoryTracking<String, FileOffset>;

}

}
