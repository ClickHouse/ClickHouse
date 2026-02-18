#pragma once
#include <base/types.h>
#include <map>

namespace DB
{

/// Common parts of PackedFilesReader and PackedFilesWriter.
namespace PackedFilesIO
{

static constexpr UInt8 VERSION = 0;
static constexpr auto ARCHIVE_EXTENSION = ".packed";

struct FileOffset
{
    UInt64 offset;
    UInt64 size;
};

using Index = std::map<String, FileOffset>;

}

}
