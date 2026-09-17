#pragma once

#include <base/types.h>

namespace DB
{

enum class FileSegmentKeyType : uint8_t
{
    General = 0,
    System, // Segment for (metadata, index, marks, etc.)
    Data, // Segment for table data
};

String getKeyTypePrefix(FileSegmentKeyType type);
String toString(FileSegmentKeyType type);

}
