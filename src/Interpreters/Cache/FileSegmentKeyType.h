#pragma once
#include <cctype>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <base/EnumReflection.h>

namespace DB
{
    enum class FileSegmentKeyType : uint8_t
    {
        General = 0,
        System, // Segment for (metadata, index, marks, etc.)
        Data, // Segment for table data
    };

    inline String getKeyTypePrefix(FileSegmentKeyType type)
    {
        if (type == FileSegmentKeyType::General)
            return "";
        return String(magic_enum::enum_name(type));
    }

    inline String toString(FileSegmentKeyType type)
    {
        return String(magic_enum::enum_name(type));
    }
}
