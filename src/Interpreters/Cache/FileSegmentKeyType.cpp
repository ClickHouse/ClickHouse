#include <Interpreters/Cache/FileSegmentKeyType.h>
#include <base/EnumReflection.h>

namespace DB
{

String getKeyTypePrefix(FileSegmentKeyType type)
{
    if (type == FileSegmentKeyType::General)
        return "";
    return String(magic_enum::enum_name(type));
}

String toString(FileSegmentKeyType type)
{
    return String(magic_enum::enum_name(type));
}

}
