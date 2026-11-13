#include <Interpreters/FileCache/QueueEntryType.h>
#include <base/EnumReflection.h>

namespace DB
{

String toString(QueueEntryType type)
{
    return String(magic_enum::enum_name(type));
}

}
