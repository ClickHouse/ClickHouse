#include <Storages/Hive/HiveSourceTask.h>

namespace DB
{
String pruneLevelToString(HivePruneLevel level) { return String(magic_enum::enum_name(level)); }
}
