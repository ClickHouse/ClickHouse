#include <mutex>
#include <Storages/Hive/HiveSourceTask.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String pruneLevelToString(HivePruneLevel level) { return String(magic_enum::enum_name(level)); }

HiveSourceCollectCallbackFactory & HiveSourceCollectCallbackFactory::instance()
{
    static HiveSourceCollectCallbackFactory instance;
    static std::once_flag once_flag;
    std::call_once(once_flag, []() { HiveSourceCollectCallbackFactory::registerBuilders(instance); });

    return instance;
}

void registerNodeHashHiveSourceFilesCollectCallback(HiveSourceCollectCallbackFactory & factory);
void HiveSourceCollectCallbackFactory::registerBuilders(HiveSourceCollectCallbackFactory & factory)
{
    registerNodeHashHiveSourceFilesCollectCallback(factory);
}

void HiveSourceCollectCallbackFactory::registerBuilder(const String & name, HiveSourceFilesCollectCallbackBuilder builder)
{
    auto it = builders.find(name);
    if (it != builders.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicated builder ({})", name);
    builders[name] = builder;
}

HiveSourceCollectorFactory & HiveSourceCollectorFactory::instance()
{
    static HiveSourceCollectorFactory instance;
    static std::once_flag once_flag;
    std::call_once(once_flag, []() { HiveSourceCollectorFactory::registerBuilders(instance); });
    return instance;
}

void registerLocalHiveSourceFilesCollector(HiveSourceCollectorFactory & factory);
void registerNodeHashHiveSourceFilesCollector(HiveSourceCollectorFactory & factory);
void HiveSourceCollectorFactory::registerBuilders(HiveSourceCollectorFactory & factory)
{
    registerLocalHiveSourceFilesCollector(factory);
    registerNodeHashHiveSourceFilesCollector(factory);
}

void HiveSourceCollectorFactory::registerBuilder(const String & name, HiveSourceFilesCollectorBuilder builder)
{
    auto it = builders.find(name);
    if (it != builders.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicated builder ({})", name);
    builders[name] = builder;
}

}
