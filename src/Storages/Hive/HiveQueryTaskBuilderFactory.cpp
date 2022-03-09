#include <Storages/Hive/HiveQueryTaskBuilderFactory.h>
#if USE_HIVE
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int INSERT_WAS_DEDUPLICATED;
}

extern void registerHiveTaskNodeHashPolicy(HiveQueryTaskBuilderFactory & factory_);

HiveQueryTaskBuilderFactory & HiveQueryTaskBuilderFactory::instance()
{
    static HiveQueryTaskBuilderFactory instance;
    return instance;
}

void HiveQueryTaskBuilderFactory::registerBuilder(const String & name, DistributedHiveQueryTaskBuilderPtr builder)
{
    if (builders.count(name))
        throw Exception(ErrorCodes::INSERT_WAS_DEDUPLICATED, "Duplicated policy: {}", name);
    builders[name] = std::move(builder);
}

void registerHiveQueryTaskBuilders()
{
    registerHiveTaskNodeHashPolicy(HiveQueryTaskBuilderFactory::instance());
}

}
#endif
