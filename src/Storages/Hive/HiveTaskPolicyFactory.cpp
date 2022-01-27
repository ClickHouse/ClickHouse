#include <Storages/Hive/HiveTaskPolicyFactory.h>
#if USE_HIVE
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int INSERT_WAS_DEDUPLICATED;
}

extern void registerHiveTaskNodeHashPolicy(HiveTaskPolicyFactory & factory_);

HiveTaskPolicyFactory & HiveTaskPolicyFactory::instance()
{
    static HiveTaskPolicyFactory instance;
    return instance;
}

void HiveTaskPolicyFactory::registerBuilders(
    const String & policy_name_, IterateCallbackBuilder iterate_callback_builder_, FilesCollectorBuilder files_collector_builder_)
{
    if (iterate_callback_builders.count(policy_name_) || files_collector_builders.count(policy_name_))
        throw Exception(ErrorCodes::INSERT_WAS_DEDUPLICATED, "Duplicated policy: {}", policy_name_);
    iterate_callback_builders[policy_name_] = iterate_callback_builder_;
    files_collector_builders[policy_name_] = files_collector_builder_;
}

void registerHiveTaskPolices()
{
    registerHiveTaskNodeHashPolicy(HiveTaskPolicyFactory::instance());
}

} // namespace DB
#endif
