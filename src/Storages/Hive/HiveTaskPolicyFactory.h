#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <boost/core/noncopyable.hpp>
#include <unordered_map>
#include <base/types.h>
namespace DB
{
class IHiveTaskFilesCollector;
class IHiveTaskIterateCallback;

class HiveTaskPolicyFactory : private boost::noncopyable
{
public:
    using IterateCallbackBuilder = std::function<std::shared_ptr<IHiveTaskIterateCallback>()>;
    using FilesCollectorBuilder = std::function<std::shared_ptr<IHiveTaskFilesCollector>()>;
    void registerBuilders(
        const String & policy_name_, IterateCallbackBuilder iterate_callback_builder_, FilesCollectorBuilder files_collector_builder_);

    static HiveTaskPolicyFactory & instance();

    inline std::shared_ptr<IHiveTaskIterateCallback> getIterateCallback(const String & policy_name_) const
    {
        auto iter = iterate_callback_builders.find(policy_name_);
        if (iter == iterate_callback_builders.end())
            return nullptr;
        return (iter->second)();
    }

    inline std::shared_ptr<IHiveTaskFilesCollector> getFilesCollector(const String & policy_name_) const
    {
        auto iter = files_collector_builders.find(policy_name_);
        if (iter == files_collector_builders.end())
            return nullptr;
        return (iter->second)();
    }

protected:
    HiveTaskPolicyFactory() = default;

private:
    std::unordered_map<String, IterateCallbackBuilder> iterate_callback_builders;
    std::unordered_map<String, FilesCollectorBuilder> files_collector_builders;
};

void registerHiveTaskPolices();

} // namespace DB
#endif
