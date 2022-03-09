#pragma once
#include <Common/config.h>
#if USE_HIVE
#include <boost/core/noncopyable.hpp>
#include <unordered_map>
#include <base/types.h>
#include <Storages/Hive/HiveQueryTask.h>
namespace DB
{

class HiveQueryTaskBuilderFactory : private boost::noncopyable
{
public:
    using TaskBuilders = std::function<std::shared_ptr<DistributedHiveQueryTaskBuilder>()>;
    void registerBuilder(const String & name, DistributedHiveQueryTaskBuilderPtr builder);

    static HiveQueryTaskBuilderFactory & instance();

    inline HiveQueryTaskIterateCallbackPtr getIterateCallback(const String & policy_name_) const
    {
        auto iter = builders.find(policy_name_);
        if (iter == builders.end())
            return nullptr;
        return (iter->second->task_iterator_callback)();
    }

    inline HiveQueryTaskFilesCollectorPtr getFilesCollector(const String & policy_name_) const
    {
        auto iter = builders.find(policy_name_);
        if (iter == builders.end())
            return nullptr;
        return (iter->second->files_collector)();
    }

protected:
    HiveQueryTaskBuilderFactory() = default;

private:
    std::unordered_map<String, DistributedHiveQueryTaskBuilderPtr> builders;
};

void registerHiveQueryTaskBuilders();

}
#endif
