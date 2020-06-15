#include <gtest/gtest.h>
#include <Interpreters/Context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <iostream>
#include <Poco/Util/Application.h>
#include <daemon/BaseDaemon.h>

DB::BackgroundProcessingPoolTaskResult checkLow(std::vector<bool>& checks, int i, std::mutex& mut)
{
    int policy;
    sched_param param;
    if (pthread_getschedparam(pthread_self(), &policy, &param))
    {
        mut.lock();
        checks[i] = false;
        mut.unlock();

        return DB::BackgroundProcessingPoolTaskResult::ERROR;
    }

    if (policy != SCHED_IDLE)
    {
        mut.lock();
        checks[i] = false;
        mut.unlock();

        return DB::BackgroundProcessingPoolTaskResult::ERROR;
    }
    return DB::BackgroundProcessingPoolTaskResult::SUCCESS;
}

DB::BackgroundProcessingPoolTaskResult odd(int *data)
{
    for (int i = 0; i < 10; ++i)
    {
        if (i % 2)
        {
            data[i] = i;
        }
    }
    return DB::BackgroundProcessingPoolTaskResult::SUCCESS;
}

DB::BackgroundProcessingPoolTaskResult even(int *data)
{
    for (int i = 0; i < 10; ++i)
    {
        if (!(i % 2))
        {
            data[i] = i;
        }
    }
    return DB::BackgroundProcessingPoolTaskResult::SUCCESS;
}

static const char * minimal_default_user_xml =
"<yandex>"
"    <profiles>"
"        <default></default>"
"    </profiles>"
"    <users>"
"        <default>"
"            <password></password>"
"            <networks>"
"                <ip>::/0</ip>"
"            </networks>"
"            <profile>default</profile>"
"            <quota>default</quota>"
"        </default>"
"    </users>"
"    <quotas>"
"        <default></default>"
"    </quotas>"
"</yandex>";

static DB::ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

TEST(BackgroundLowPriorityProcessingPool, SimpleCase)
{
    using namespace DB;
    const auto & context_holder = getContext();
    Context ctx = context_holder.context;

    ConfigurationPtr config = getConfigurationFromXMLString(minimal_default_user_xml);
    ctx.setConfig(config);

    int data[10];
    BackgroundProcessingPool & pool = ctx.getBackgroundLowPriorityPool();

    auto task_handle = pool.createTask([&data] { return odd(data); });
    pool.startTask(task_handle, false);
    auto even_handle = pool.createTask([&data] { return even(data); });
    pool.startTask(even_handle, false);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    pool.removeTask(task_handle);
    pool.removeTask(even_handle);
    for (int i = 0; i < 10; ++i)
    {
        ASSERT_EQ(i, data[i]) << "failed on " << i << " step\n";
    }

    std::mutex mut;
    std::vector<bool> checks(16, true);
    std::vector<BackgroundProcessingPool::TaskHandle> handles(16);
    for (int i = 0; i < 16; ++i)
    {
        handles[i] = pool.createTask([&] { return checkLow(checks, i, mut); });
        pool.startTask(handles[i]);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    for (int i = 0; i < 16; ++i)
    {
        pool.removeTask(handles[i]);
    }
    ctx.shutdown();

    for (bool check_idle : checks)
    {
        ASSERT_TRUE(check_idle) << "failed on low priority checks\n";
    }

}

