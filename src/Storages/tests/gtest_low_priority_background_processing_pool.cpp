#include <gtest/gtest.h>
#include <Interpreters/Context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <iostream>
#include <Poco/Util/Application.h>
#include <daemon/BaseDaemon.h>

DB::BackgroundProcessingPoolTaskResult checkLow()
{
    int policy;
    sched_param param;
    if (pthread_getschedparam(pthread_self(), &policy, &param)) {
        return DB::BackgroundProcessingPoolTaskResult::ERROR;
    }

    if (policy != SCHED_IDLE) {
        return DB::BackgroundProcessingPoolTaskResult::ERROR;
    }
    return DB::BackgroundProcessingPoolTaskResult::SUCCESS;
}

DB::BackgroundProcessingPoolTaskResult odd(int *data)
{
    for (int i = 0; i < 10; ++i) {
        if (i % 2) {
            data[i] = i;
        }
    }
    return DB::BackgroundProcessingPoolTaskResult::SUCCESS;
}

DB::BackgroundProcessingPoolTaskResult even(int *data)
{
    for (int i = 0; i < 10; ++i) {
        if (!(i % 2)) {
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

TEST(LowPriorityBackgroundProcessingPool, SimpleCase)
{
    using namespace DB;
    const auto & context_holder = getContext();
    Context ctx = context_holder.context;

    ConfigurationPtr config = getConfigurationFromXMLString(minimal_default_user_xml);
    ctx.setConfig(config);

    int data[10];
    BackgroundProcessingPool & pool = ctx.getBackgroundLowPriorityPool();

    auto task_handle = pool.createTask([&data] { return odd(data); });
    pool.startTask(task_handle);
    auto even_handle = pool.createTask([&data] { return even(data); });
    pool.startTask(even_handle);
    auto check_low_handle = pool.createTask([] { return checkLow(); });
    pool.startTask(check_low_handle);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    pool.removeTask(task_handle);
    pool.removeTask(even_handle);
    pool.removeTask(check_low_handle);
    ctx.shutdown();

    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(i, data[i]) << "failed on " << i << " step\n";
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
