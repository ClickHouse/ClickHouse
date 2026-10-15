#include "config.h"

#if USE_XRAY

#include <Interpreters/InstrumentationManager.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(InstrumentationManager, shouldPatchFunction)
{
    ASSERT_TRUE(InstrumentationManager::shouldPatchFunction("QueryMetricLog::startQuery",
        "DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)"));

    ASSERT_FALSE(InstrumentationManager::shouldPatchFunction("QueryMetricLog::startQuery",
        "std::__1::__function::__policy_func<void ()>::__call_func[abi:ne210105]<DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)::$_0>(std::__1::__function::__policy_storage const*)"));

    ASSERT_TRUE(InstrumentationManager::shouldPatchFunction("QueryMetricLog::startQuery",
        "DB::something<bool>::otherClass<MyClass>::QueryMetricLog::startQuery"));
}

#endif
