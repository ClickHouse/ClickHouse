#if defined(WYRD_ENABLED)

#include <Common/Wyrd/Testing/Test.h>

#include <Common/ThreadPool.h>

#include <base/sanitizer_options.h>

#include <fmt/format.h>

#include <string>
#include <utility>

namespace DB::Wyrd
{

namespace
{

using Runner = void (*)(std::function<void()> body, TestParams params);

class RegisteredTest : public testing::Test
{
public:
    RegisteredTest(Runner runner_, std::function<void()> body_, TestParams params_)
        : runner(runner_)
        , body(std::move(body_))
        , params(params_)
    {
    }

    void TestBody() override
    {
        runner(body, params);
    }

private:
    Runner runner;
    std::function<void()> body;
    TestParams params;
};

void registerTestOfMode(const char * suite, const char * name, const char * mode_name, const char * file, int line, Runner runner, std::function<void()> body, TestParams params)
{
    const std::string test_name = fmt::format("{}/{}", name, mode_name);
    testing::RegisterTest(
        suite, test_name.c_str(), nullptr, nullptr, file, line,
        [runner, body, params]() -> testing::Test * { return new RegisteredTest(runner, body, params); });
}

}

bool registerTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params)
{
    registerAdversaryTest(suite, name, file, line, body, params);
    registerSimulationTest(suite, name, file, line, std::move(body), params);
    return true;
}

bool registerAdversaryTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params)
{
    registerTestOfMode(suite, name, "adversary", file, line, runAdversaryTest, std::move(body), params);
    return true;
}

bool registerSimulationTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params)
{
    registerTestOfMode(suite, name, "simulation", file, line, runSimulationTest, std::move(body), params);
    return true;
}

}

int main(int argc, char ** argv)
{
    testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    GlobalThreadPool::shutdown();
    return result;
}

#endif
