#include <Common/Wyrd/Testing/Test.h>

#include <fmt/format.h>

#include <string>
#include <string_view>
#include <utility>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

namespace
{

#if defined(WYRD_SIMULATION)
constexpr std::string_view strategy_name = "Simulation";
#elif defined(WYRD_ADVERSARY)
constexpr std::string_view strategy_name = "Adversary";
#else
constexpr std::string_view strategy_name = "Plain";
#endif

class RegisteredTest : public testing::Test
{
public:
    RegisteredTest(std::function<void()> body_, TestParams params_)
        : body(std::move(body_))
        , params(params_)
    {
    }

    void TestBody() override
    {
        runTest(body, params);
    }

private:
    std::function<void()> body;
    TestParams params;
};

}

bool registerTest(const char * suite, const char * name, const char * file, int line, std::function<void()> body, TestParams params)
{
    const std::string test_name = fmt::format("{}/{}", name, strategy_name);
    testing::RegisterTest(
        suite, test_name.c_str(), nullptr, nullptr, file, line,
        [body, params]() -> testing::Test * { return new RegisteredTest(body, params); });
    return true;
}

}
