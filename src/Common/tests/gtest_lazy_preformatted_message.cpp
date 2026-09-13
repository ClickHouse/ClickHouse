#include <Common/LazyPreformattedMessage/api.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

using namespace DB;

TEST(LazyPreformattedMessage, SameTextAsPreformatted)
{
    std::string name = "all_1_1_0";
    size_t level = 3;

    auto lazy = createLazyMessage("Part {} has level {}", refArg(name), copyArg(level));
    auto eager = PreformattedMessage::create("Part {} has level {}", name, level);

    EXPECT_EQ(lazy.format(), eager.text);
    EXPECT_EQ(lazy.format(), eager.text);
}

TEST(LazyPreformattedMessage, RefSeesChangesCopyDoesNot)
{
    std::string ref = "a";
    std::string copy = "b";

    auto message = createLazyMessage("{} {}", refArg(ref), copyArg(copy));
    ref = "x";
    copy = "y";

    EXPECT_EQ(message.format(), "x b");
}

TEST(LazyPreformattedMessage, Move)
{
    int value = 42;
    auto source = createLazyMessage("{}", copyArg(value));
    auto moved = std::move(source);

    EXPECT_EQ(moved.format(), "42");
}

TEST(LazyPreformattedMessage, Lanes)
{
    std::vector<LazyPreformattedMessage::Message> alive;
    for (size_t i = 0; i < 16; ++i)
        alive.push_back(createLazyMessage("{}", copyArg(i)));

    for (size_t i = 0; i < 16; ++i)
        EXPECT_EQ(alive[i].format(), std::to_string(i));

    alive.clear();
    EXPECT_EQ(createLazyMessage("{}", copyArg(0)).format(), "0");
}
