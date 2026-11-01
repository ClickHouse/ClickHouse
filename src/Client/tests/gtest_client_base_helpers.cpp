#include <gtest/gtest.h>

#include <Client/ClientBaseHelpers.h>

#include "config.h"

#if USE_REPLXX

#include <algorithm>
#include <vector>

namespace DB
{
namespace
{

constexpr std::string_view background_commands[]{"\\bg", "\\cancel", "\\fg", "\\jobs"};

TEST(ClientBaseHelpers, HighlightsInteractiveClientCommands)
{
    const auto command_color = replxx::color::bold(replxx::Replxx::Color::DEFAULT);

    for (const auto command : background_commands)
    {
        const String query(command);
        std::vector colors(query.size(), replxx::Replxx::Color::DEFAULT);
        ASSERT_TRUE(highlightClientCommand(query, colors, background_commands));
        EXPECT_TRUE(std::ranges::all_of(colors, [&](const auto color) { return color == command_color; }));
    }
}

TEST(ClientBaseHelpers, MatchesCommandsCaseInsensitivelyWithWhitespaceAndSemicolon)
{
    const String query = "  \\JOBS;";
    std::vector colors(query.size(), replxx::Replxx::Color::DEFAULT);
    ASSERT_TRUE(highlightClientCommand(query, colors, background_commands));

    const auto command_color = replxx::color::bold(replxx::Replxx::Color::DEFAULT);
    EXPECT_EQ(colors[0], replxx::Replxx::Color::DEFAULT);
    EXPECT_EQ(colors[1], replxx::Replxx::Color::DEFAULT);
    EXPECT_TRUE(std::ranges::all_of(colors.begin() + 2, colors.end() - 1, [&](const auto color) { return color == command_color; }));
    EXPECT_EQ(colors.back(), replxx::Replxx::Color::DEFAULT);
}

TEST(ClientBaseHelpers, LeavesUnknownCommandsForTheSqlHighlighter)
{
    for (const String query : {"\\job", "\\jobs_extra", "\\jobs; SELECT 1"})
    {
        std::vector colors(query.size(), replxx::Replxx::Color::DEFAULT);
        EXPECT_FALSE(highlightClientCommand(query, colors, background_commands));
        EXPECT_TRUE(std::ranges::all_of(colors, [](const auto color) { return color == replxx::Replxx::Color::DEFAULT; }));
    }
}

}
}

#endif
