#include <gtest/gtest.h>

#include <IO/Ask.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

bool askFromString(const std::string & input, bool default_yes)
{
    ReadBufferFromString in(input);
    WriteBufferFromOwnString out;
    return ask("Sure? ", in, out, default_yes);
}

}

TEST(Ask, Answers)
{
    EXPECT_TRUE(askFromString("y\n", false));
    EXPECT_TRUE(askFromString("Y\n", false));
    EXPECT_FALSE(askFromString("n\n", true));
    EXPECT_FALSE(askFromString("N\n", true));

    /// An empty line (the user just pressing Enter) takes the default.
    EXPECT_TRUE(askFromString("\n", true));
    EXPECT_FALSE(askFromString("\n", false));

    /// An unrecognized answer is asked again.
    EXPECT_TRUE(askFromString("what\ny\n", false));
    EXPECT_FALSE(askFromString("what\nn\n", true));
}

TEST(Ask, EOFFailsClosed)
{
    /// EOF (e.g. Ctrl+D) means the input was aborted, not answered: it must not act as the
    /// default, especially when the default is "yes" (a confirmation prompt of the AI agent
    /// must not run the query when the user terminates the input).
    EXPECT_FALSE(askFromString("", true));
    EXPECT_FALSE(askFromString("", false));

    /// An unrecognized answer followed by EOF.
    EXPECT_FALSE(askFromString("what\n", true));
}
