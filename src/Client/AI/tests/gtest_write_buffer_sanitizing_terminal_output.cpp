#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/WriteBufferSanitizingTerminalOutput.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

/// Writes `input` through the sanitizing buffer, flushing it after each of the `splits` bytes, so
/// that a multi-byte sequence can be cut between two chunks.
String sanitize(std::string_view input, const std::vector<size_t> & splits = {})
{
    WriteBufferFromOwnString out;
    {
        WriteBufferSanitizingTerminalOutput sanitizing(out);
        size_t written = 0;
        for (size_t split : splits)
        {
            sanitizing.write(input.data() + written, split - written);
            sanitizing.next();
            written = split;
        }
        sanitizing.write(input.data() + written, input.size() - written);
        sanitizing.finalize();
    }
    return out.str();
}

}

TEST(WriteBufferSanitizingTerminalOutput, ReplacesControls)
{
    /// `ESC` and `BEL` become their Control Pictures, the line feed and the tab stay.
    EXPECT_EQ(sanitize("a\x1b]52;c;U0VDUkVU\x07\tb\n"), "a␛]52;c;U0VDUkVU␇\tb\n");
    EXPECT_EQ(sanitize("\x7f"), "␡");
    /// The C1 control CSI, a stray continuation byte, a byte that cannot start a sequence, and a
    /// sequence cut short become U+FFFD.
    EXPECT_EQ(sanitize("\xc2\x9b" "31m"), "�31m");
    EXPECT_EQ(sanitize("\x9b" "31m"), "�31m");
    EXPECT_EQ(sanitize("\xff"), "�");
    EXPECT_EQ(sanitize("\xe2\x82x"), "�x");
    EXPECT_EQ(sanitize("\xe2\x82"), "�");
    /// Printable text, multi-byte characters among it, is kept as it is.
    EXPECT_EQ(sanitize("plain € text \U0001F600  "), "plain € text \U0001F600  ");
}

TEST(WriteBufferSanitizingTerminalOutput, SequenceSplitBetweenChunks)
{
    /// Every split of the input gives the same output as the input written at once.
    const std::string input = "x\xc2\x9by€z\U0001F600\xe2\x82q\x1b[2J\xc3\xa9";
    const String expected = sanitize(input);
    EXPECT_EQ(expected, "x�y€z\U0001F600�q␛[2Jé");
    for (size_t split = 1; split < input.size(); ++split)
        EXPECT_EQ(sanitize(input, {split}), expected) << split;
    /// And so does a chunk per byte.
    std::vector<size_t> every_byte;
    for (size_t split = 1; split < input.size(); ++split)
        every_byte.push_back(split);
    EXPECT_EQ(sanitize(input, every_byte), expected);
}

#endif
