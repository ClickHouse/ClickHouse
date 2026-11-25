#include <Common/AsynchronousMetrics.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

AsynchronousMetricKeyValues parse(std::string_view text)
{
    ReadBufferFromString in(text);
    return readCPUFrequenciesFromProcCPUInfo(in);
}

}

TEST(AsynchronousMetricsCPUInfo, ParsesFrequencies)
{
    EXPECT_EQ(
        parse("processor\t: 0\nvendor_id\t: GenuineIntel\ncpu MHz\t\t: 1000.000\n\nprocessor\t: 1\ncpu MHz\t\t: 800.500\n"),
        (AsynchronousMetricKeyValues{{"0", 1000}, {"1", 800.5}}));

    /// s390x
    EXPECT_EQ(
        parse("processor 3: version = FF, identification = 039C88, machine = 3906\ncpu MHz dynamic : 5200\n"),
        (AsynchronousMetricKeyValues{{"3", 5200}}));

    /// The last line has no EOL.
    EXPECT_EQ(parse("processor\t: 2\ncpu MHz\t\t: 1000"), (AsynchronousMetricKeyValues{{"2", 1000}}));

    EXPECT_EQ(parse(""), AsynchronousMetricKeyValues{});
}

TEST(AsynchronousMetricsCPUInfo, SkipsLinesThatDoNotParse)
{
    const std::string core0 = "processor\t: 0\ncpu MHz\t\t: 1.5\n";

    for (const char * line : {"cpu MHz\t\t 1234", "cpu MHz\t\t: ", "cpu MHz\t\t:", "cpu MHz\t\t: abc"})
        EXPECT_EQ(parse(core0 + "processor\t: 1\n" + line + "\n"), (AsynchronousMetricKeyValues{{"0", 1.5}})) << line;

    /// The frequency after a `processor` line that does not parse is not attributed to the previous core.
    for (const char * line : {"processor\t: ", "processor\t: 99999999999"})
        EXPECT_EQ(parse(core0 + line + "\ncpu MHz\t\t: 2.5\n"), (AsynchronousMetricKeyValues{{"0", 1.5}})) << line;
}
