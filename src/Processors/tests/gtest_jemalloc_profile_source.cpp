#include "config.h"

#if USE_JEMALLOC

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include <Columns/ColumnString.h>
#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeString.h>
#include <IO/readIntText.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/JemallocProfileSource.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

/// Subset of a real jemalloc heap_v2 profile with sampling interval 524288.
/// Contains 7 stacks with varying allocation sizes to exercise the Poisson correction.
constexpr std::string_view test_heap_profile = R"(heap_v2/524288
  t*: 162: 11166737 [0: 0]
  t0: 0: 0 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08714 0x191dbd6c 0x17793e14 0x1f4d5b48 0x131ecee0 0x1e449e5c 0x131d4118 0x131d10e8 0xcac9260 0xffff9db37400 0xffff9db374d8
  t*: 1: 5121 [0: 0]
  t0: 1: 5121 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08714 0x191dbd6c 0x1979221c 0x197aee08 0x1321bb7c 0x1e4268d8 0x1e426ed4 0x1e3dacfc 0x1e3d8e30 0xffff9db90398 0xffff9dbf9e9c
  t*: 25: 128032 [0: 0]
  t3127: 25: 128032 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08714 0xb3c8080 0x11c34d60 0x169b0bf4 0x169b4c74 0x169cac24 0x12f7901c 0xffff9db90398 0xffff9dbf9e9c
  t*: 1: 128 [0: 0]
  t3821: 1: 128 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08914 0x17a7f00c 0x17e5c7fc 0x17e5a53c 0x1321bb7c 0x1e4268d8 0x1e426ed4 0x1e3dacfc 0x1e3d8e30 0xffff9db90398 0xffff9dbf9e9c
  t*: 3: 247015 [0: 0]
  t2993: 1: 82338 [0: 0]
  t3009: 1: 82338 [0: 0]
  t3196: 1: 82338 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08914 0x17a7f00c 0x17e5c7fc 0x17e64fb8 0x197bd8d0 0x1321bb7c 0x1e4268d8 0x1e426ed4 0x1e3dacfc 0x1e3d8e30 0xffff9db90398 0xffff9dbf9e9c
  t*: 2: 164677 [0: 0]
  t3195: 1: 82338 [0: 0]
  t3211: 1: 82338 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08714 0x12ed7044 0x12f7af00 0x12f721b4 0x12f7901c 0xffff9db90398 0xffff9dbf9e9c
  t*: 129: 10621636 [0: 0]
  t588: 1: 82338 [0: 0]
  t597: 1: 82338 [0: 0]
  t598: 1: 82338 [0: 0]
@ 0x1e51c218 0x1e51c118 0x1e50731c 0x1e49c720 0x12e08714 0x12ed7044 0xaaa0000 0xbbb0000 0xffff9db90398 0xffff9dbf9e9c
  t*: 1: 128 [0: 0]
  t100: 1: 128 [0: 0]
)";

/// Raw t* bytes sum: 11,166,737
/// Expected Poisson-corrected bytes total (matching jeprof AdjustSamples): 90,662,471
/// Computed with: scale = 1/(1-exp(-mean_size/524288)) per stack, then sum(bytes * scale).
constexpr UInt64 expected_raw_total = 11166737;
constexpr UInt64 expected_corrected_total = 90662471;

/// Raw t* count sum: 162
/// Expected Poisson-corrected count total (matching jeprof AdjustSamples): 11,792
/// Computed with: scale = 1/(1-exp(-mean_size/524288)) per stack, then sum(count * scale).
constexpr UInt64 expected_raw_count_total = 162;
constexpr UInt64 expected_corrected_count_total = 11792;

std::string writeToTempFile(std::string_view content, const std::string & name)
{
    auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream out(path);
    out.write(content.data(), content.size());
    return path.string();
}

UInt64 sumCollapsedValues(std::string_view collapsed_output)
{
    UInt64 total = 0;
    while (!collapsed_output.empty())
    {
        auto nl = collapsed_output.find('\n');
        auto line = collapsed_output.substr(0, nl);
        collapsed_output.remove_prefix(nl == std::string_view::npos ? collapsed_output.size() : nl + 1);

        auto last_space = line.rfind(' ');
        if (last_space != std::string_view::npos)
            total += parseInt<UInt64>(line.substr(last_space + 1));
    }
    return total;
}

/// Run JemallocProfileSource in collapsed mode with the given collapsed_use_count flag
/// and collect the output as a string.
std::string runCollapsedProfile(const std::string & input_filename, bool collapsed_use_count)
{
    Block header;
    header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "line"});
    auto source = std::make_shared<JemallocProfileSource>(
        input_filename,
        std::make_shared<const Block>(std::move(header)),
        DEFAULT_BLOCK_SIZE,
        JemallocProfileFormat::Collapsed,
        false, /* symbolize_with_inline */
        collapsed_use_count);

    std::string output;
    WriteBufferFromString out(output);
    QueryPipeline pipeline(std::move(source));
    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
    {
        const auto & column = assert_cast<const ColumnString &>(*block.getByPosition(0).column);
        for (size_t i = 0; i < column.size(); ++i)
        {
            auto sv = column.getDataAt(i);
            out.write(sv.data(), sv.size());
            writeChar('\n', out);
        }
    }
    out.finalize();
    return output;
}

}

/// Write a real jemalloc heap_v2 profile to disk, process it through
/// symbolizeJemallocHeapProfile in collapsed mode, and verify the total
/// matches the expected Poisson sampling correction.
TEST(JemallocProfileSource, CollapsedSamplingCorrection)
{
    auto input = writeToTempFile(test_heap_profile, "gtest_jemalloc_profile.heap");

    auto collapsed_output = symbolizeJemallocHeapProfileToString(input, JemallocProfileFormat::Collapsed, false);

    UInt64 total = sumCollapsedValues(collapsed_output);

    /// Allow +-1 per stack for floating-point rounding (7 stacks).
    EXPECT_NEAR(static_cast<double>(total), static_cast<double>(expected_corrected_total), 7.0);
    EXPECT_GT(total, expected_raw_total);

    std::filesystem::remove(input);
}

/// Same heap_v2 profile, but verify the Poisson correction for count mode
/// (collapsed_use_count = true).
TEST(JemallocProfileSource, CollapsedSamplingCorrectionCount)
{
    auto input = writeToTempFile(test_heap_profile, "gtest_jemalloc_profile_count.heap");

    std::string collapsed_output = runCollapsedProfile(input, /* collapsed_use_count= */ true);

    UInt64 total = sumCollapsedValues(collapsed_output);

    /// Allow +-1 per stack for floating-point rounding (7 stacks).
    EXPECT_NEAR(static_cast<double>(total), static_cast<double>(expected_corrected_count_total), 7.0);
    EXPECT_GT(total, expected_raw_count_total);

    std::filesystem::remove(input);
}

/// When the profile lacks a heap_v2/ header, sampling correction should be
/// disabled and raw values passed through unchanged.
TEST(JemallocProfileSource, CollapsedNoV2HeaderRawPassthrough)
{
    /// Build a v1-style profile by replacing the first line of the test profile.
    std::string profile_no_v2 = "heap\n";
    auto nl = test_heap_profile.find('\n');
    profile_no_v2 += std::string(test_heap_profile.substr(nl + 1));

    auto input = writeToTempFile(profile_no_v2, "gtest_jemalloc_profile_no_v2.heap");

    /// Bytes mode — should return raw bytes without Poisson correction.
    {
        std::string collapsed_output = runCollapsedProfile(input, /* collapsed_use_count= */ false);
        UInt64 total = sumCollapsedValues(collapsed_output);
        EXPECT_EQ(total, expected_raw_total);
    }

    /// Count mode — should return raw counts without Poisson correction.
    {
        std::string collapsed_output = runCollapsedProfile(input, /* collapsed_use_count= */ true);
        UInt64 total = sumCollapsedValues(collapsed_output);
        EXPECT_EQ(total, expected_raw_count_total);
    }

    std::filesystem::remove(input);
}

/// When the heap_v2 header has a malformed interval (e.g. "heap_v2/abc"),
/// parseSamplingInterval should return 0 and raw values should pass through.
TEST(JemallocProfileSource, CollapsedMalformedHeaderRawPassthrough)
{
    std::string profile_malformed = "heap_v2/abc\n";
    auto nl = test_heap_profile.find('\n');
    profile_malformed += std::string(test_heap_profile.substr(nl + 1));

    auto input = writeToTempFile(profile_malformed, "gtest_jemalloc_profile_malformed.heap");

    /// Bytes mode — should return raw bytes without Poisson correction.
    {
        std::string collapsed_output = runCollapsedProfile(input, /* collapsed_use_count= */ false);
        UInt64 total = sumCollapsedValues(collapsed_output);
        EXPECT_EQ(total, expected_raw_total);
    }

    /// Count mode — should return raw counts without Poisson correction.
    {
        std::string collapsed_output = runCollapsedProfile(input, /* collapsed_use_count= */ true);
        UInt64 total = sumCollapsedValues(collapsed_output);
        EXPECT_EQ(total, expected_raw_count_total);
    }

    std::filesystem::remove(input);
}

#endif
