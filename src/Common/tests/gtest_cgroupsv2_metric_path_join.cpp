#if defined(OS_LINUX)

#include <gtest/gtest.h>

#include <base/cgroupsv2.h>

#include <filesystem>
#include <string>
#include <string_view>

TEST(CgroupsV2, MetricPathJoinDoesNotRequireTrailingSlash)
{
    if (!cgroupsV2Enabled())
        GTEST_SKIP() << "cgroup v2 is not enabled";

    constexpr std::string_view file_name = "memory.max";
    const auto cgroup_dir = getCgroupsV2PathContainingFile(file_name);
    if (!cgroup_dir)
        GTEST_SKIP() << "cgroup v2 directory for the current process is not detected";

    std::string dir = *cgroup_dir;
    while (!dir.empty() && dir.back() == '/')
        dir.pop_back();

    const auto correct_path = (std::filesystem::path(dir) / file_name).string();
    const auto wrong_path = dir + std::string(file_name);

    std::error_code ec;
    if (!std::filesystem::exists(correct_path, ec))
        GTEST_SKIP() << "cgroup v2 path is not accessible: " << correct_path;

    EXPECT_NE(correct_path, wrong_path);
    EXPECT_FALSE(std::filesystem::exists(wrong_path, ec));
}

#endif
