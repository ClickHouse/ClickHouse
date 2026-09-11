#include "config.h"

#if USE_JEMALLOC && defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Common/SensitiveString.h>

#if USE_AWS_S3
#include <aws/core/utils/memory/stl/AWSString.h>
#endif

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <string>

namespace
{

bool isExcludedFromCoreDump(const void * ptr)
{
    auto address = reinterpret_cast<uintptr_t>(ptr);
    std::ifstream smaps("/proc/self/smaps");
    std::string line;
    bool inside = false;
    while (std::getline(smaps, line))
    {
        uintptr_t begin;
        uintptr_t end;
        if (sscanf(line.c_str(), "%lx-%lx", &begin, &end) == 2)
            inside = begin <= address && address < end;
        else if (inside && line.starts_with("VmFlags:"))
            return line.find(" dd") != std::string::npos;
    }
    return false;
}

}

TEST(NoDumpArenas, SensitiveStringIsExcludedFromCoreDump)
{
    std::string secret(1 << 20, 's');
    DB::SensitiveString sensitive(secret);
    EXPECT_EQ(std::string_view(sensitive), secret);
    EXPECT_TRUE(isExcludedFromCoreDump(std::string_view(sensitive).data()));
    EXPECT_FALSE(isExcludedFromCoreDump(secret.data()));
}

TEST(NoDumpArenas, SmallSensitiveStringIsExcludedFromCoreDump)
{
    DB::SensitiveString sensitive(std::string(3, 's'));
    EXPECT_TRUE(isExcludedFromCoreDump(std::string_view(sensitive).data()));
}

#if USE_AWS_S3

TEST(NoDumpArenas, AwsStringIsExcludedFromCoreDump)
{
    Aws::String s(1 << 20, 's');
    EXPECT_TRUE(isExcludedFromCoreDump(s.data()));
}

#endif

#endif
