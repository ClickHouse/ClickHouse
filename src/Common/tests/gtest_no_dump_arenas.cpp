#include "config.h"

#if USE_JEMALLOC && defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Common/SensitiveString.h>

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
    EXPECT_EQ(sensitive.view(), secret);
    EXPECT_TRUE(isExcludedFromCoreDump(sensitive.view().data()));
    EXPECT_FALSE(isExcludedFromCoreDump(secret.data()));
}

#endif
