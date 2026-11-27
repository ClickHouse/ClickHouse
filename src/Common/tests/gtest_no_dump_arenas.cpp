#include "config.h"

#if USE_JEMALLOC && defined(OS_LINUX)

#include <gtest/gtest.h>

#include <Common/SecretString.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Text.h>
#include <fmt/format.h>

#if USE_AWS_S3
#include <aws/core/utils/memory/stl/AWSString.h>
#endif

#include <charconv>
#include <cstdint>
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
        uintptr_t begin = 0;
        uintptr_t end = 0;
        const char * line_end = line.data() + line.size();
        auto [separator, begin_error] = std::from_chars(line.data(), line_end, begin, 16);
        if (begin_error == std::errc() && separator != line_end && *separator == '-'
            && std::from_chars(separator + 1, line_end, end, 16).ec == std::errc())
            inside = begin <= address && address < end;
        else if (inside && line.starts_with("VmFlags:"))
            return line.contains(" dd");
    }
    return false;
}

}

TEST(NoDumpArenas, SecretStringIsExcludedFromCoreDump)
{
    std::string secret(1 << 20, 's');
    DB::SecretString sensitive(secret);
    EXPECT_EQ(std::string_view(sensitive), secret);
    EXPECT_TRUE(isExcludedFromCoreDump(std::string_view(sensitive).data()));
    EXPECT_FALSE(isExcludedFromCoreDump(secret.data()));
}

TEST(NoDumpArenas, SmallSecretStringIsExcludedFromCoreDump)
{
    DB::SecretString sensitive(std::string(3, 's'));
    EXPECT_TRUE(isExcludedFromCoreDump(std::string_view(sensitive).data()));
}

TEST(NoDumpArenas, ConfigValueIsExcludedFromCoreDump)
{
    std::string secret(1 << 20, 's');
    Poco::XML::DOMParser parser;
    Poco::AutoPtr<Poco::XML::Document> document = parser.parseString(fmt::format("<clickhouse><secret>{}</secret></clickhouse>", secret));
    const auto * text = document->getNodeByPath("clickhouse/secret")->firstChild();
    EXPECT_TRUE(isExcludedFromCoreDump(text->getNodeValue().data()));
}

#if USE_AWS_S3

TEST(NoDumpArenas, AwsStringIsExcludedFromCoreDump)
{
    Aws::String s(1 << 20, 's');
    EXPECT_TRUE(isExcludedFromCoreDump(s.data()));
}

#endif

#endif
