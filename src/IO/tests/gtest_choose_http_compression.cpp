#include <gtest/gtest.h>

#include "config.h"

#include <IO/CompressionMethod.h>

using namespace DB;

TEST(ChooseHTTPCompressionMethod, PrefersServerOrder)
{
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd, gzip, br"), CompressionMethod::Zstd);
#if USE_BROTLI
    EXPECT_EQ(chooseHTTPCompressionMethod("br, gzip"), CompressionMethod::Brotli);
#else
    EXPECT_EQ(chooseHTTPCompressionMethod("br, gzip"), CompressionMethod::Gzip);
#endif
    EXPECT_EQ(chooseHTTPCompressionMethod("gzip"), CompressionMethod::Gzip);
}

TEST(ChooseHTTPCompressionMethod, SkipsZeroQValue)
{
    EXPECT_EQ(chooseHTTPCompressionMethod("br;q=0, gzip"), CompressionMethod::Gzip);
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd;q=0"), CompressionMethod::None);
    EXPECT_EQ(chooseHTTPCompressionMethod("gzip;q=0.0"), CompressionMethod::None);
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd;q=0, br;q=0.000"), CompressionMethod::None);
}

TEST(ChooseHTTPCompressionMethod, HandlesDefaultQValue)
{
    EXPECT_EQ(chooseHTTPCompressionMethod("gzip;q=0.5, zstd"), CompressionMethod::Zstd);
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd;q=0.1, br;q=1"), CompressionMethod::Zstd);
#if USE_BROTLI
    EXPECT_EQ(chooseHTTPCompressionMethod("br;q=0.8, gzip"), CompressionMethod::Brotli);
#else
    EXPECT_EQ(chooseHTTPCompressionMethod("br;q=0.8, gzip"), CompressionMethod::Gzip);
#endif
#if USE_BZIP2
    EXPECT_EQ(chooseHTTPCompressionMethod("bz2;q=0.8, gzip"), CompressionMethod::Gzip);
#else
    EXPECT_EQ(chooseHTTPCompressionMethod("bz2;q=0.8, gzip"), CompressionMethod::Gzip);
#endif
}

TEST(ChooseHTTPCompressionMethod, NoSubstringFalsePositives)
{
    EXPECT_EQ(chooseHTTPCompressionMethod("X-gzip-custom"), CompressionMethod::None);
    EXPECT_EQ(chooseHTTPCompressionMethod("my-br-extra, gzip"), CompressionMethod::Gzip);
}

TEST(ChooseHTTPCompressionMethod, IgnoresWhitespace)
{
    EXPECT_EQ(chooseHTTPCompressionMethod("  gzip, zstd  "), CompressionMethod::Zstd);
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd; q=0, gzip ; q=1"), CompressionMethod::Gzip);
}

TEST(ChooseHTTPCompressionMethod, Empty)
{
    EXPECT_EQ(chooseHTTPCompressionMethod(""), CompressionMethod::None);
}

TEST(ChooseHTTPCompressionMethod, CaseInsensitiveCodingTokens)
{
    EXPECT_EQ(chooseHTTPCompressionMethod("GZip"), CompressionMethod::Gzip);
    EXPECT_EQ(chooseHTTPCompressionMethod("gZIP"), CompressionMethod::Gzip);
    EXPECT_EQ(chooseHTTPCompressionMethod("ZSTD"), CompressionMethod::Zstd);
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd, GZIP"), CompressionMethod::Zstd);
#if USE_SNAPPY
    EXPECT_EQ(chooseHTTPCompressionMethod("Snappy;q=0.5, gzIP;q=0.3"), CompressionMethod::Snappy);
#else
    EXPECT_EQ(chooseHTTPCompressionMethod("Snappy;q=0.5, gzIP;q=0.3"), CompressionMethod::Gzip);
#endif
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd;Q=0, GZip;Q=0.5"), CompressionMethod::Gzip);
}

TEST(ChooseHTTPCompressionMethod, StarWildcard)
{
    /// * matches any encoding — server picks zstd (first in preferred[])
    EXPECT_EQ(chooseHTTPCompressionMethod("*"), CompressionMethod::Zstd);
    /// *;q=0 rejects everything not explicitly listed
    EXPECT_EQ(chooseHTTPCompressionMethod("*;q=0"), CompressionMethod::None);
    /// gzip;q=0 is rejected, * picks up zstd
    EXPECT_EQ(chooseHTTPCompressionMethod("gzip;q=0, *;q=1"), CompressionMethod::Zstd);
    /// zstd;q=0 explicitly rejected, * skips to next preferred
#if USE_BROTLI
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd;q=0, *;q=1"), CompressionMethod::Brotli);
#else
    EXPECT_EQ(chooseHTTPCompressionMethod("zstd;q=0, *;q=1"), CompressionMethod::Lz4);
#endif
}
