#include <gtest/gtest.h>

#include <Common/config.h>


#if USE_AWS_S3

#include <aws/core/http/URI.h>


TEST(IOTestAwsHttpUri, NoEmptyPathInUri)
{
    /// See https://github.com/ClickHouse/aws-sdk-cpp/pull/5

    Aws::Http::URI url_one("http://domain.com/?delete");

    EXPECT_EQ(url_one.GetURIString(), "http://domain.com/?delete");

    url_one.SetPath("");
    EXPECT_EQ(url_one.GetURIString(), "http://domain.com/?delete");

    Aws::Http::URI url_two("http://domain.com?delete");
    EXPECT_EQ(url_one.GetURIString(), "http://domain.com/?delete");

    url_two.SetPath("");
    EXPECT_EQ(url_one.GetURIString(), "http://domain.com/?delete");

    EXPECT_EQ(url_one.GetURIString(false), "http://domain.com");
    EXPECT_EQ(url_two.GetURIString(false), "http://domain.com");
}

#endif
