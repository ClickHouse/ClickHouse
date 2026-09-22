#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <IO/HTTPCommon.h>

#include <string>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}
}


TEST(HTTPException, MasksCredentialsInURI)
{
    const HTTPException exception(
        ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
        "http://unameprobe:upwprobe@127.0.0.1:1/bucket/key"
        "?X-Amz-Signature=sigprobe&password=qpwprobe&GoogleAccessId=gcsprobe&list-type=2",
        Poco::Net::HTTPResponse::HTTP_FORBIDDEN,
        "Forbidden",
        "denied");

    const std::string & message = exception.message();

    /// Of the two userinfo maskers, only `maskURIUserinfo` removes the user name; `maskURIPassword` keeps it.
    EXPECT_FALSE(message.contains("unameprobe")) << message;
    EXPECT_FALSE(message.contains("upwprobe")) << message;
    EXPECT_FALSE(message.contains("sigprobe")) << message;
    EXPECT_FALSE(message.contains("qpwprobe")) << message;
    EXPECT_FALSE(message.contains("gcsprobe")) << message;

    EXPECT_TRUE(message.contains("[HIDDEN]@")) << message;
    EXPECT_TRUE(message.contains("password=[HIDDEN]")) << message;
    EXPECT_TRUE(message.contains("GoogleAccessId=[HIDDEN]")) << message;

    EXPECT_TRUE(message.contains("list-type=2")) << message;
    EXPECT_TRUE(message.contains("127.0.0.1")) << message;
}
