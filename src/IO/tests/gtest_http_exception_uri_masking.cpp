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

TEST(HTTPException, MasksCredentialsInBareRequestTarget)
{
    /// The argument `assertResponseIsOk` receives on the write path is the request target
    /// `WriteBufferFromHTTP` builds with `getPathAndQuery()`, which has no scheme and no authority.
    const HTTPException exception(
        ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
        "/bucket/key?X-Amz-Credential=AKIAKEYIDPROBE%2Fx&Expires=1758600000&prefix=a/b",
        Poco::Net::HTTPResponse::HTTP_FORBIDDEN,
        "Forbidden",
        "denied");

    const std::string & message = exception.message();

    EXPECT_FALSE(message.contains("AKIAKEYIDPROBE")) << message;
    EXPECT_FALSE(message.contains("1758600000")) << message;

    EXPECT_TRUE(message.contains("X-Amz-Credential=[HIDDEN]")) << message;
    /// `Expires` is not classified as sensitive by name; only `maskPresignedURLParameters` reaches it.
    EXPECT_TRUE(message.contains("Expires=[HIDDEN]")) << message;

    EXPECT_TRUE(message.contains("/bucket/key")) << message;
    EXPECT_TRUE(message.contains("prefix=a/b")) << message;
}

TEST(HTTPException, KeepsAURIWithoutCredentialsIntact)
{
    const std::string uri = "http://127.0.0.1:1/bucket/key?list-type=2&prefix=a/b&delimiter=/";

    const HTTPException exception(
        ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
        uri,
        Poco::Net::HTTPResponse::HTTP_FORBIDDEN,
        "Forbidden",
        "denied");

    const std::string & message = exception.message();

    /// A URI that carries no credential is reported verbatim, which a blanket redactor would fail.
    EXPECT_TRUE(message.contains(uri)) << message;
    EXPECT_FALSE(message.contains("[HIDDEN]")) << message;
}
