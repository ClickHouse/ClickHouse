#include <config.h>

#if USE_ODPS_TUNNEL

#include <Storages/OdpsRecordReader.h>

#include <gtest/gtest.h>

namespace DB
{
namespace
{

TEST(OdpsReadError, ClassifiesStructuredTransportErrors)
{
    const apsara::odps::sdk::OdpsException throttled(apsara::odps::sdk::FLOW_EXCEEDED, "quota exceeded");
    const apsara::odps::sdk::OdpsException timeout(apsara::odps::sdk::REQUEST_TIMEOUT, "request timed out");
    const apsara::odps::sdk::OdpsException legacy_timeout("Connection Timeout");

    EXPECT_EQ(classifyOdpsReadError(throttled), OdpsReadErrorKind::Retryable);
    EXPECT_EQ(classifyOdpsReadError(timeout), OdpsReadErrorKind::Retryable);
    EXPECT_EQ(classifyOdpsReadError(legacy_timeout), OdpsReadErrorKind::Retryable);
}

TEST(OdpsReadError, NormalizesInterruptedStreamsWithoutRetryingCorruption)
{
    const apsara::odps::sdk::OdpsTunnelException interrupted(
        apsara::odps::sdk::INTERNAL_ERROR,
        "Read tag error, maybe EOF reached.");
    const apsara::odps::sdk::OdpsTunnelException corrupt(
        apsara::odps::sdk::INTERNAL_ERROR,
        "Record total checksum error, received: 1, actually: 2");
    const apsara::odps::sdk::OdpsTunnelException unauthorized("AuthorizationDenied", "permission denied");

    EXPECT_EQ(classifyOdpsReadError(interrupted), OdpsReadErrorKind::Retryable);
    EXPECT_EQ(classifyOdpsReadError(corrupt), OdpsReadErrorKind::Unrecoverable);
    EXPECT_EQ(classifyOdpsReadError(unauthorized), OdpsReadErrorKind::Unrecoverable);
}

#if USE_ODPS_ARROW
TEST(OdpsReadError, NormalizesArrowTransportTruncation)
{
    const apsara::odps::sdk::OdpsTunnelException interrupted("ArrowHttpInputStream Deserialize Exception");
    EXPECT_EQ(classifyOdpsReadError(interrupted), OdpsReadErrorKind::Retryable);
}
#endif

}
}

#endif
