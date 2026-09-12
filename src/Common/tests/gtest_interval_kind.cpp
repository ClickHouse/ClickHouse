#include <Common/Exception.h>
#include <Common/IntervalKind.h>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#ifdef DEBUG_OR_SANITIZER_BUILD
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#endif

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
template <typename Result>
void checkInvalidKinds(Result (DB::IntervalKind::*method)() const)
{
    for (unsigned value : {0x0Bu, 0x7Fu, 0xFFu})
    {
        SCOPED_TRACE(value);
        const DB::IntervalKind interval(static_cast<DB::IntervalKind::Kind>(value));
#ifdef DEBUG_OR_SANITIZER_BUILD
        EXPECT_DEATH(
            {
                Poco::Logger::root().setChannel(new Poco::ConsoleChannel);
                (interval.*method)();
            },
            "Unexpected IntervalKind");
#else
        EXPECT_THAT(
            [&] { (interval.*method)(); },
            ::testing::Throws<DB::Exception>(::testing::Property(&DB::Exception::code, DB::ErrorCodes::LOGICAL_ERROR)));
#endif
    }
}
}

TEST(IntervalKindDeathTest, InvalidKinds)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
#endif
    checkInvalidKinds(&DB::IntervalKind::toString);
    checkInvalidKinds(&DB::IntervalKind::toAvgNanoseconds);
    checkInvalidKinds(&DB::IntervalKind::toAvgMilliseconds);
    checkInvalidKinds(&DB::IntervalKind::toAvgSeconds);
    checkInvalidKinds(&DB::IntervalKind::toSeconds);
    checkInvalidKinds(&DB::IntervalKind::isFixedLength);
    checkInvalidKinds(&DB::IntervalKind::toKeyword);
    checkInvalidKinds(&DB::IntervalKind::toLowercasedKeyword);
    checkInvalidKinds(&DB::IntervalKind::toDateDiffUnit);
    checkInvalidKinds(&DB::IntervalKind::toNameOfFunctionToIntervalDataType);
    checkInvalidKinds(&DB::IntervalKind::toNameOfFunctionExtractTimePart);
}
