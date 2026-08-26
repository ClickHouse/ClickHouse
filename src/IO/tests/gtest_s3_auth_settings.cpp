#include <gtest/gtest.h>
#include <Core/Settings.h>
#include <IO/S3AuthSettings.h>
#include <IO/S3Defines.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/AutoPtr.h>
#include <sstream>

using namespace DB;

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsUInt64 gcs_max_conditional_put_bytes;
}

namespace
{
Poco::AutoPtr<Poco::Util::XMLConfiguration> makeDiskConfig(const std::string & inner)
{
    std::istringstream iss("<clickhouse><disk>" + inner + "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(iss);
}
}

/// The cap is a property of the GCS conditional-write dialect, so it is read from the disk block
/// unprefixed, exactly like `gcs_issue_compose_request` beside it.
TEST(S3AuthSettingsConfig, GcsConditionalPutCapParsesFromDiskBlock)
{
    Settings query_settings;

    auto with_override = makeDiskConfig(
        "<gcs_max_conditional_put_bytes>4096</gcs_max_conditional_put_bytes>");
    S3::S3AuthSettings overridden(*with_override, query_settings, "disk");
    EXPECT_EQ(overridden[S3AuthSetting::gcs_max_conditional_put_bytes].value, 4096u);

    auto without = makeDiskConfig("<endpoint>http://x/y</endpoint>");
    S3::S3AuthSettings defaulted(*without, query_settings, "disk");
    EXPECT_EQ(defaulted[S3AuthSetting::gcs_max_conditional_put_bytes].value,
              S3::DEFAULT_GCS_MAX_CONDITIONAL_PUT_BYTES);
}
