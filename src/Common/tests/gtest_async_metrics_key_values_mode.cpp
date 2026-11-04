#include <Common/AsynchronousMetrics.h>

#include <gtest/gtest.h>

#include <cmath>

using namespace DB;

/// The pre-26.8 name of a key-value metric is a compatibility contract with every monitoring setup that was
/// built before the change, and the mangling is not uniform: the key was appended to the name, appended after
/// an underscore, put in the middle of the name, or prepended to it, depending on the family. Each of these
/// shapes is checked here, one family at a time, because a typo in the table would otherwise only show up as
/// a metric silently missing from a dashboard.
TEST(AsynchronousMetricsKeyValuesMode, LegacyNameOfEveryNamingShape)
{
    /// The key was appended right after the name.
    EXPECT_EQ(getLegacyAsynchronousMetricName("OSUserTimeCPU", "3"), "OSUserTimeCPU3");
    EXPECT_EQ(getLegacyAsynchronousMetricName("OSIOWaitTimeCPU", "0"), "OSIOWaitTimeCPU0");
    EXPECT_EQ(getLegacyAsynchronousMetricName("OSGuestNiceTimeCPU", "11"), "OSGuestNiceTimeCPU11");

    /// The key was appended after an underscore.
    EXPECT_EQ(getLegacyAsynchronousMetricName("BlockReadBytes", "sda"), "BlockReadBytes_sda");
    EXPECT_EQ(getLegacyAsynchronousMetricName("BlockActiveTimePerOp", "nvme0n1"), "BlockActiveTimePerOp_nvme0n1");
    EXPECT_EQ(getLegacyAsynchronousMetricName("NetworkReceiveDrop", "eth0"), "NetworkReceiveDrop_eth0");
    EXPECT_EQ(getLegacyAsynchronousMetricName("CPUFrequencyMHz", "7"), "CPUFrequencyMHz_7");
    EXPECT_EQ(getLegacyAsynchronousMetricName("DiskTotal", "default"), "DiskTotal_default");
    EXPECT_EQ(getLegacyAsynchronousMetricName("DiskGetObjectThrottlerRPS", "s3"), "DiskGetObjectThrottlerRPS_s3");

    /// The key was in the middle of the name.
    EXPECT_EQ(getLegacyAsynchronousMetricName("EDACCorrectable", "0"), "EDAC0_Correctable");
    EXPECT_EQ(getLegacyAsynchronousMetricName("EDACUncorrectable", "1"), "EDAC1_Uncorrectable");
    EXPECT_EQ(getLegacyAsynchronousMetricName("AsyncLoggingQueueSize", "TextLog"), "AsyncLoggingTextLogQueueSize");

    /// The key was a prefix of the name.
    EXPECT_EQ(getLegacyAsynchronousMetricName("DeadBlobsQueueEstimate", "s3_disk"), "s3_diskDeadBlobsQueueEstimate");
    EXPECT_EQ(getLegacyAsynchronousMetricName("MissingBlobsQueueEstimate", "s3_disk"), "s3_diskMissingBlobsQueueEstimate");

    /// `Temperature` merged two families that were named differently: the thermal zones, whose numeric keys
    /// were appended right after the name, and the hardware monitors, whose names followed an underscore.
    EXPECT_EQ(getLegacyAsynchronousMetricName("Temperature", "3"), "Temperature3");
    EXPECT_EQ(getLegacyAsynchronousMetricName("Temperature", "coretemp_Core_0"), "Temperature_coretemp_Core_0");
    /// A key that only starts with a digit is a hardware monitor name, not a thermal zone number.
    EXPECT_EQ(getLegacyAsynchronousMetricName("Temperature", "0x_sensor"), "Temperature_0x_sensor");

    /// A family that never had a legacy name, and a metric that is not a key-value one at all.
    EXPECT_EQ(getLegacyAsynchronousMetricName("SomeMetricAddedLater", "key"), "");
    EXPECT_EQ(getLegacyAsynchronousMetricName("Uptime", ""), "");
}

namespace
{

AsynchronousMetricValues makeValues()
{
    AsynchronousMetricValues values;
    values["Uptime"] = AsynchronousMetricValue(1000, "The uptime.");
    values["DiskTotal"] = AsynchronousMetricValue("disk", AsynchronousMetricKeyValues{{"default", 10}, {"s3", 20}}, "The size.");
    values["EDACCorrectable"] = AsynchronousMetricValue("controller", AsynchronousMetricKeyValues{{"0", 1}}, "The errors.");
    /// A key-value family introduced after the change: it has no legacy name, so it is published as it is.
    values["NewFamily"] = AsynchronousMetricValue("thing", AsynchronousMetricKeyValues{{"a", 5}}, "Something new.");
    return values;
}

}

TEST(AsynchronousMetricsKeyValuesMode, KeyValuesLeavesTheValuesAlone)
{
    auto values = makeValues();
    applyAsynchronousMetricsKeyValuesMode(values, AsynchronousMetricsKeyValuesMode::KeyValues);

    EXPECT_EQ(values.size(), 4);
    EXPECT_TRUE(values.at("DiskTotal").isMap());
    EXPECT_FALSE(values.contains("DiskTotal_default"));
}

TEST(AsynchronousMetricsKeyValuesMode, LegacyNamesReplacesTheKeyValueForm)
{
    auto values = makeValues();
    applyAsynchronousMetricsKeyValuesMode(values, AsynchronousMetricsKeyValuesMode::LegacyNames);

    EXPECT_FALSE(values.contains("DiskTotal"));
    EXPECT_FALSE(values.contains("EDACCorrectable"));

    EXPECT_EQ(values.at("DiskTotal_default").value, 10);
    EXPECT_EQ(values.at("DiskTotal_s3").value, 20);
    EXPECT_FALSE(values.at("DiskTotal_default").isMap());
    EXPECT_STREQ(values.at("DiskTotal_default").documentation, "The size.");
    EXPECT_EQ(values.at("EDAC0_Correctable").value, 1);

    /// A scalar metric and a family with no legacy name are untouched.
    EXPECT_EQ(values.at("Uptime").value, 1000);
    EXPECT_TRUE(values.at("NewFamily").isMap());
}

TEST(AsynchronousMetricsKeyValuesMode, BothPublishesTheTwoForms)
{
    auto values = makeValues();
    applyAsynchronousMetricsKeyValuesMode(values, AsynchronousMetricsKeyValuesMode::Both);

    EXPECT_TRUE(values.at("DiskTotal").isMap());
    EXPECT_TRUE(std::isnan(values.at("DiskTotal").value));
    EXPECT_EQ(values.at("DiskTotal").key_values.at("s3"), 20);
    EXPECT_EQ(values.at("DiskTotal_s3").value, 20);
    EXPECT_EQ(values.at("EDAC0_Correctable").value, 1);
    EXPECT_TRUE(values.at("EDACCorrectable").isMap());
    EXPECT_TRUE(values.contains("NewFamily"));
}
