#include <gtest/gtest.h>

#include <Disks/warnIfExt4CorruptionKernelBug.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

using namespace DB;

namespace
{

constexpr auto warning_type = Context::WarningType::LINUX_KERNEL_EXT4_CORRUPTION_BUG;

class Ext4CorruptionKernelBugWarningTest : public ::testing::Test
{
protected:
    ContextPtr context = getContext().context;

    void SetUp() override { reset(); }
    void TearDown() override { reset(); }

    void reset()
    {
        setSuppressRegexp("");
        flushExt4CorruptionKernelBugWarning(*context);
        context->addOrUpdateWarningMessage(warning_type, std::nullopt);
    }

    void setSuppressRegexp(const String & regexp)
    {
        Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration());
        if (!regexp.empty())
            config->setString("warning_supress_regexp", regexp);
        getContext().context->setConfig(config);
    }

    String stored() const
    {
        auto message = context->getWarningMessage(warning_type);
        return message ? message->text : "";
    }
};

}

TEST_F(Ext4CorruptionKernelBugWarningTest, CommittedBatchIsPublished)
{
    {
        Ext4CorruptionKernelBugWarningBatch batch;
        recordExt4CorruptionKernelBugProbe("ext4", "/committed", "disk a");
        recordExt4CorruptionKernelBugProbe("xfs", "/other", "disk b");
        EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 0);
        batch.commit();
    }
    EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 1);
    EXPECT_NE(stored().find("/committed"), String::npos);
}

TEST_F(Ext4CorruptionKernelBugWarningTest, UncommittedBatchIsDiscarded)
{
    {
        Ext4CorruptionKernelBugWarningBatch outer;
        {
            Ext4CorruptionKernelBugWarningBatch inner;
            recordExt4CorruptionKernelBugProbe("ext4", "/aborted", "disk a");
        }
        outer.commit();
    }
    EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 0);
    EXPECT_EQ(stored(), "");
}

TEST_F(Ext4CorruptionKernelBugWarningTest, UndeterminedProbeKeepsStoredExt4Hit)
{
    recordExt4CorruptionKernelBugProbe("ext4", "/hit", "disk a");
    recordExt4CorruptionKernelBugProbe("", "/unknown", "disk b");
    EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 1);
    EXPECT_NE(stored().find("/hit"), String::npos);

    recordExt4CorruptionKernelBugProbe("", "/unknown", "disk b");
    EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 0);
    EXPECT_NE(stored().find("/hit"), String::npos);
}

TEST_F(Ext4CorruptionKernelBugWarningTest, SuppressedProbeKeepsEarlierHit)
{
    setSuppressRegexp("suppressed_disk");
    recordExt4CorruptionKernelBugProbe("ext4", "/hit", "disk a");
    recordExt4CorruptionKernelBugProbe("ext4", "/suppressed_disk", "disk b");
    EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 1);
    EXPECT_NE(stored().find("/hit"), String::npos);

    /// A suppressed hit stores nothing, so a later undetermined probe is published.
    reset();
    setSuppressRegexp("suppressed_disk");
    recordExt4CorruptionKernelBugProbe("ext4", "/suppressed_disk", "disk a");
    recordExt4CorruptionKernelBugProbe("", "/unknown", "disk b");
    EXPECT_EQ(flushExt4CorruptionKernelBugWarning(*context), 1);
    EXPECT_NE(stored().find("/unknown"), String::npos);
}
