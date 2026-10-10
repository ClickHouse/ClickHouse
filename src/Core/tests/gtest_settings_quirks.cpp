#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <IO/preadNoWait.h>

namespace DB::Setting
{
extern const SettingsString local_filesystem_read_method;
}

using namespace DB;


TEST(SettingsQuirks, PreadThreadpoolNeedsPreadNoWait)
{
    /// 'pread_threadpool' hands a read off to a thread pool for everything it cannot find in the
    /// page cache, and it needs `preadNoWait` to tell the two apart. Where that check is
    /// unavailable, every read pays for the hand-off, so the default is switched to 'pread'.
    Settings settings;
    ASSERT_EQ(settings[Setting::local_filesystem_read_method].value, "pread_threadpool");
    ASSERT_FALSE(settings[Setting::local_filesystem_read_method].changed);

    applySettingsQuirks(settings);

    EXPECT_EQ(
        settings[Setting::local_filesystem_read_method].value,
        preadNoWaitUnavailableReason().empty() ? "pread_threadpool" : "pread");

    /// The switch is a property of this host, not of the query. A changed setting is serialized
    /// into the query the initiator sends to the remote shards, and a host that cannot use the
    /// system call must not impose 'pread' on the shards that can.
    EXPECT_FALSE(settings[Setting::local_filesystem_read_method].changed);
    for (const auto & change : settings.changes())
        EXPECT_NE(change.name, "local_filesystem_read_method");
}

TEST(SettingsQuirks, AnExplicitReadMethodIsKept)
{
    /// Only the default is switched: an explicitly requested read method is left alone,
    /// on any system.
    for (const auto & method : {"pread_threadpool", "read", "pread", "mmap", "pread_fake_async", "io_uring"})
    {
        Settings settings;
        settings[Setting::local_filesystem_read_method] = method;
        ASSERT_TRUE(settings[Setting::local_filesystem_read_method].changed);

        applySettingsQuirks(settings);

        EXPECT_EQ(settings[Setting::local_filesystem_read_method].value, method);
    }
}
