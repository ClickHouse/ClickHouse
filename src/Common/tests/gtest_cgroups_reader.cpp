#if defined(OS_LINUX)

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>

#include <IO/WriteBufferFromFile.h>
#include <Common/MemoryWorker.h>
#include <Common/filesystemHelpers.h>

using namespace DB;


const std::string SAMPLE_FILE[2] = {
    R"(cache 4673703936
rss 2232029184
rss_huge 0
shmem 0
mapped_file 344678400
dirty 4730880
writeback 135168
swap 0
pgpgin 2038569918
pgpgout 2036883790
pgfault 2055373287
pgmajfault 0
inactive_anon 2156335104
active_anon 0
inactive_file 2841305088
active_file 1653915648
unevictable 256008192
hierarchical_memory_limit 8589934592
hierarchical_memsw_limit 8589934592
total_cache 4673703936
total_rss 2232029184
total_rss_huge 0
total_shmem 0
total_mapped_file 344678400
total_dirty 4730880
total_writeback 135168
total_swap 0
total_pgpgin 2038569918
total_pgpgout 2036883790
total_pgfault 2055373287
total_pgmajfault 0
total_inactive_anon 2156335104
total_active_anon 0
total_inactive_file 2841305088
total_active_file 1653915648
total_unevictable 256008192
)",
    R"(anon 10429399040
file 17410793472
kernel 1537789952
kernel_stack 3833856
pagetables 65441792
sec_pagetables 0
percpu 15232
sock 4192
vmalloc 0
shmem 0
zswap 0
zswapped 0
file_mapped 344010752
file_dirty 2060857344
file_writeback 0
swapcached 0
anon_thp 0
file_thp 0
shmem_thp 0
inactive_anon 0
active_anon 10429370368
inactive_file 8693084160
active_file 8717561856
unevictable 0
slab_reclaimable 1460982504
slab_unreclaimable 5152864
slab 1466135368
workingset_refault_anon 0
workingset_refault_file 0
workingset_activate_anon 0
workingset_activate_file 0
workingset_restore_anon 0
workingset_restore_file 0
workingset_nodereclaim 0
pgscan 0
pgsteal 0
pgscan_kswapd 0
pgscan_direct 0
pgscan_khugepaged 0
pgsteal_kswapd 0
pgsteal_direct 0
pgsteal_khugepaged 0
pgfault 43026352
pgmajfault 36762
pgrefill 0
pgactivate 0
pgdeactivate 0
pglazyfree 259
pglazyfreed 0
zswpin 0
zswpout 0
thp_fault_alloc 0
thp_collapse_alloc 0
)"};

const std::string EXPECTED[2]
    = {"{\"active_anon\": 0, \"active_file\": 1653915648, \"cache\": 4673703936, \"dirty\": 4730880, \"hierarchical_memory_limit\": "
       "8589934592, \"hierarchical_memsw_limit\": 8589934592, \"inactive_anon\": 2156335104, \"inactive_file\": 2841305088, "
       "\"mapped_file\": 344678400, \"pgfault\": 2055373287, \"pgmajfault\": 0, \"pgpgin\": 2038569918, \"pgpgout\": 2036883790, \"rss\": "
       "2232029184, \"rss_huge\": 0, \"shmem\": 0, \"swap\": 0, \"total_active_anon\": 0, \"total_active_file\": 1653915648, "
       "\"total_cache\": 4673703936, \"total_dirty\": 4730880, \"total_inactive_anon\": 2156335104, \"total_inactive_file\": 2841305088, "
       "\"total_mapped_file\": 344678400, \"total_pgfault\": 2055373287, \"total_pgmajfault\": 0, \"total_pgpgin\": 2038569918, "
       "\"total_pgpgout\": 2036883790, \"total_rss\": 2232029184, \"total_rss_huge\": 0, \"total_shmem\": 0, \"total_swap\": 0, "
       "\"total_unevictable\": 256008192, \"total_writeback\": 135168, \"unevictable\": 256008192, \"writeback\": 135168}",
       "{\"active_anon\": 10429370368, \"active_file\": 8717561856, \"anon\": 10429399040, \"anon_thp\": 0, \"file\": 17410793472, "
       "\"file_dirty\": 2060857344, \"file_mapped\": 344010752, \"file_thp\": 0, \"file_writeback\": 0, \"inactive_anon\": 0, "
       "\"inactive_file\": 8693084160, \"kernel\": 1537789952, \"kernel_stack\": 3833856, \"pagetables\": 65441792, \"percpu\": 15232, "
       "\"pgactivate\": 0, \"pgdeactivate\": 0, \"pgfault\": 43026352, \"pglazyfree\": 259, \"pglazyfreed\": 0, \"pgmajfault\": 36762, "
       "\"pgrefill\": 0, \"pgscan\": 0, \"pgscan_direct\": 0, \"pgscan_khugepaged\": 0, \"pgscan_kswapd\": 0, \"pgsteal\": 0, "
       "\"pgsteal_direct\": 0, \"pgsteal_khugepaged\": 0, \"pgsteal_kswapd\": 0, \"sec_pagetables\": 0, \"shmem\": 0, \"shmem_thp\": 0, "
       "\"slab\": 1466135368, \"slab_reclaimable\": 1460982504, \"slab_unreclaimable\": 5152864, \"sock\": 4192, \"swapcached\": 0, "
       "\"thp_collapse_alloc\": 0, \"thp_fault_alloc\": 0, \"unevictable\": 0, \"vmalloc\": 0, \"workingset_activate_anon\": 0, "
       "\"workingset_activate_file\": 0, \"workingset_nodereclaim\": 0, \"workingset_refault_anon\": 0, \"workingset_refault_file\": 0, "
       "\"workingset_restore_anon\": 0, \"workingset_restore_file\": 0, \"zswap\": 0, \"zswapped\": 0, \"zswpin\": 0, \"zswpout\": 0}"};


class CgroupsMemoryUsageObserverFixture : public ::testing::TestWithParam<ICgroupsReader::CgroupsVersion>
{
    void SetUp() override
    {
        const uint8_t version = static_cast<uint8_t>(GetParam());
        tmp_dir = fmt::format("./test_cgroups_{}", magic_enum::enum_name(GetParam()));
        fs::create_directories(tmp_dir);

        auto stat_file = WriteBufferFromFile(tmp_dir + "/memory.stat");
        stat_file.write(SAMPLE_FILE[version].data(), SAMPLE_FILE[version].size());
        stat_file.finalize();
        stat_file.sync();

        if (GetParam() == ICgroupsReader::CgroupsVersion::V2)
        {
            auto current_file = WriteBufferFromFile(tmp_dir + "/memory.current");
            current_file.write("29645422592", 11);
            current_file.finalize();
            current_file.sync();
        }
    }

protected:
    std::string tmp_dir;
};


TEST_P(CgroupsMemoryUsageObserverFixture, ReadMemoryUsageTest)
{
    const auto version = GetParam();
    auto reader = ICgroupsReader::createCgroupsReader(version, tmp_dir);
    ASSERT_EQ(
        reader->readMemoryUsage(),
        version == ICgroupsReader::CgroupsVersion::V1 ? /* rss from memory.stat */ 2232029184
                                                                  : /* anon+sock+kernel-slab_reclaimable from memory.stat */ 10506210680);
}


TEST_P(CgroupsMemoryUsageObserverFixture, DumpAllStatsTest)
{
    const auto version = GetParam();
    auto reader = ICgroupsReader::createCgroupsReader(version, tmp_dir);
    ASSERT_EQ(reader->dumpAllStats(), EXPECTED[static_cast<uint8_t>(version)]);
}


INSTANTIATE_TEST_SUITE_P(
    CgroupsMemoryUsageObserverTests,
    CgroupsMemoryUsageObserverFixture,
    ::testing::Values(ICgroupsReader::CgroupsVersion::V1, ICgroupsReader::CgroupsVersion::V2));


/// Test cgroupv2 memory.stat without kernel/slab_reclaimable (older kernels).
/// Result should be just anon + sock.
TEST(CgroupsV2NoKernel, ReadMemoryUsageTest)
{
    std::string tmp_dir = "./test_cgroups_v2_no_kernel";
    fs::create_directories(tmp_dir);

    auto stat_file = WriteBufferFromFile(tmp_dir + "/memory.stat");
    std::string content = R"(anon 5000000000
file 1000000000
sock 1000
inactive_anon 0
active_anon 5000000000
)";
    stat_file.write(content.data(), content.size());
    stat_file.finalize();
    stat_file.sync();

    auto reader = ICgroupsReader::createCgroupsReader(ICgroupsReader::CgroupsVersion::V2, tmp_dir);
    ASSERT_EQ(reader->readMemoryUsage(), /* anon + sock */ 5000001000);

    fs::remove_all(tmp_dir);
}

/// Decision matrix for the cgroup-aware dynamic hard-limit headroom computation
/// (`MemoryWorker::readAvailableForDynamicLimit`), exercised through the pure
/// `decideCgroupLevelAvailability` helper so the cases do not require real cgroup files.
TEST(DynamicHardLimitCgroupDecision, DecideCgroupLevelAvailability)
{
    using namespace MemoryWorkerHelpers;
    constexpr uint64_t host_ram = 64ull << 30; /// 64 GiB

    /// Finite ancestor `memory.max` with same-level `memory.current`: headroom = max - used.
    {
        auto d = decideCgroupLevelAvailability("10000000000", /* used = */ 3000000000, host_ram);
        ASSERT_EQ(d.kind, CgroupLevelKind::Finite);
        ASSERT_EQ(d.available, 7000000000ull);
    }

    /// A finite limit may have trailing fields/whitespace; only the first token matters.
    {
        auto d = decideCgroupLevelAvailability("10000000000\n", /* used = */ 1, host_ram);
        ASSERT_EQ(d.kind, CgroupLevelKind::Finite);
        ASSERT_EQ(d.available, 9999999999ull);
    }

    /// Usage at or above the limit is a genuine "fully under pressure" signal: available = 0,
    /// distinct from a read failure (which the caller turns into `std::nullopt`).
    {
        auto d = decideCgroupLevelAvailability("10000000000", /* used = */ 10000000000, host_ram);
        ASSERT_EQ(d.kind, CgroupLevelKind::Finite);
        ASSERT_EQ(d.available, 0ull);

        auto d2 = decideCgroupLevelAvailability("10000000000", /* used = */ 20000000000, host_ram);
        ASSERT_EQ(d2.kind, CgroupLevelKind::Finite);
        ASSERT_EQ(d2.available, 0ull);
    }

    /// cgroup v2 `"max"` token: no limit at this level.
    {
        auto d = decideCgroupLevelAvailability("max", /* used = */ 123, host_ram);
        ASSERT_EQ(d.kind, CgroupLevelKind::Unbounded);
    }

    /// cgroup v1 "no limit" sentinel (>= host RAM, e.g. PAGE_COUNTER_MAX ~ 2^63): unbounded.
    {
        auto d = decideCgroupLevelAvailability("9223372036854771712", /* used = */ 123, host_ram);
        ASSERT_EQ(d.kind, CgroupLevelKind::Unbounded);

        /// A limit exactly at host RAM is also treated as unbounded.
        auto d2 = decideCgroupLevelAvailability(std::to_string(host_ram), /* used = */ 0, host_ram);
        ASSERT_EQ(d2.kind, CgroupLevelKind::Unbounded);
    }

    /// With the sentinel filter disabled (`host_memory_bytes == 0`), a huge value is finite.
    {
        auto d = decideCgroupLevelAvailability("9223372036854771712", /* used = */ 0, /* host_memory_bytes = */ 0);
        ASSERT_EQ(d.kind, CgroupLevelKind::Finite);
    }

    /// A `0` limit or an unparseable token is treated as "no usable finite limit", not as a
    /// zero-headroom finite limit (a `0` cap is never a real budget for a running server).
    {
        auto zero = decideCgroupLevelAvailability("0", /* used = */ 0, host_ram);
        ASSERT_EQ(zero.kind, CgroupLevelKind::Unbounded);

        auto garbage = decideCgroupLevelAvailability("not-a-number", /* used = */ 0, host_ram);
        ASSERT_EQ(garbage.kind, CgroupLevelKind::Unbounded);

        auto empty = decideCgroupLevelAvailability("", /* used = */ 0, host_ram);
        ASSERT_EQ(empty.kind, CgroupLevelKind::Unbounded);
    }
}

#endif
