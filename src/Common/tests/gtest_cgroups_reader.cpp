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
sock 0
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
       "\"slab\": 1466135368, \"slab_reclaimable\": 1460982504, \"slab_unreclaimable\": 5152864, \"sock\": 0, \"swapcached\": 0, "
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
        stat_file.sync();

        if (GetParam() == ICgroupsReader::CgroupsVersion::V2)
        {
            auto current_file = WriteBufferFromFile(tmp_dir + "/memory.current");
            current_file.write("29645422592", 11);
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
                                                                  : /* anon from memory.stat */ 10429399040);
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

#endif
