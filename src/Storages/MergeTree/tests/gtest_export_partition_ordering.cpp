#include <gtest/gtest.h>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>

namespace DB
{

class ExportPartitionOrderingTest : public ::testing::Test
{
protected:
    ExportPartitionTaskEntriesContainer container;
    ExportPartitionTaskEntriesContainer::index<ExportPartitionTaskEntryTagByCompositeKey>::type & by_key;
    ExportPartitionTaskEntriesContainer::index<ExportPartitionTaskEntryTagByCreateTime>::type & by_create_time;

    ExportPartitionOrderingTest()
        : by_key(container.get<ExportPartitionTaskEntryTagByCompositeKey>())
        , by_create_time(container.get<ExportPartitionTaskEntryTagByCreateTime>())
    {
    }
};

TEST_F(ExportPartitionOrderingTest, IterationOrderMatchesCreateTime)
{
    time_t base_time = 1000;
    
    ExportReplicatedMergeTreePartitionManifest manifest1;
    manifest1.partition_id = "2020";
    manifest1.destination_database = "db1";
    manifest1.destination_table = "table1";
    manifest1.transaction_id = "tx1";
    manifest1.create_time = base_time + 300; // Latest
    
    ExportReplicatedMergeTreePartitionManifest manifest2;
    manifest2.partition_id = "2021";
    manifest2.destination_database = "db1";
    manifest2.destination_table = "table1";
    manifest2.transaction_id = "tx2";
    manifest2.create_time = base_time + 100; // Middle
    
    ExportReplicatedMergeTreePartitionManifest manifest3;
    manifest3.partition_id = "2022";
    manifest3.destination_database = "db1";
    manifest3.destination_table = "table1";
    manifest3.transaction_id = "tx3";
    manifest3.create_time = base_time; // Oldest

    ExportReplicatedMergeTreePartitionTaskEntry entry1{manifest1, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, {}};
    ExportReplicatedMergeTreePartitionTaskEntry entry2{manifest2, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, {}};
    ExportReplicatedMergeTreePartitionTaskEntry entry3{manifest3, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, {}};

    // Insert in reverse order
    by_key.insert(entry1);
    by_key.insert(entry2);
    by_key.insert(entry3);

    // Verify iteration order matches create_time (ascending)
    auto it = by_create_time.begin();
    ASSERT_NE(it, by_create_time.end());
    EXPECT_EQ(it->manifest.partition_id, "2022"); // Oldest first
    EXPECT_EQ(it->manifest.create_time, base_time);
    
    ++it;
    ASSERT_NE(it, by_create_time.end());
    EXPECT_EQ(it->manifest.partition_id, "2021");
    EXPECT_EQ(it->manifest.create_time, base_time + 100);
    
    ++it;
    ASSERT_NE(it, by_create_time.end());
    EXPECT_EQ(it->manifest.partition_id, "2020");
    EXPECT_EQ(it->manifest.create_time, base_time + 300);
    
    ++it;
    EXPECT_EQ(it, by_create_time.end());
}

}
