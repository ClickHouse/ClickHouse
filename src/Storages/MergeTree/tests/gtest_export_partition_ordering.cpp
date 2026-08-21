#include <gtest/gtest.h>
#include <sstream>
#include <Storages/ExportReplicatedMergeTreePartitionTaskEntry.h>
#include <Storages/MergeTree/ExportPartitionUtils.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMergeTreePartExportSchemaMismatchMode export_merge_tree_part_schema_mismatch_mode;
}

namespace
{
    ExportReplicatedMergeTreePartitionManifest makeValidManifest()
    {
        ExportReplicatedMergeTreePartitionManifest manifest;
        manifest.transaction_id = "tx1";
        manifest.query_id = "query1";
        manifest.partition_id = "2020";
        manifest.destination_database = "db1";
        manifest.destination_table = "table1";
        manifest.source_replica = "r1";
        manifest.number_of_parts = 1;
        manifest.create_time = 1000;
        manifest.task_timeout_seconds = 60;
        manifest.max_threads = 1;
        manifest.parallel_formatting = true;
        manifest.parquet_parallel_encoding = true;
        manifest.max_bytes_per_file = 1000000;
        manifest.max_rows_per_file = 1000;
        manifest.file_already_exists_policy = MergeTreePartExportManifest::FileAlreadyExistsPolicy::error;
        manifest.filename_pattern = "{part_name}";
        return manifest;
    }
}

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

class ExportPartitionManifestBackCompatTest : public ::testing::Test
{
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

    ExportReplicatedMergeTreePartitionTaskEntry entry1{manifest1, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, {}, {}, {}, {}};
    ExportReplicatedMergeTreePartitionTaskEntry entry2{manifest2, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, {}, {}, {}, {}};
    ExportReplicatedMergeTreePartitionTaskEntry entry3{manifest3, ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING, {}, {}, {}, {}};

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


TEST_F(ExportPartitionManifestBackCompatTest, MissingSchemaMismatchModeParsesAsNullopt)
{
    auto manifest = makeValidManifest();
    manifest.schema_mismatch_mode = MergeTreePartExportSchemaMismatchMode::ignore_extra_source_columns_by_position;

    Poco::JSON::Parser parser;
    auto json = parser.parse(manifest.toJsonString()).extract<Poco::JSON::Object::Ptr>();
    json->remove("schema_mismatch_mode");
    std::ostringstream oss;
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);

    auto parsed = ExportReplicatedMergeTreePartitionManifest::fromJsonString(oss.str());
    EXPECT_FALSE(parsed.schema_mismatch_mode.has_value());
}

TEST_F(ExportPartitionManifestBackCompatTest, SchemaMismatchModeRoundTripsForEveryValue)
{
    for (const auto value : magic_enum::enum_values<MergeTreePartExportSchemaMismatchMode>())
    {
        auto manifest = makeValidManifest();
        manifest.schema_mismatch_mode = value;

        auto parsed = ExportReplicatedMergeTreePartitionManifest::fromJsonString(manifest.toJsonString());

        ASSERT_TRUE(parsed.schema_mismatch_mode.has_value()) << "value=" << magic_enum::enum_name(value);
        EXPECT_EQ(*parsed.schema_mismatch_mode, value) << "value=" << magic_enum::enum_name(value);
    }
}

TEST_F(ExportPartitionManifestBackCompatTest, MissingSchemaMismatchModeFallsBackToStrictInWorkerContext)
{
    auto manifest = makeValidManifest();
    ASSERT_FALSE(manifest.schema_mismatch_mode.has_value());

    auto worker_context = ExportPartitionUtils::getContextCopyWithTaskSettings(getContext().context, manifest);

    EXPECT_EQ(
        worker_context->getSettingsRef()[Setting::export_merge_tree_part_schema_mismatch_mode].value,
        MergeTreePartExportSchemaMismatchMode::strict);
}

TEST_F(ExportPartitionManifestBackCompatTest, SchemaMismatchModeAppliedToWorkerContextForEveryValue)
{
    for (const auto value : magic_enum::enum_values<MergeTreePartExportSchemaMismatchMode>())
    {
        auto manifest = makeValidManifest();
        manifest.schema_mismatch_mode = value;

        auto worker_context = ExportPartitionUtils::getContextCopyWithTaskSettings(getContext().context, manifest);

        EXPECT_EQ(
            worker_context->getSettingsRef()[Setting::export_merge_tree_part_schema_mismatch_mode].value,
            value) << "value=" << magic_enum::enum_name(value);
    }
}

}
