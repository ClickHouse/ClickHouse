#include <gtest/gtest.h>

#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTFunction.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StorageMergeTree.h>

#include <Poco/TemporaryFile.h>

TEST(MergeTreeDataPart, ExistingBytesOnDiskAfterLightweightDelete)
{
    using namespace DB;

    MainThreadStatus::getInstance();
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();
    getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    auto context = Context::createCopy(getContext().context);
    StorageInMemoryMetadata metadata;
    metadata.setColumns({{"id", std::make_shared<DataTypeUInt64>()}});
    metadata.sorting_key = KeyDescription::getKeyFromAST(makeASTFunction("tuple"), metadata.columns, {}, context);
    metadata.primary_key = metadata.sorting_key;
    metadata.primary_key.definition_ast = nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

    auto settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    settings->set("exclude_deleted_rows_for_part_size_in_merge", true);
    Poco::TemporaryFile table_dir(".");
    auto storage = std::make_shared<StorageMergeTree>(
        StorageID("test_db", "existing_bytes_on_disk"), table_dir.path() + "/", metadata, LoadingStrictnessLevel::ATTACH,
        context, "", MergeTreeData::MergingParams{}, std::move(settings));

    const String part_name = "all_1_1_0";
    auto part = MergeTreeDataPartBuilder(
        *storage, part_name, storage->getStoragePolicy()->getVolume(0), storage->getRelativeDataPath(),
        part_name, context->getReadSettings(), PartDirIntent::CreateFresh)
        .withPartFormat({MergeTreeDataPartType::Wide, MergeTreeDataPartStorageType::Full})
        .build();
    auto columns = metadata.getColumns().getAllPhysical();
    columns.emplace_back(RowExistsColumn::name, RowExistsColumn::type);
    part->setColumns(columns, SerializationInfoByName(SerializationInfoSettings{}), 0);

    auto existing_bytes = [&](UInt64 bytes, size_t total_rows, std::optional<size_t> existing_rows)
    {
        part->setBytesOnDisk(bytes);
        part->rows_count = total_rows;
        part->existing_rows_count = existing_rows;
        return part->getExistingBytesOnDisk();
    };

    EXPECT_EQ(existing_bytes(100ULL << 30, 40'000'000'000, 39'999'999'999), 107'374'182'397ULL);
    constexpr auto max = std::numeric_limits<UInt64>::max();
    EXPECT_EQ(existing_bytes(max, max, max - 1), max - 1);
    EXPECT_EQ(existing_bytes(11, 3, 2), 7u);
    EXPECT_EQ(existing_bytes(11, 3, 0), 0u);
    EXPECT_EQ(existing_bytes(11, 3, 3), 11u);
    EXPECT_EQ(existing_bytes(11, 0, 0), 11u);
    EXPECT_EQ(existing_bytes(11, 3, std::nullopt), 11u);

    storage->flushAndShutdown();
}
