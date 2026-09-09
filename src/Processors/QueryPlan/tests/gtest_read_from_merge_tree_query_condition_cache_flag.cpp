#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SetSerialization.h>
#include <Parsers/ASTFunction.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// A `MergeTree` table with a single `UInt64` column and `ORDER BY tuple()`, attached to its own
/// in-memory database: `ReadFromMergeTree::deserialize` resolves the table through
/// `DatabaseCatalog`, so the round-trip needs the table to be reachable by name.
struct TableFixture
{
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> storage;
    StorageMetadataHandle metadata_handle;
    StorageSnapshotPtr storage_snapshot;

    /// Unique name: the `DatabaseCatalog` is process-wide and shared with the other gtests in this
    /// binary, so a generic name like "test" would collide with whichever of them runs first.
    static constexpr auto database_name = "read_from_merge_tree_qcc_flag_test_db";

    TableFixture()
        : context(Context::createCopy(getContext().context))
    {
        MainThreadStatus::getInstance();
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        StorageInMemoryMetadata metadata;

        ColumnsDescription columns;
        columns.add(ColumnDescription("k", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(columns);

        auto order_by_ast = makeASTFunction("tuple");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

        auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

        /// `ATTACH` skips the sanity checks a fresh `CREATE` would run against the (empty) data path.
        storage = std::make_shared<StorageMergeTree>(
            StorageID(database_name, "t"),
            "store/test_read_from_merge_tree_qcc_flag/",
            metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/"",
            MergeTreeData::MergingParams{},
            std::move(storage_settings));

        /// `ReadFromMergeTree::deserialize` looks the table up by name, so it must resolve to *this*
        /// fixture's storage: every test builds a fresh one, and the previous instance has already been
        /// shut down by then.
        DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
        {
            database = std::make_shared<DatabaseMemory>(database_name, context);
            DatabaseCatalog::instance().attachDatabase(database_name, database);
        }
        else if (database->isTableExist("t", context))
        {
            database->detachTable(context, "t");
        }
        database->attachTable(context, "t", storage, {});

        /// The handle owns the metadata; converting a temporary handle to `StorageMetadataPtr` is
        /// deleted, so it has to outlive the snapshot call.
        metadata_handle = storage->getInMemoryMetadataPtr(context, false);

        /// One row, hence one part: `readFromParts` (used by both the fixture and
        /// `ReadFromMergeTree::deserialize`) short-circuits to no step at all for a part-less read,
        /// so an empty table could not carry a round-trip.
        insertOneRow();

        storage_snapshot = storage->getStorageSnapshot(metadata_handle, context);
    }

    void insertOneRow()
    {
        auto type = std::make_shared<DataTypeUInt64>();
        auto column = type->createColumn();
        column->insert(Field(UInt64(1)));
        Block block{ColumnWithTypeAndName(std::move(column), type, "k")};

        auto sink = storage->write(nullptr, metadata_handle, context, /*async_insert=*/false);

        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(block)));
        /// The `MergeTree` sink reads the deduplication info off the chunk, so the insert chain that
        /// normally feeds it (`InsertDependenciesBuilder`) has to be stood in for here.
        pipe.addSimpleTransform([](const SharedHeader & header) -> ProcessorPtr
        { return std::make_shared<AddDeduplicationInfoTransform>(header); });

        QueryPipeline pipeline(std::move(pipe));
        pipeline.complete(sink);
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }

    ~TableFixture() { storage->flushAndShutdown(); }

    /// A read of the (empty) table, built through the same `readFromParts` entry point the
    /// deserializer uses, so both sides of the round-trip are shaped identically.
    std::unique_ptr<ReadFromMergeTree> makeRead()
    {
        const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);

        /// Exactly what `ReadFromMergeTree::deserialize` reconstructs, so the round-trip compares
        /// like with like. Modifiers also stand in for the query AST: `SelectQueryInfo::isFinal`
        /// falls back to dereferencing `query` when they are absent.
        SelectQueryInfo query_info;
        query_info.table_expression_modifiers.emplace(/*has_final=*/false, std::nullopt, std::nullopt);

        MergeTreeDataSelectExecutor executor(*storage);
        auto step = executor.readFromParts(
            snapshot_data.parts,
            snapshot_data.mutations_snapshot,
            Names{"k"},
            storage_snapshot,
            query_info,
            context,
            /*max_block_size=*/DEFAULT_BLOCK_SIZE,
            /*num_streams=*/1);

        auto * read = typeid_cast<ReadFromMergeTree *>(step.get());
        if (!read)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "readFromParts did not produce a ReadFromMergeTree step");

        step.release();
        return std::unique_ptr<ReadFromMergeTree>(read);
    }
};

String serializeRead(const ReadFromMergeTree & read, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    read.serialize(ctx);
    return out.str();
}

std::unique_ptr<ReadFromMergeTree> deserializeRead(const String & bytes, const TableFixture & fixture, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    SharedHeaders input_headers;
    SharedHeader output_header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>()->createColumn(), std::make_shared<DataTypeUInt64>(), "k")});
    ContextPtr context = fixture.context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, version, false};

    auto step = ReadFromMergeTree::deserialize(ctx);
    auto * read = typeid_cast<ReadFromMergeTree *>(step.get());
    if (!read)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "deserialize did not produce a ReadFromMergeTree step");

    step.release();
    return std::unique_ptr<ReadFromMergeTree>(read);
}

constexpr UInt64 too_old_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_QUERY_CONDITION_CACHE_FLAG - 1;

}

/// The optimizer disables the query condition cache of a read for *correctness*, not performance
/// (lazy `FINAL`, vector search, hand-built lookup filters). That decision travels as flag bit 64,
/// which only a peer at
/// `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_QUERY_CONDITION_CACHE_FLAG` or above understands:
/// an older peer would ignore the bit and rebuild the read with the cache enabled. Such a read must
/// therefore fail closed rather than be shipped.
TEST(ReadFromMergeTreeQueryConditionCacheFlag, SerializationRejectsOlderPeerWhenCacheIsDisabled)
{
    TableFixture fixture;

    auto read = fixture.makeRead();
    read->disableQueryConditionCache();

    EXPECT_THROW(serializeRead(*read, too_old_version), Exception);
    EXPECT_NO_THROW(serializeRead(*read, DBMS_QUERY_PLAN_SERIALIZATION_VERSION));
}

/// The gate is specific to the disabled case: an ordinary read (cache allowed) carries no new
/// information and stays shippable to any peer, so the version bump must not break plain reads.
TEST(ReadFromMergeTreeQueryConditionCacheFlag, SerializationAllowsOlderPeerWhenCacheIsEnabled)
{
    TableFixture fixture;

    auto read = fixture.makeRead();

    EXPECT_NO_THROW(serializeRead(*read, too_old_version));
    EXPECT_NO_THROW(serializeRead(*read, DBMS_QUERY_PLAN_SERIALIZATION_VERSION));
}

/// `allow_query_condition_cache` is private and has no getter, so the restored step's value is
/// observed the same way the flag is enforced: re-serializing it for a peer below the gate can only
/// throw if the disable survived the round-trip. Re-serializing at the current version must also
/// reproduce the original bytes, which pins the whole wire layout of the flag, not just the gate.
TEST(ReadFromMergeTreeQueryConditionCacheFlag, RoundTripRestoresDisabledCache)
{
    TableFixture fixture;

    auto read = fixture.makeRead();
    read->disableQueryConditionCache();

    String bytes = serializeRead(*read, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto restored = deserializeRead(bytes, fixture, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    EXPECT_THROW(serializeRead(*restored, too_old_version), Exception);
    EXPECT_EQ(serializeRead(*restored, DBMS_QUERY_PLAN_SERIALIZATION_VERSION), bytes);
}

/// The mirror case: a read whose cache was never disabled must not come back disabled, otherwise
/// every distributed read would silently lose the cache.
TEST(ReadFromMergeTreeQueryConditionCacheFlag, RoundTripKeepsEnabledCache)
{
    TableFixture fixture;

    auto read = fixture.makeRead();

    String bytes = serializeRead(*read, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto restored = deserializeRead(bytes, fixture, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    EXPECT_NO_THROW(serializeRead(*restored, too_old_version));
    EXPECT_EQ(serializeRead(*restored, DBMS_QUERY_PLAN_SERIALIZATION_VERSION), bytes);
}
