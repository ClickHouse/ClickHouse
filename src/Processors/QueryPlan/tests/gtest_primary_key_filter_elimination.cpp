#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseMemory.h>
#include <Functions/FunctionFactory.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/Util/MapConfiguration.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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

namespace
{

class PrimaryKeyFilterEliminationTest : public ::testing::Test
{
protected:
    ContextMutablePtr context;
    DatabasePtr database;
    std::shared_ptr<StorageMergeTree> storage;
    StorageMetadataHandle metadata_handle;
    StorageSnapshotPtr storage_snapshot;
    String table_name;

    void SetUp() override
    {
        context = Context::createCopy(getContext().context);
        /// Key analysis initializes SQL functions from the global configuration, including when
        /// this test runs alone without a server application or another test initializing it.
        context->setConfig(Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration));
        context->setSetting("compile_expressions", Field(false));
        context->setSetting("compile_aggregate_expressions", Field(false));
        context->setSetting("use_query_condition_cache", Field(false));
        MainThreadStatus::getInstance();
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getMergeTreePrefixesDeserializationThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        StorageInMemoryMetadata metadata;
        ColumnsDescription columns;
        columns.add(ColumnDescription("k", std::make_shared<DataTypeUInt64>()));
        columns.add(ColumnDescription("amount", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(columns);

        ASTPtr order_by = make_intrusive<ASTIdentifier>("k");
        metadata.sorting_key = KeyDescription::getKeyFromAST(order_by, metadata.columns, {}, context);
        metadata.primary_key = KeyDescription::getKeyFromAST(order_by, metadata.columns, {}, context);
        metadata.primary_key.definition_ast = nullptr;
        metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, {}, context);
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, metadata.getColumnsRequiredForPartitionKey(), metadata.primary_key, &metadata.partition_key, context));

        auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
        storage_settings->set("index_granularity", Field(UInt64(8)));
        storage_settings->set("index_granularity_bytes", Field(UInt64(0)));
        storage_settings->set("min_rows_for_wide_part", Field(UInt64(0)));
        storage_settings->set("min_bytes_for_wide_part", Field(UInt64(0)));

        /// A fresh path also makes repeated gtest runs independent of previously written parts.
        table_name = "t_" + toString(UUIDHelpers::generateV4());
        constexpr auto database_name = "primary_key_filter_elimination_test_db";
        storage = std::make_shared<StorageMergeTree>(
            StorageID(database_name, table_name),
            "store/test_primary_key_filter_elimination/" + table_name + "/",
            metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/"",
            MergeTreeData::MergingParams{},
            std::move(storage_settings));

        database = DatabaseCatalog::instance().tryGetDatabase(database_name);
        if (!database)
        {
            database = std::make_shared<DatabaseMemory>(database_name, context);
            DatabaseCatalog::instance().attachDatabase(database_name, database);
        }
        database->attachTable(context, table_name, storage, {});
        metadata_handle = storage->getInMemoryMetadataPtr(context, false);

        auto type = std::make_shared<DataTypeUInt64>();
        auto key = type->createColumn();
        auto amount = type->createColumn();
        for (UInt64 value = 0; value < 256; ++value)
        {
            key->insert(value);
            amount->insert(value);
        }
        Block block{
            ColumnWithTypeAndName(std::move(key), type, "k"),
            ColumnWithTypeAndName(std::move(amount), type, "amount")};
        auto sink = storage->write(nullptr, metadata_handle, context, /*async_insert=*/false);
        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(block)));
        pipe.addSimpleTransform([](const SharedHeader & header) -> ProcessorPtr
        {
            return std::make_shared<AddDeduplicationInfoTransform>(header);
        });
        QueryPipeline pipeline(std::move(pipe));
        pipeline.complete(sink);
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
        storage_snapshot = storage->getStorageSnapshot(metadata_handle, context);
    }

    void TearDown() override
    {
        if (storage)
        {
            storage->flushAndShutdown();
            storage_snapshot.reset();
            storage->drop();
            if (database && database->isTableExist(table_name, context))
                database->detachTable(context, table_name);
        }
    }

    const ActionsDAG::Node & compare(ActionsDAG & dag, const String & function, UInt64 bound) const
    {
        auto type = std::make_shared<DataTypeUInt64>();
        const auto & constant = dag.addColumn(type->createColumnConst(1, bound), type, toString(bound));
        return dag.addFunction(
            FunctionFactory::instance().get(function, context), {&dag.findInOutputs("k"), &constant}, function + toString(bound));
    }

    QueryPlanStepPtr makeRead() const
    {
        ActionsDAG broad_filter(NamesAndTypesList{{"k", std::make_shared<DataTypeUInt64>()}});
        const auto & predicate = compare(broad_filter, "less", 243);
        broad_filter.getOutputs() = {&predicate};

        SelectQueryInfo query_info;
        query_info.table_expression_modifiers.emplace(/*has_final=*/false, std::nullopt, std::nullopt);
        query_info.filter_actions_dag = std::make_shared<const ActionsDAG>(std::move(broad_filter));
        const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
        MergeTreeDataSelectExecutor executor(*storage);
        return executor.readFromParts(
            snapshot_data.parts, snapshot_data.mutations_snapshot, Names{"k", "amount"}, storage_snapshot,
            query_info, context, /*max_block_size=*/7, /*num_streams=*/1);
    }

    std::unique_ptr<AggregatingStep> makeAggregate(const SharedHeader & header) const
    {
        AggregateFunctionProperties properties;
        AggregateDescription sum;
        sum.function = AggregateFunctionFactory::instance().get(
            "sum", NullsAction::EMPTY, {std::make_shared<DataTypeUInt64>()}, {}, properties);
        sum.argument_names = {"amount"};
        sum.column_name = "sum(amount)";
        Aggregator::Params params(
            Names{}, AggregateDescriptions{sum}, /*overflow_row=*/false, /*max_threads=*/1,
            /*max_block_size=*/7, /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
            /*serialize_string_with_zero_byte=*/false, /*enable_packed_string_keys=*/true);
        params.only_merge = false;
        return std::make_unique<AggregatingStep>(
            header, std::move(params), GroupingSetsParamsList{}, /*final=*/true, /*max_block_size=*/7,
            /*aggregation_in_order_max_block_bytes=*/0, /*merge_threads=*/1, /*temporary_data_merge_threads=*/1,
            /*storage_has_evenly_distributed_read=*/false, /*group_by_use_nulls=*/false,
            SortDescription{}, SortDescription{}, /*should_produce_results_in_order_of_bucket_number=*/false,
            /*memory_bound_merging_of_aggregation_results_enabled=*/false,
            /*explicit_sorting_required_for_aggregation_in_order=*/false);
    }
};

}

TEST_F(PrimaryKeyFilterEliminationTest, ReanalyzesExactRangesForTighterRuntimeFilter)
{
    auto read_step = makeRead();
    auto * read = typeid_cast<ReadFromMergeTree *>(read_step.get());
    ASSERT_NE(read, nullptr);

    /// Analyze a weaker pruning predicate on the same read that the optimizer will later use.
    /// These proofs certify boundary rows of the tighter runtime filter, so reusing them would
    /// incorrectly bypass row filtering. Separate SQL queries cannot exercise this state reuse.
    auto broad_analysis = read->selectRangesToRead(/*find_exact_ranges=*/true);
    ASSERT_TRUE(broad_analysis->isUsable());
    ASSERT_EQ(broad_analysis->parts_with_ranges.size(), 1);
    ASSERT_FALSE(broad_analysis->parts_with_ranges.front().exact_ranges.empty());
    ASSERT_EQ(read->getOrCreateAnalyzedResult(), broad_analysis);

    QueryPlan plan;
    plan.addStep(std::move(read_step));
    ASSERT_EQ(plan.getRootNode()->step.get(), read);
    ActionsDAG runtime_filter(plan.getCurrentHeader()->getColumnsWithTypeAndName());
    const auto & lower = compare(runtime_filter, "greaterOrEquals", 13);
    const auto & upper = compare(runtime_filter, "less", 115);
    const auto & predicate = runtime_filter.addFunction(
        FunctionFactory::instance().get("and", context), {&lower, &upper}, "filter");
    const auto * amount = &runtime_filter.findInOutputs("amount");
    runtime_filter.getOutputs() = {amount, &predicate};
    plan.addStep(std::make_unique<FilterStep>(
        plan.getCurrentHeader(), std::move(runtime_filter), "filter", /*remove_filter_column=*/true));
    plan.addStep(makeAggregate(plan.getCurrentHeader()));

    /// Invoke the production pass directly so earlier optimization passes cannot replace the
    /// deliberately pre-analyzed read. Transfer all nodes back into an ordinary executable plan.
    auto * root = plan.getRootNode();
    auto [nodes, resources] = QueryPlan::detachNodesAndResources(std::move(plan));
    QueryPlanOptimizationSettings optimization_settings(context);
    ASSERT_TRUE(QueryPlanOptimizations::optimizePrimaryKeyFilterElimination(*root, nodes, optimization_settings));
    auto optimized_plan = QueryPlan::extractSubplan(root, nodes);
    optimized_plan.addResources(std::move(resources));
    auto builder = optimized_plan.buildQueryPipeline(
        optimization_settings, BuildQueryPipelineSettings(context), /*do_optimize=*/false);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    PullingPipelineExecutor executor(pipeline);

    size_t rows = 0;
    UInt64 result = 0;
    Block block;
    while (executor.pull(block))
    {
        rows += block.rows();
        for (size_t row = 0; row < block.rows(); ++row)
            result += block.getByName("sum(amount)").column->getUInt(row);
    }
    EXPECT_EQ(rows, 1);
    /// Only 13..114 match; accepting the two boundary granules instead includes 8..119 (7112).
    EXPECT_EQ(result, 6477);
}
