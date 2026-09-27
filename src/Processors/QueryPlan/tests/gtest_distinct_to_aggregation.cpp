#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Columns/ColumnConst.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/SetSerialization.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/StorageValues.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemPrimes.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

SharedHeader makeSourceHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{{type->createColumn(), type, "k"}});
}

bool convertsDistinctToAggregation(QueryPlanStepPtr source)
{
    QueryPlan plan;
    const auto header = source->getOutputHeader();
    plan.addStep(std::move(source));
    plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, Names{"k"}, false));
    QueryPlanOptimizationSettings settings(getContext().context);
    settings.convert_distinct_to_aggregation = true;
    QueryPlan::Nodes nodes;
    QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);
    return plan.getRootNode()->step->getName() == "Aggregating";
}

QueryTreeNodePtr makeSourceQuery(bool has_bounded_read)
{
    const auto header = makeSourceHeader();
    const auto context = getContext().context;
    StoragePtr storage;
    if (has_bounded_read)
        storage = std::make_shared<StorageValues>(StorageID("test", "finite"),
            ColumnsDescription(header->getNamesAndTypesList()), *header);
    else
        storage = std::make_shared<StorageValues>(StorageID("test", "opaque"),
            ColumnsDescription(header->getNamesAndTypesList()), Pipe(std::make_shared<NullSource>(header)));
    auto query = std::make_shared<QueryNode>(context);
    query->getJoinTreeNode() = std::make_shared<TableNode>(storage, context);
    return query;
}

QueryPlan makePlanWithDeserializedDistinct(bool preliminary)
{
    const auto header = makeSourceHeader();
    DistinctStep distinct(header, SizeLimits{}, /*limit_hint=*/0, Names{"k"}, preliminary);

    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    IQueryPlanStep::Serialization serialization{out, serialized_sets};
    serialization.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    distinct.serialize(serialization);
    QueryPlanSerializationSettings settings;
    distinct.serializeSettings(settings, serialization.version);

    ReadBufferFromString in(out.str());
    DeserializedSetsRegistry deserialized_sets;
    ContextPtr context = getContext().context;
    SharedHeaders input_headers{header};
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, context, input_headers, header, settings, 0, serialization.version, false};

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadNothingStep>(header));
    plan.addStep(DistinctStep::deserialize(deserialization, preliminary));
    return plan;
}

QueryPlanStepPtr roundTripAggregation(const AggregatingStep & step, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialized_sets;
    IQueryPlanStep::Serialization serialization{out, serialized_sets};
    serialization.version = version;
    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    settings.writeChangedBinary(out);
    step.serialize(serialization);

    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings restored_settings;
    restored_settings.readBinary(in);
    DeserializedSetsRegistry deserialized_sets;
    ContextPtr context = getContext().context;
    IQueryPlanStep::Deserialization deserialization{
        in, deserialized_sets, {}, context, step.getInputHeaders(), step.getOutputHeader(), restored_settings, 0, version, false};
    auto restored = AggregatingStep::deserialize(deserialization);
    EXPECT_TRUE(in.eof());
    return restored;
}

}

TEST(DistinctToAggregation, DeserializedStepUsesOptimizationSettings)
{
    for (bool enabled : {false, true})
    {
        for (bool preliminary : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "enabled=" << enabled << ", preliminary=" << preliminary);
            auto plan = makePlanWithDeserializedDistinct(preliminary);
            QueryPlanOptimizationSettings settings(getContext().context);
            settings.convert_distinct_to_aggregation = enabled;
            QueryPlan::Nodes nodes;
            QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);
            EXPECT_EQ(plan.getRootNode()->step->getName(), enabled && !preliminary ? "Aggregating" : "Distinct");
        }
    }
}

TEST(DistinctToAggregation, PreservesExplicitSortDescription)
{
    for (bool distinct_in_order : {false, true})
    {
        SCOPED_TRACE(::testing::Message() << "distinct_in_order=" << distinct_in_order);
        const auto header = makeSourceHeader();
        SortDescription sort_description;
        sort_description.emplace_back("k", 1, 1);
        auto distinct = std::make_unique<DistinctStep>(header, SizeLimits{}, 0, Names{"k"}, false);
        distinct->applyOrder(sort_description);

        QueryPlan plan;
        plan.addStep(std::make_unique<ReadNothingStep>(header));
        plan.addStep(std::move(distinct));

        QueryPlanOptimizationSettings settings(getContext().context);
        settings.convert_distinct_to_aggregation = true;
        settings.distinct_in_order = distinct_in_order;
        QueryPlan::Nodes nodes;
        QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);

        ASSERT_EQ(plan.getRootNode()->step->getName(), "Distinct");
        EXPECT_EQ(plan.getRootNode()->step->getSortDescription(), sort_description);
    }
}

TEST(DistinctToAggregation, DuplicateColumnsRequireIdenticalValues)
{
    enum class DuplicateColumns
    {
        RepeatedOutput,
        AliasedOutput,
        DifferentOutputs,
        PassthroughOutput,
        PassthroughInputs,
    };

    for (auto duplicate_columns : {DuplicateColumns::RepeatedOutput, DuplicateColumns::AliasedOutput,
             DuplicateColumns::DifferentOutputs, DuplicateColumns::PassthroughOutput, DuplicateColumns::PassthroughInputs})
    {
        for (bool preliminary : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "duplicate_columns=" << static_cast<int>(duplicate_columns)
                                             << ", preliminary=" << preliminary);
            auto type = std::make_shared<DataTypeUInt64>();
            Block input_header{{type->createColumn(), type, "x"}, {type->createColumn(), type, "y"}};
            ActionsDAG actions;
            const auto & x = actions.addInput("x", type);
            switch (duplicate_columns)
            {
                case DuplicateColumns::RepeatedOutput:
                    actions.getOutputs() = {&x, &x};
                    break;
                case DuplicateColumns::AliasedOutput:
                    actions.getOutputs() = {&actions.addAlias(x, "k"), &actions.addAlias(x, "k")};
                    break;
                case DuplicateColumns::DifferentOutputs:
                {
                    const auto & y = actions.addInput("y", type);
                    actions.getOutputs() = {&actions.addAlias(x, "k"), &actions.addAlias(y, "k")};
                    break;
                }
                case DuplicateColumns::PassthroughOutput:
                    actions.getOutputs() = {&actions.addAlias(x, "y")};
                    break;
                case DuplicateColumns::PassthroughInputs:
                    input_header.insert({type->createColumn(), type, "y"});
                    actions.getOutputs() = {&x};
                    break;
            }

            QueryPlan plan;
            plan.addStep(std::make_unique<ReadNothingStep>(std::make_shared<const Block>(std::move(input_header))));
            plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(actions)));
            const auto header = plan.getCurrentHeader();
            if (preliminary)
                plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, header->getNames(), true));
            plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, header->getNames(), false));

            QueryPlanOptimizationSettings settings(getContext().context);
            settings.convert_distinct_to_aggregation = true;
            QueryPlan::Nodes nodes;
            QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);
            const bool can_convert = duplicate_columns == DuplicateColumns::RepeatedOutput
                || duplicate_columns == DuplicateColumns::AliasedOutput;
            EXPECT_EQ(plan.getRootNode()->step->getName(), can_convert ? "Aggregating" : "Distinct");
            EXPECT_TRUE(blocksHaveEqualStructure(*plan.getCurrentHeader(), *header));
        }
    }
}

TEST(DistinctToAggregation, ConstantColumnsPreserveOutputHeader)
{
    for (bool all_constant : {false, true})
    {
        for (const Names & keys : {Names{"c", "k"}, Names{"k"}, Names{}, Names{"c"}})
        {
            SCOPED_TRACE(::testing::Message() << "all_constant=" << all_constant << ", keys=" << ::testing::PrintToString(keys));
            auto type = std::make_shared<DataTypeUInt64>();
            ColumnPtr key_column = type->createColumn();
            if (all_constant)
                key_column = type->createColumnConst(0, UInt64(3));
            auto header = std::make_shared<const Block>(Block{
                {type->createColumnConst(0, UInt64(7)), type, "c"}, {key_column, type, "k"}});
            QueryPlan plan;
            plan.addStep(std::make_unique<ReadNothingStep>(header));
            plan.addStep(std::make_unique<DistinctStep>(header, SizeLimits{}, 0, keys, false));

            QueryPlanOptimizationSettings settings(getContext().context);
            settings.convert_distinct_to_aggregation = true;
            QueryPlan::Nodes nodes;
            QueryPlanOptimizations::applyOrder(settings, *plan.getRootNode(), nodes);

            const auto & root = *plan.getRootNode();
            const bool can_convert = !all_constant && keys != Names{"c"};
            EXPECT_EQ(root.step->getName(), can_convert ? "Expression" : "Distinct");
            EXPECT_TRUE(blocksHaveEqualStructure(*root.step->getOutputHeader(), *header));
            if (can_convert)
            {
                const auto * aggregation = typeid_cast<const AggregatingStep *>(root.children.front()->step.get());
                ASSERT_NE(aggregation, nullptr);
                EXPECT_EQ(aggregation->getParams().keys, Names{"k"});
            }
        }
    }
}

TEST(DistinctToAggregation, PreparedSourceProperties)
{
    for (bool bounded : {false, true})
    {
        for (bool totals : {false, true})
        {
            for (bool extremes : {false, true})
            {
                SCOPED_TRACE(::testing::Message() << "bounded=" << bounded << ", totals=" << totals << ", extremes=" << extremes);
                const auto header = makeSourceHeader();
                Pipe pipe(std::make_shared<NullSource>(header));
                if (totals)
                    pipe.addTotalsSource(std::make_shared<NullSource>(header));
                if (extremes)
                    pipe.addExtremesSource(std::make_shared<NullSource>(header));
                EXPECT_EQ(convertsDistinctToAggregation(std::make_unique<ReadFromPreparedSource>(std::move(pipe), bounded)),
                    bounded && !totals && !extremes);
            }
        }
    }
}

TEST(DistinctToAggregation, RemoteSourceProperties)
{
    for (const auto & [query_tree, bounded] : std::vector<std::pair<QueryTreeNodePtr, bool>>{
             {nullptr, false}, {makeSourceQuery(false), false}, {makeSourceQuery(true), true}})
    {
        for (bool totals : {false, true})
        {
            for (bool extremes : {false, true})
            {
                for (auto stage : {QueryProcessingStage::FetchColumns, QueryProcessingStage::Complete})
                {
                    SCOPED_TRACE(::testing::Message() << "bounded=" << bounded << ", totals=" << totals
                        << ", extremes=" << extremes << ", stage=" << stage);
                    auto context = Context::createCopy(getContext().context);
                    context->setSetting("extremes", extremes);
                    ParserSelectQuery parser;
                    auto query = parseQuery(parser,
                        totals ? "SELECT k FROM test.source GROUP BY k WITH TOTALS" : "SELECT k FROM test.source", 0, 0, 0);
                    ClusterProxy::SelectStreamFactory::Shards shards;
                    for (const auto & shard_query_tree : {makeSourceQuery(true), query_tree})
                        shards.push_back({
                            .query = query,
                            .query_tree = shard_query_tree,
                            .planner_context = {},
                            .query_plan = {},
                            .main_table = StorageID("test", "source"),
                            .header = makeSourceHeader(),
                            .shard_info = {},
                        });
                    auto source = std::make_unique<ReadFromRemote>(std::move(shards), makeSourceHeader(), stage,
                        StorageID("test", "source"), nullptr, context, nullptr, Scalars{}, Tables{},
                        getLogger("DistinctToAggregationTest"), 2, nullptr, "test");
                    EXPECT_EQ(convertsDistinctToAggregation(std::move(source)),
                        bounded && (stage != QueryProcessingStage::Complete || (!totals && !extremes)));
                }
            }
        }
    }
}

TEST(DistinctToAggregation, ParallelReplicaSourceProperties)
{
    ParserSelectQuery parser;
    auto query = parseQuery(parser, "SELECT k FROM test.source", 0, 0, 0);
    auto context = Context::createCopy(getContext().context);
    auto cluster = std::make_shared<Cluster>(context->getSettingsRef(), HostsByShard{{"127.0.0.1:9000"}},
        ClusterConnectionParameters{"default", "", 9000, true, true, false, "", Priority{1}, "test", ""});
    for (const auto & [query_tree, bounded] : std::vector<std::pair<QueryTreeNodePtr, bool>>{
             {nullptr, false}, {makeSourceQuery(false), false}, {makeSourceQuery(true), true}})
    {
        auto source = std::make_unique<ReadFromParallelRemoteReplicasStep>(query,
            query_tree, nullptr, cluster, StorageID("test", "source"), nullptr, makeSourceHeader(),
            QueryProcessingStage::FetchColumns, context, nullptr, Scalars{}, Tables{},
            getLogger("DistinctToAggregationTest"), nullptr, std::vector<ConnectionPoolPtr>{});
        EXPECT_EQ(convertsDistinctToAggregation(std::move(source)), bounded);
    }
}

TEST(DistinctToAggregation, TableFunctionArgumentsAreNotInputSources)
{
    const auto context = getContext().context;
    ParserSelectQuery parser;
    auto query = buildQueryTree(parseQuery(parser,
        "SELECT * FROM viewExplain('AST', '', (SELECT * FROM numbers(10)))", 0, 0, 0), context);
    auto & function = query->as<QueryNode &>().getJoinTreeNode()->as<TableFunctionNode &>();

    for (bool bounded : {false, true})
    {
        const auto source = makeSourceQuery(bounded);
        const auto storage = source->as<QueryNode &>().getJoinTreeNode()->as<TableNode &>().getStorage();
        function.resolve(nullptr, storage, context, {0, 1, 2});
        EXPECT_EQ(hasBoundedInput(query), bounded);
    }
}

TEST(DistinctToAggregation, QueryInputBounds)
{
    auto context = getContext().context;
    auto query = makeSourceQuery(true);
    EXPECT_TRUE(hasBoundedInput(query));
    query->as<QueryNode &>().setIsRecursiveWith(true);
    EXPECT_FALSE(hasBoundedInput(query));
    auto union_node = std::make_shared<UnionNode>(context, SelectUnionMode::UNION_ALL);
    union_node->getQueries().getNodes() = {makeSourceQuery(true), makeSourceQuery(true)};
    EXPECT_TRUE(hasBoundedInput(union_node));
    union_node->getQueries().getNodes().push_back(makeSourceQuery(false));
    EXPECT_FALSE(hasBoundedInput(union_node));
    union_node->getQueries().getNodes().pop_back();
    union_node->setIsRecursiveCTE(true);
    EXPECT_FALSE(hasBoundedInput(union_node));

    auto table = makeSourceQuery(true)->as<QueryNode &>().getJoinTreeNode();
    table->as<TableNode &>().getTableExpressionModifiers() = TableExpressionModifiers(false, {}, {}, StreamSettings{});
    EXPECT_FALSE(hasBoundedInput(table));

    for (bool bounded : {false, true})
    {
        const std::optional<UInt64> limit = bounded ? std::optional<UInt64>(100) : std::nullopt;
        const StorageID storage_id("test", "source");
        EXPECT_EQ(hasBoundedInput(std::make_shared<TableNode>(
            std::make_shared<StorageSystemNumbers>(storage_id, true, "k",
                bounded ? std::optional<UInt128>(100) : std::nullopt), context)), bounded);
        EXPECT_EQ(hasBoundedInput(std::make_shared<TableNode>(
            std::make_shared<StorageSystemPrimes>(storage_id, "k", limit), context)), bounded);
        const auto source = makeSourceQuery(bounded);
        const auto storage = source->as<QueryNode &>().getJoinTreeNode()->as<TableNode &>().getStorage();
        auto function = std::make_shared<TableFunctionNode>("values");
        function->resolve(nullptr, storage, context, {});
        EXPECT_EQ(hasBoundedInput(function), bounded);
        function->getTableExpressionModifiers() = TableExpressionModifiers(false, {}, {}, StreamSettings{});
        EXPECT_FALSE(hasBoundedInput(function));
    }
}

TEST(DistinctToAggregation, DistinctLimitsSurviveSerialization)
{
    for (const auto & limits : {SizeLimits{}, SizeLimits{100, 1000000, OverflowMode::THROW}})
    {
        SCOPED_TRACE(::testing::Message() << "max_rows=" << limits.max_rows << ", max_bytes=" << limits.max_bytes);
        const auto header = makeSourceHeader();
        QueryPlan plan;
        plan.addStep(std::make_unique<ReadNothingStep>(header));
        plan.addStep(std::make_unique<DistinctStep>(header, limits, 0, Names{"k"}, false));
        QueryPlanOptimizationSettings settings(getContext().context);
        QueryPlan::Nodes nodes;
        ASSERT_TRUE(QueryPlanOptimizations::tryConvertDistinctToAggregation(*plan.getRootNode(), nodes, settings));

        const auto & step = static_cast<const AggregatingStep &>(*plan.getRootNode()->step);
        EXPECT_TRUE(step.isSerializable());
        EXPECT_THROW(roundTripAggregation(step,
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_AGGREGATION_LIMITS - 1), Exception);
        auto restored = roundTripAggregation(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
        const auto & params = static_cast<const AggregatingStep &>(*restored).getParams();
        EXPECT_EQ(params.max_rows_to_group_by, limits.max_rows);
        EXPECT_EQ(params.max_bytes_to_group_by, limits.max_bytes);
        EXPECT_EQ(params.limit_errors, step.getParams().limit_errors);
        EXPECT_EQ(params.max_bytes_before_external_group_by, 0);
        EXPECT_EQ(params.enable_adaptive_aggregator, step.getParams().enable_adaptive_aggregator);
    }
}

TEST(DistinctToAggregation, DefaultAggregationLimitsSurviveSerialization)
{
    Aggregator::Params params(Names{"k"}, AggregateDescriptions{}, /*overflow_row=*/false,
        /*max_threads=*/1, /*max_block_size=*/1000, /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5,
        /*serialize_string_with_zero_byte=*/false, /*enable_packed_string_keys=*/true);
    const AggregatingStep step(makeSourceHeader(), std::move(params), GroupingSetsParamsList{},
        /*final=*/true, /*max_block_size=*/1000, /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/1, /*temporary_data_merge_threads=*/1, /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false, SortDescription{}, SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false);

    for (UInt64 version : {DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_AGGREGATION_LIMITS - 1,
             DBMS_QUERY_PLAN_SERIALIZATION_VERSION})
    {
        SCOPED_TRACE(version);
        auto restored = roundTripAggregation(step, version);
        const auto & restored_params = static_cast<const AggregatingStep &>(*restored).getParams();
        EXPECT_EQ(restored_params.max_bytes_to_group_by, 0);
        EXPECT_EQ(restored_params.limit_errors, Aggregator::Params::LimitErrors{});
        EXPECT_TRUE(restored_params.only_merge);
    }
}
