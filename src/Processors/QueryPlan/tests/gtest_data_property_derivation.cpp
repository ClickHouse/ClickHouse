#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/DataPropertyDerivation.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <array>

using namespace DB;
using namespace DB::QueryPlanOptimizations;
using DataPropertyColumnSet = DB::QueryPlanOptimizations::ColumnSet;

namespace
{

DataPropertyProvenance lineageProvenance(DataPropertyTransformationKind transformation)
{
    return DataPropertyProvenance::transformation(transformation);
}

void addColumn(Block & header, const String & name, const DataTypePtr & type)
{
    header.insert(ColumnWithTypeAndName(type->createColumn(), type, name));
}

const ActionsDAG::Node &
addOutputFunction(ActionsDAG & dag, const String & function_name, ActionsDAG::NodeRawConstPtrs children, const String & result_name)
{
    auto resolver = FunctionFactory::instance().get(function_name, getContext().context);
    const auto & node = dag.addFunction(resolver, children, result_name);
    dag.addOrReplaceInOutputs(node);
    return node;
}

DataPropertySet propertiesWithUniqueKey(DataPropertyColumnSet key, const Block & header)
{
    DataPropertySet properties;
    properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration(std::move(key)));
    for (size_t position = 0; position < header.columns(); ++position)
        properties.addNonNullColumn({position, header.getByPosition(position).name});
    return properties;
}

DataPropertySet completeProperties(const Block & header)
{
    DataPropertySet properties;
    properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration({{0, header.getByPosition(0).name}}));
    if (header.columns() > 1)
    {
        properties.addFunctionalDependency(
            {{{0, header.getByPosition(0).name}},
             {{1, header.getByPosition(1).name}},
             DataPropertyDependencyKind::Exact,
             DataPropertyProvenance::transformation(DataPropertyTransformationKind::Identity)});
    }
    for (size_t position = 0; position < header.columns(); ++position)
        properties.addNonNullColumn({position, header.getByPosition(position).name});
    properties.addLineage(
        {{0, header.getByPosition(0).name},
         {0, 0, header.getByPosition(0).name},
         ColumnLineageKind::Identity,
         lineageProvenance(DataPropertyTransformationKind::Identity)});
    return properties;
}

class TestSourceStep final : public ISourceStep
{
public:
    explicit TestSourceStep(SharedHeader header)
        : ISourceStep(header)
    {
    }

    String getName() const override { return "DataPropertyTestSource"; }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
    QueryPlanStepPtr clone() const override { return std::make_unique<TestSourceStep>(*this); }
};

std::unique_ptr<AggregatingStep> makeFinalAggregationStep(const SharedHeader & input_header, const Names & keys)
{
    Aggregator::Params params(keys, AggregateDescriptions{}, false, 1, 65536, 0.5f, false, true);
    return std::make_unique<AggregatingStep>(
        input_header,
        params,
        GroupingSetsParamsList{},
        true,
        65536,
        0,
        1,
        1,
        false,
        false,
        SortDescription{},
        SortDescription{},
        false,
        false,
        false);
}

std::unique_ptr<ExpressionStep> makeRenamingExpressionStep(const SharedHeader & input_header, const String & prefix)
{
    ActionsDAG actions;
    for (const auto & column : *input_header)
    {
        const auto & input = actions.addInput(column.name, column.type);
        const auto & output = actions.addAlias(input, prefix + column.name);
        actions.addOrReplaceInOutputs(output);
    }
    return std::make_unique<ExpressionStep>(input_header, actions.clone());
}

std::unique_ptr<JoinStepLogical> makeLogicalJoinStep(
    const SharedHeader & left_header,
    const SharedHeader & right_header,
    JoinKind kind,
    JoinStrictness strictness,
    const Names & output_names)
{
    JoinExpressionActions expression_actions(*left_header, *right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    auto & outputs = actions_dag->getOutputs();
    outputs.clear();
    for (const auto & output_name : output_names)
    {
        const auto & inputs = actions_dag->getInputs();
        const auto input = std::ranges::find(inputs, output_name, &ActionsDAG::Node::result_name);
        if (input == inputs.end())
            throw std::runtime_error("makeLogicalJoinStep: output name '" + output_name + "' matches no input column");
        outputs.push_back(*input);
    }

    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator(kind, strictness),
        static_cast<JoinExpressionActions &&>(expression_actions),
        outputs,
        JoinSettings(getContext().context->getSettingsRef()),
        SortingStep::Settings(65536));
}
}

TEST(DataPropertyDerivation, DeclaredUniqueKeyMapsToOutputPositions)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    addColumn(header, "tenant", std::make_shared<DataTypeUInt32>());

    StorageInMemoryMetadata metadata;
    metadata.unique_key.column_names = {"tenant", "id"};

    auto properties = deriveDataPropertiesForStorageRead(header, &metadata);

    ASSERT_EQ(properties.uniqueKeys().size(), 1u);
    EXPECT_EQ(properties.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "id"}, {1, "tenant"}}));
    EXPECT_EQ(properties.uniqueKeys().front().provenance, DataPropertyProvenance::storageDeclaration());
    EXPECT_EQ(properties.uniqueKeys().front().equality_mode, DataPropertyEqualityMode::NonNullOrdinaryEquality);
}

TEST(DataPropertyDerivation, MissingOrAmbiguousUniqueKeyIsDropped)
{
    StorageInMemoryMetadata metadata;
    metadata.unique_key.column_names = {"id"};

    Block missing_header;
    addColumn(missing_header, "value", std::make_shared<DataTypeUInt64>());
    EXPECT_TRUE(deriveDataPropertiesForStorageRead(missing_header, &metadata).uniqueKeys().empty());

    Block ambiguous_header;
    addColumn(ambiguous_header, "id", std::make_shared<DataTypeUInt64>());
    addColumn(ambiguous_header, "id", std::make_shared<DataTypeUInt64>());
    EXPECT_TRUE(deriveDataPropertiesForStorageRead(ambiguous_header, &metadata).uniqueKeys().empty());
}

TEST(DataPropertyDerivation, PrimarySortingAndPartitionKeysAreNotUnique)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());

    StorageInMemoryMetadata metadata;
    metadata.primary_key.column_names = {"id"};
    metadata.sorting_key.column_names = {"id"};
    metadata.partition_key.column_names = {"id"};

    auto properties = deriveDataPropertiesForStorageRead(header, &metadata);
    EXPECT_TRUE(properties.uniqueKeys().empty());
}

TEST(DataPropertyDerivation, NonNullFactsHandleLowCardinalityNullable)
{
    Block header;
    addColumn(header, "plain", std::make_shared<DataTypeUInt64>());
    addColumn(header, "nullable", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()));
    addColumn(header, "low_cardinality", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));
    addColumn(
        header,
        "low_cardinality_nullable",
        std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())));
    addColumn(header, "dynamic", std::make_shared<DataTypeDynamic>());
    addColumn(
        header,
        "variant",
        std::make_shared<DataTypeVariant>(DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>()}));

    auto properties = deriveDataPropertiesForStorageRead(header, nullptr);

    EXPECT_EQ(properties.nonNullColumns(), (DataPropertyColumnSet{{0, "plain"}, {2, "low_cardinality"}}));
}

TEST(DataPropertyDerivation, AliasPreservesUniqueKey)
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block header;
    addColumn(header, "id", type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & input = dag.addInput("id", type);
    const auto & alias = dag.addAlias(input, "renamed_id");
    dag.addOrReplaceInOutputs(alias);
    ExpressionStep expression(shared_header, dag.clone());

    auto input_properties = propertiesWithUniqueKey({{0, "id"}}, header);
    const std::array children{input_properties};
    auto properties = deriveDataProperties(expression, children);

    ASSERT_EQ(properties.uniqueKeys().size(), 1u);
    EXPECT_EQ(properties.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "renamed_id"}}));
    EXPECT_EQ(properties.uniqueKeys().front().provenance.origin, DataPropertyOrigin::StorageDeclaration);
    EXPECT_EQ(properties.uniqueKeys().front().provenance.confidence, DataPropertyConfidence::DiagnosticOnly);
    EXPECT_EQ(
        properties.uniqueKeys().front().provenance,
        input_properties.uniqueKeys().front().provenance.transformed(DataPropertyTransformationKind::Identity));
    EXPECT_FALSE(isProvenStrongBagKey(properties.uniqueKeys().front()));
    ASSERT_EQ(properties.columnLineage().size(), 1u);
    EXPECT_EQ(properties.columnLineage().front().kind, ColumnLineageKind::Identity);
    EXPECT_EQ(properties.columnLineage().front().provenance, lineageProvenance(DataPropertyTransformationKind::Identity));
}

TEST(DataPropertyDerivation, DeterministicNonIdentityFunctionDoesNotPreserveUniqueKey)
{
    tryRegisterFunctions();

    auto type = std::make_shared<DataTypeDate>();
    Block header;
    addColumn(header, "id", type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & input = dag.addInput("id", type);
    addOutputFunction(dag, "toYear", {&input}, "year");
    ExpressionStep expression(shared_header, dag.clone());

    const std::array children{propertiesWithUniqueKey({{0, "id"}}, header)};
    auto properties = deriveDataProperties(expression, children);

    EXPECT_TRUE(properties.uniqueKeys().empty());
    ASSERT_EQ(properties.columnLineage().size(), 1u);
    EXPECT_EQ(properties.columnLineage().front().kind, ColumnLineageKind::NDVBound);
    EXPECT_EQ(properties.columnLineage().front().provenance, lineageProvenance(DataPropertyTransformationKind::NDVBoundExpression));
}

TEST(DataPropertyDerivation, ArrayJoinDropsAllFacts)
{
    auto id_type = std::make_shared<DataTypeUInt64>();
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    Block header;
    addColumn(header, "id", id_type);
    addColumn(header, "arr", array_type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & id_input = dag.addInput("id", id_type);
    const auto & arr_input = dag.addInput("arr", array_type);
    const auto & unnested = dag.addArrayJoin(arr_input, "value");
    dag.addOrReplaceInOutputs(id_input);
    dag.addOrReplaceInOutputs(unnested);
    ExpressionStep expression(shared_header, dag.clone());

    /// `id` is unique below the step, but `arrayJoin` duplicates each row `length(arr)` times:
    /// a preserved unique key would produce a false proven cardinality cap, so the step must
    /// derive no facts at all.
    const std::array children{propertiesWithUniqueKey({{0, "id"}}, header)};
    auto properties = deriveDataProperties(expression, children);

    EXPECT_TRUE(properties.uniqueKeys().empty());
    EXPECT_TRUE(properties.functionalDependencies().empty());
    EXPECT_TRUE(properties.nonNullColumns().empty());
    EXPECT_TRUE(properties.columnLineage().empty());
}

TEST(DataPropertyDerivation, MaterializeRecordsValuePreservingLineage)
{
    tryRegisterFunctions();

    auto type = std::make_shared<DataTypeUInt64>();
    Block header;
    addColumn(header, "id", type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & input = dag.addInput("id", type);
    addOutputFunction(dag, "materialize", {&input}, "materialized_id");
    ExpressionStep expression(shared_header, dag.clone());

    const std::array children{DataPropertySet{}};
    const auto properties = deriveDataProperties(expression, children);

    ASSERT_EQ(properties.columnLineage().size(), 1u);
    EXPECT_EQ(properties.columnLineage().front().kind, ColumnLineageKind::ValuePreserving);
    EXPECT_EQ(properties.columnLineage().front().provenance, lineageProvenance(DataPropertyTransformationKind::ValuePreservingExpression));
}

TEST(DataPropertyDerivation, ToNullablePreservesValuesAndDataProperties)
{
    tryRegisterFunctions();

    auto type = std::make_shared<DataTypeUInt64>();
    Block header;
    addColumn(header, "id", type);
    addColumn(header, "value", type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & id = dag.addInput("id", type);
    const auto & value = dag.addInput("value", type);
    addOutputFunction(dag, "toNullable", {&id}, "nullable_id");
    addOutputFunction(dag, "toNullable", {&value}, "nullable_value");
    ExpressionStep expression(shared_header, dag.clone());

    const std::array children{completeProperties(header)};
    const auto properties = deriveDataProperties(expression, children);

    ASSERT_EQ(properties.uniqueKeys().size(), 1u);
    EXPECT_EQ(properties.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "nullable_id"}}));
    ASSERT_EQ(properties.functionalDependencies().size(), 1u);
    EXPECT_EQ(properties.functionalDependencies().front().determinant, (DataPropertyColumnSet{{0, "nullable_id"}}));
    EXPECT_EQ(properties.functionalDependencies().front().dependents, (DataPropertyColumnSet{{1, "nullable_value"}}));
    EXPECT_EQ(properties.functionalDependencies().front().kind, DataPropertyDependencyKind::Exact);
    EXPECT_EQ(properties.nonNullColumns(), (DataPropertyColumnSet{{0, "nullable_id"}, {1, "nullable_value"}}));
    ASSERT_EQ(properties.columnLineage().size(), 2u);
    EXPECT_EQ(properties.columnLineage()[0].kind, ColumnLineageKind::ValuePreserving);
    EXPECT_EQ(properties.columnLineage()[1].kind, ColumnLineageKind::ValuePreserving);
}

TEST(DataPropertyDerivation, ToNullableDoesNotInventNonNullFact)
{
    tryRegisterFunctions();

    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    Block header;
    addColumn(header, "id", type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & input = dag.addInput("id", type);
    addOutputFunction(dag, "toNullable", {&input}, "nullable_id");
    ExpressionStep expression(shared_header, dag.clone());

    const std::array children{DataPropertySet{}};
    const auto properties = deriveDataProperties(expression, children);

    EXPECT_TRUE(properties.nonNullColumns().empty());
    ASSERT_EQ(properties.columnLineage().size(), 1u);
    EXPECT_EQ(properties.columnLineage().front().kind, ColumnLineageKind::ValuePreserving);
}

TEST(DataPropertyDerivation, ProjectingPartOfCompositeKeyDropsKey)
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block header;
    addColumn(header, "id", type);
    addColumn(header, "tenant", type);
    auto shared_header = std::make_shared<const Block>(header);

    ActionsDAG dag;
    const auto & id = dag.addInput("id", type);
    dag.addInput("tenant", type);
    dag.addOrReplaceInOutputs(id);
    ExpressionStep expression(shared_header, dag.clone());

    const std::array children{propertiesWithUniqueKey({{0, "id"}, {1, "tenant"}}, header)};
    auto properties = deriveDataProperties(expression, children);
    EXPECT_TRUE(properties.uniqueKeys().empty());
}

TEST(DataPropertyDerivation, AggregationGroupingIsUniqueOnlyInOrdinaryFinalMode)
{
    Block output_header;
    addColumn(output_header, "tenant", std::make_shared<DataTypeUInt64>());
    addColumn(output_header, "count", std::make_shared<DataTypeUInt64>());

    auto ordinary = deriveDataPropertiesForAggregation(output_header, {"tenant"}, {.final = true});
    ASSERT_EQ(ordinary.uniqueKeys().size(), 1u);
    EXPECT_EQ(ordinary.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "tenant"}}));
    EXPECT_EQ(ordinary.uniqueKeys().front().provenance, DataPropertyProvenance::aggregationGrouping());
    EXPECT_TRUE(isProvenStrongBagKey(ordinary.uniqueKeys().front()));

    EXPECT_TRUE(deriveDataPropertiesForAggregation(output_header, {"tenant"}, {.final = false}).uniqueKeys().empty());
    EXPECT_TRUE(
        deriveDataPropertiesForAggregation(output_header, {"tenant"}, {.final = true, .has_grouping_sets = true}).uniqueKeys().empty());
    EXPECT_TRUE(
        deriveDataPropertiesForAggregation(output_header, {"tenant"}, {.final = true, .has_overflow_row = true}).uniqueKeys().empty());
    EXPECT_TRUE(deriveDataPropertiesForAggregation(output_header, {}, {.final = true}).uniqueKeys().empty());
}

TEST(DataPropertyDerivation, JoinDropsKeysAndNullExtendedFacts)
{
    Block left_header;
    addColumn(left_header, "left_id", std::make_shared<DataTypeUInt64>());
    Block right_header;
    addColumn(right_header, "right_id", std::make_shared<DataTypeUInt64>());

    Block output_header = left_header;
    output_header.insert(right_header.getByPosition(0));

    auto left = propertiesWithUniqueKey({{0, "left_id"}}, left_header);
    auto right = propertiesWithUniqueKey({{0, "right_id"}}, right_header);

    auto inner
        = deriveDataPropertiesForJoin(JoinKind::Inner, JoinStrictness::All, output_header, {left_header, left}, {right_header, right});
    EXPECT_TRUE(inner.uniqueKeys().empty());
    EXPECT_EQ(inner.nonNullColumns(), (DataPropertyColumnSet{{0, "left_id"}, {1, "right_id"}}));

    auto left_outer
        = deriveDataPropertiesForJoin(JoinKind::Left, JoinStrictness::All, output_header, {left_header, left}, {right_header, right});
    EXPECT_EQ(left_outer.nonNullColumns(), (DataPropertyColumnSet{{0, "left_id"}}));

    auto right_outer
        = deriveDataPropertiesForJoin(JoinKind::Right, JoinStrictness::All, output_header, {left_header, left}, {right_header, right});
    EXPECT_EQ(right_outer.nonNullColumns(), (DataPropertyColumnSet{{1, "right_id"}}));

    auto full_outer
        = deriveDataPropertiesForJoin(JoinKind::Full, JoinStrictness::All, output_header, {left_header, left}, {right_header, right});
    EXPECT_TRUE(full_outer.nonNullColumns().empty());
}

TEST(DataPropertyDerivation, SemiAndAntiJoinsPreserveOnlySubsetSide)
{
    Block left_header;
    addColumn(left_header, "left_id", std::make_shared<DataTypeUInt64>());
    Block right_header;
    addColumn(right_header, "right_id", std::make_shared<DataTypeUInt64>());

    auto left = propertiesWithUniqueKey({{0, "left_id"}}, left_header);
    auto right = propertiesWithUniqueKey({{0, "right_id"}}, right_header);

    for (const auto strictness : {JoinStrictness::Semi, JoinStrictness::Anti})
    {
        auto properties = deriveDataPropertiesForJoin(JoinKind::Left, strictness, left_header, {left_header, left}, {right_header, right});
        ASSERT_EQ(properties.uniqueKeys().size(), 1u);
        EXPECT_EQ(properties.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "left_id"}}));
        EXPECT_EQ(properties.uniqueKeys().front().provenance.origin, DataPropertyOrigin::StorageDeclaration);
        EXPECT_EQ(properties.uniqueKeys().front().provenance.confidence, DataPropertyConfidence::DiagnosticOnly);
        EXPECT_EQ(
            properties.uniqueKeys().front().provenance,
            DataPropertyProvenance::storageDeclaration().transformed(DataPropertyTransformationKind::JoinPreservation));
        EXPECT_FALSE(isProvenStrongBagKey(properties.uniqueKeys().front()));
    }
}

TEST(DataPropertyDerivation, PlanTraversalDerivesRootProperties)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);

    QueryPlan::Node root;
    root.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    root.children = {&leaf};

    auto properties = deriveDataPropertiesForPlanDAG(root);
    EXPECT_EQ(properties.nonNullColumns(), (DataPropertyColumnSet{{0, "id"}}));
}

TEST(DataPropertyDerivation, ManualDumpFormatsRootProperties)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);

    QueryPlan::Node root;
    root.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    root.children = {&leaf};

    EXPECT_NE(deriveDataPropertiesForPlanDAG(root).dump().find("non_null=[0:id]"), String::npos);
}

TEST(DataPropertyDerivation, PlanTraversalFollowsCommonSubplanReference)
{
    Block input_header;
    addColumn(input_header, "id", std::make_shared<DataTypeUInt64>());
    addColumn(input_header, "tenant", std::make_shared<DataTypeUInt32>());
    auto shared_input_header = std::make_shared<const Block>(input_header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_input_header);

    QueryPlan::Node aggregation;
    aggregation.step = makeFinalAggregationStep(shared_input_header, {"id", "tenant"});
    aggregation.children = {&leaf};

    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(aggregation.step->getOutputHeader());
    common_subplan.children = {&aggregation};

    Block reference_header;
    reference_header.insert(input_header.getByName("tenant"));
    reference_header.insert(input_header.getByName("id"));
    auto shared_reference_header = std::make_shared<const Block>(reference_header);

    QueryPlan::Node reference;
    reference.step
        = std::make_unique<CommonSubplanReferenceStep>(shared_reference_header, &common_subplan, ColumnIdentifiers{"tenant", "id"});

    auto properties = deriveDataPropertiesForPlanDAG(reference);
    ASSERT_EQ(properties.uniqueKeys().size(), 1u);
    EXPECT_EQ(properties.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "tenant"}, {1, "id"}}));
    EXPECT_EQ(properties.uniqueKeys().front().provenance, DataPropertyProvenance::aggregationGrouping());
    EXPECT_EQ(properties.nonNullColumns(), (DataPropertyColumnSet{{0, "tenant"}, {1, "id"}}));
    EXPECT_TRUE(properties.columnLineage().empty());

    Block partial_header;
    partial_header.insert(input_header.getByName("id"));
    QueryPlan::Node partial_reference;
    partial_reference.step = std::make_unique<CommonSubplanReferenceStep>(
        std::make_shared<const Block>(partial_header), &common_subplan, ColumnIdentifiers{"id"});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(partial_reference).uniqueKeys().empty());
}

TEST(DataPropertyDerivation, PlanTraversalRejectsCommonSubplanReferenceCycle)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node reference;
    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(shared_header);
    common_subplan.children = {&reference};
    reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &common_subplan, ColumnIdentifiers{"id"});

    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(reference).empty());
}

TEST(DataPropertyDerivation, SafeRowSubsetStepsPreserveFacts)
{
    auto id_type = std::make_shared<DataTypeUInt64>();
    auto filter_type = std::make_shared<DataTypeUInt8>();
    Block header;
    addColumn(header, "id", id_type);
    addColumn(header, "keep", filter_type);
    auto shared_header = std::make_shared<const Block>(header);

    auto child = propertiesWithUniqueKey({{0, "id"}}, header);
    const std::array children{child};

    LimitStep limit(shared_header, 10, 0);
    EXPECT_EQ(deriveDataProperties(limit, children), child);

    SortDescription sort_description;
    sort_description.emplace_back("id");
    SortingStep sorting(shared_header, sort_description, 0, SortingStep::Settings(65536));
    EXPECT_EQ(deriveDataProperties(sorting, children), child);

    auto fill_description = sort_description;
    fill_description.front().with_fill = true;
    SortingStep sorting_with_fill(shared_header, fill_description, 0, SortingStep::Settings(65536));
    auto filled = deriveDataProperties(sorting_with_fill, children);
    EXPECT_TRUE(filled.uniqueKeys().empty());
    EXPECT_TRUE(filled.functionalDependencies().empty());
    EXPECT_TRUE(filled.nonNullColumns().empty());
    EXPECT_TRUE(filled.columnLineage().empty());

    ActionsDAG filter_dag;
    const auto & id = filter_dag.addInput("id", id_type);
    const auto & keep = filter_dag.addInput("keep", filter_type);
    filter_dag.addOrReplaceInOutputs(id);
    filter_dag.addOrReplaceInOutputs(keep);
    FilterStep filter(shared_header, filter_dag.clone(), "keep", true);
    auto filtered = deriveDataProperties(filter, children);
    ASSERT_EQ(filtered.uniqueKeys().size(), 1u);
    EXPECT_EQ(filtered.uniqueKeys().front().columns, (DataPropertyColumnSet{{0, "id"}}));
    EXPECT_EQ(
        filtered.uniqueKeys().front().provenance,
        DataPropertyProvenance::storageDeclaration().transformed(DataPropertyTransformationKind::FilterSubset));
    EXPECT_FALSE(isProvenStrongBagKey(filtered.uniqueKeys().front()));
}

TEST(DataPropertyDerivation, CompleteFactsPassThroughWithoutModifyingSafeCaller)
{
    Block header;
    addColumn(header, "id_with_a_long_name", std::make_shared<DataTypeUInt64>());
    addColumn(header, "value_with_a_long_name", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    const auto expected = completeProperties(header);
    const std::array children{expected};

    LimitStep limit(shared_header, 10, 0);
    EXPECT_EQ(deriveDataProperties(limit, children), expected);
    EXPECT_EQ(children.front(), expected);

    SortDescription sort_description;
    sort_description.emplace_back("id_with_a_long_name");
    SortingStep sorting(shared_header, sort_description, 0, SortingStep::Settings(65536));
    EXPECT_EQ(deriveDataProperties(sorting, children), expected);
    EXPECT_EQ(children.front(), expected);
}

TEST(DataPropertyDerivation, JoinDerivationAppendsPreservedFactsDirectly)
{
    Block left_header;
    addColumn(left_header, "left_id", std::make_shared<DataTypeUInt64>());
    addColumn(left_header, "left_value", std::make_shared<DataTypeUInt64>());
    Block right_header;
    addColumn(right_header, "right_id", std::make_shared<DataTypeUInt64>());
    addColumn(right_header, "right_value", std::make_shared<DataTypeUInt64>());

    Block output_header = left_header;
    output_header.insert(right_header.getByPosition(0));
    output_header.insert(right_header.getByPosition(1));

    const auto left = completeProperties(left_header);
    const auto right = completeProperties(right_header);
    auto append_expected_side = [](DataPropertySet & result,
                                   const DataPropertySet & source_properties,
                                   const Block & source_header,
                                   size_t output_offset,
                                   size_t child_index,
                                   bool preserve_keys_and_dependencies)
    {
        if (preserve_keys_and_dependencies)
        {
            result.addUniqueKey(source_properties.uniqueKeys().front().remap(
                {{output_offset, source_header.getByPosition(0).name}}, DataPropertyPreservingTransformationKind::JoinPreservation));
            result.addFunctionalDependency(source_properties.functionalDependencies().front().remap(
                {{output_offset, source_header.getByPosition(0).name}},
                {{output_offset + 1, source_header.getByPosition(1).name}},
                DataPropertyPreservingTransformationKind::JoinPreservation));
        }
        for (size_t position = 0; position < source_header.columns(); ++position)
        {
            const auto & name = source_header.getByPosition(position).name;
            result.addNonNullColumn({output_offset + position, name});
            result.addLineage(
                {{output_offset + position, name},
                 {child_index, position, name},
                 ColumnLineageKind::Identity,
                 lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
        }
    };

    DataPropertySet both_sides;
    append_expected_side(both_sides, left, left_header, 0, 0, false);
    append_expected_side(both_sides, right, right_header, 2, 1, false);
    for (const auto kind : {JoinKind::Inner, JoinKind::Cross, JoinKind::Comma})
    {
        EXPECT_EQ(
            deriveDataPropertiesForJoin(kind, JoinStrictness::All, output_header, {left_header, left}, {right_header, right}), both_sides);
    }
    EXPECT_EQ(
        deriveDataPropertiesForJoin(JoinKind::Inner, JoinStrictness::Semi, output_header, {left_header, left}, {right_header, right}),
        both_sides);

    DataPropertySet left_only;
    append_expected_side(left_only, left, left_header, 0, 0, false);
    EXPECT_EQ(
        deriveDataPropertiesForJoin(JoinKind::Left, JoinStrictness::All, output_header, {left_header, left}, {right_header, right}),
        left_only);

    DataPropertySet right_only;
    append_expected_side(right_only, right, right_header, 2, 1, false);
    EXPECT_EQ(
        deriveDataPropertiesForJoin(JoinKind::Right, JoinStrictness::All, output_header, {left_header, left}, {right_header, right}),
        right_only);

    for (const auto kind : {JoinKind::Full, JoinKind::Paste})
    {
        EXPECT_TRUE(
            deriveDataPropertiesForJoin(kind, JoinStrictness::All, output_header, {left_header, left}, {right_header, right}).empty());
    }

    DataPropertySet left_subset;
    append_expected_side(left_subset, left, left_header, 0, 0, true);
    DataPropertySet right_subset;
    append_expected_side(right_subset, right, right_header, 0, 1, true);
    for (const auto strictness : {JoinStrictness::Semi, JoinStrictness::Anti})
    {
        EXPECT_EQ(
            deriveDataPropertiesForJoin(JoinKind::Left, strictness, left_header, {left_header, left}, {right_header, right}), left_subset);
        EXPECT_EQ(
            deriveDataPropertiesForJoin(JoinKind::Right, strictness, right_header, {left_header, left}, {right_header, right}),
            right_subset);
    }
}

TEST(DataPropertyDerivation, JoinPreservedNonNullFactsRespectNullableOutputTypes)
{
    Block left_header;
    addColumn(left_header, "nullable_left", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()));
    addColumn(
        left_header,
        "low_cardinality_nullable_left",
        std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())));
    Block right_header;
    addColumn(right_header, "right_id", std::make_shared<DataTypeUInt64>());
    Block output_header = left_header;
    output_header.insert(right_header.getByPosition(0));

    DataPropertySet left;
    left.addNonNullColumn({0, "nullable_left"});
    left.addNonNullColumn({1, "low_cardinality_nullable_left"});
    DataPropertySet right;
    right.addNonNullColumn({0, "right_id"});

    DataPropertySet expected;
    expected.addNonNullColumn({2, "right_id"});
    expected.addLineage(
        {{0, "nullable_left"},
         {0, 0, "nullable_left"},
         ColumnLineageKind::Identity,
         lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
    expected.addLineage(
        {{1, "low_cardinality_nullable_left"},
         {0, 1, "low_cardinality_nullable_left"},
         ColumnLineageKind::Identity,
         lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
    expected.addLineage(
        {{2, "right_id"},
         {1, 0, "right_id"},
         ColumnLineageKind::Identity,
         lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});

    EXPECT_EQ(
        deriveDataPropertiesForJoin(JoinKind::Inner, JoinStrictness::All, output_header, {left_header, left}, {right_header, right}),
        expected);
}

TEST(DataPropertyDerivation, MultipleCommonReferencesShareOneProducer)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    addColumn(header, "tenant", std::make_shared<DataTypeUInt32>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);
    QueryPlan::Node producer;
    producer.step = makeFinalAggregationStep(shared_header, {"id", "tenant"});
    producer.children = {&leaf};
    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(producer.step->getOutputHeader());
    common_subplan.children = {&producer};

    QueryPlan::Node first_reference;
    first_reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &common_subplan, ColumnIdentifiers{"id", "tenant"});

    Block reordered_header;
    reordered_header.insert(header.getByName("tenant"));
    reordered_header.insert(header.getByName("id"));
    QueryPlan::Node second_reference;
    second_reference.step = std::make_unique<CommonSubplanReferenceStep>(
        std::make_shared<const Block>(reordered_header), &common_subplan, ColumnIdentifiers{"tenant", "id"});

    QueryPlan::Node renamed_first_reference;
    renamed_first_reference.step = makeRenamingExpressionStep(shared_header, "first_");
    renamed_first_reference.children = {&first_reference};

    const auto reordered_shared_header = second_reference.step->getOutputHeader();
    QueryPlan::Node root;
    root.step = makeLogicalJoinStep(
        renamed_first_reference.step->getOutputHeader(), reordered_shared_header, JoinKind::Right, JoinStrictness::Semi, {"tenant", "id"});
    root.children = {&renamed_first_reference, &second_reference};

    DataPropertySet expected;
    expected.addUniqueKey(
        UniqueKeyFact::fromAggregationGrouping({{0, "tenant"}, {1, "id"}})
            .remap({{0, "tenant"}, {1, "id"}}, DataPropertyPreservingTransformationKind::JoinPreservation));
    expected.addNonNullColumn({0, "tenant"});
    expected.addNonNullColumn({1, "id"});
    expected.addLineage(
        {{0, "tenant"},
         {1, 0, "tenant"},
         ColumnLineageKind::Identity,
         lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
    expected.addLineage(
        {{1, "id"}, {1, 1, "id"}, ColumnLineageKind::Identity, lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
    EXPECT_EQ(deriveDataPropertiesForPlanDAG(root), expected);
}

TEST(DataPropertyDerivation, ProducerSupportsMixedOrdinaryAndReferenceConsumers)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);
    QueryPlan::Node producer;
    producer.step = makeFinalAggregationStep(shared_header, {"id"});
    producer.children = {&leaf};
    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(producer.step->getOutputHeader());
    common_subplan.children = {&producer};
    QueryPlan::Node reference;
    reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &common_subplan, ColumnIdentifiers{"id"});

    QueryPlan::Node renamed_reference;
    renamed_reference.step = makeRenamingExpressionStep(shared_header, "reference_");
    renamed_reference.children = {&reference};

    QueryPlan::Node root;
    root.step
        = makeLogicalJoinStep(renamed_reference.step->getOutputHeader(), shared_header, JoinKind::Right, JoinStrictness::Semi, {"id"});
    root.children = {&renamed_reference, &producer};

    DataPropertySet expected;
    expected.addUniqueKey(
        UniqueKeyFact::fromAggregationGrouping({{0, "id"}}).remap({{0, "id"}}, DataPropertyPreservingTransformationKind::JoinPreservation));
    expected.addNonNullColumn({0, "id"});
    expected.addLineage(
        {{0, "id"}, {1, 0, "id"}, ColumnLineageKind::Identity, lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
    EXPECT_EQ(deriveDataPropertiesForPlanDAG(root), expected);
}

TEST(DataPropertyDerivation, SharedReferenceAndOrdinaryNodesAreEvaluatedOnce)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);
    QueryPlan::Node producer;
    producer.step = makeFinalAggregationStep(shared_header, {"id"});
    producer.children = {&leaf};
    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(shared_header);
    common_subplan.children = {&producer};
    QueryPlan::Node reference;
    reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &common_subplan, ColumnIdentifiers{"id"});

    QueryPlan::Node first_reference_parent;
    first_reference_parent.step = makeRenamingExpressionStep(shared_header, "first_");
    first_reference_parent.children = {&reference};
    QueryPlan::Node second_reference_parent;
    second_reference_parent.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    second_reference_parent.children = {&reference};
    QueryPlan::Node root_with_shared_reference;
    root_with_shared_reference.step
        = makeLogicalJoinStep(first_reference_parent.step->getOutputHeader(), shared_header, JoinKind::Right, JoinStrictness::Semi, {"id"});
    root_with_shared_reference.children = {&first_reference_parent, &second_reference_parent};

    DataPropertySet expected;
    expected.addUniqueKey(
        UniqueKeyFact::fromAggregationGrouping({{0, "id"}}).remap({{0, "id"}}, DataPropertyPreservingTransformationKind::JoinPreservation));
    expected.addNonNullColumn({0, "id"});
    expected.addLineage(
        {{0, "id"}, {1, 0, "id"}, ColumnLineageKind::Identity, lineageProvenance(DataPropertyTransformationKind::JoinPreservation)});
    EXPECT_EQ(deriveDataPropertiesForPlanDAG(root_with_shared_reference), expected);

    QueryPlan::Node first_ordinary_parent;
    first_ordinary_parent.step = makeRenamingExpressionStep(shared_header, "first_");
    first_ordinary_parent.children = {&producer};
    QueryPlan::Node second_ordinary_parent;
    second_ordinary_parent.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    second_ordinary_parent.children = {&producer};
    QueryPlan::Node root_with_shared_ordinary_child;
    root_with_shared_ordinary_child.step
        = makeLogicalJoinStep(first_ordinary_parent.step->getOutputHeader(), shared_header, JoinKind::Right, JoinStrictness::Semi, {"id"});
    root_with_shared_ordinary_child.children = {&first_ordinary_parent, &second_ordinary_parent};
    EXPECT_EQ(deriveDataPropertiesForPlanDAG(root_with_shared_ordinary_child), expected);

    Block unused_left_header;
    addColumn(unused_left_header, "unused_left_id", std::make_shared<DataTypeUInt64>());
    QueryPlan::Node root_with_repeated_edge;
    root_with_repeated_edge.step = makeLogicalJoinStep(
        std::make_shared<const Block>(unused_left_header), shared_header, JoinKind::Right, JoinStrictness::Semi, {"id"});
    root_with_repeated_edge.children = {&producer, &producer};
    EXPECT_EQ(deriveDataPropertiesForPlanDAG(root_with_repeated_edge), expected);

    QueryPlan::Node unsupported_arity;
    unsupported_arity.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    unsupported_arity.children = {&producer, &producer, &producer};
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(unsupported_arity).empty());
}

TEST(DataPropertyDerivation, NestedCommonReferencesPreserveMappedFacts)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);
    QueryPlan::Node aggregation;
    aggregation.step = makeFinalAggregationStep(shared_header, {"id"});
    aggregation.children = {&leaf};
    QueryPlan::Node inner_common_subplan;
    inner_common_subplan.step = std::make_unique<CommonSubplanStep>(aggregation.step->getOutputHeader());
    inner_common_subplan.children = {&aggregation};
    QueryPlan::Node inner_reference;
    inner_reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &inner_common_subplan, ColumnIdentifiers{"id"});
    QueryPlan::Node outer_common_subplan;
    outer_common_subplan.step = std::make_unique<CommonSubplanStep>(shared_header);
    outer_common_subplan.children = {&inner_reference};
    QueryPlan::Node outer_reference;
    outer_reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &outer_common_subplan, ColumnIdentifiers{"id"});

    const auto properties = deriveDataPropertiesForPlanDAG(outer_reference);
    EXPECT_EQ(properties.uniqueKeys(), (UniqueKeyFacts{UniqueKeyFact::fromAggregationGrouping({{0, "id"}})}));
    EXPECT_EQ(properties.nonNullColumns(), (DataPropertyColumnSet{{0, "id"}}));
}

TEST(DataPropertyDerivation, StructurallyUnresolvedCommonReferencesAreEmpty)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    auto expect_empty_reference = [&](QueryPlan::Node * referenced_root)
    {
        QueryPlan::Node reference;
        reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, referenced_root, ColumnIdentifiers{"id"});
        EXPECT_TRUE(deriveDataPropertiesForPlanDAG(reference).empty());
    };

    expect_empty_reference(nullptr);

    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);
    expect_empty_reference(&leaf);

    QueryPlan::Node empty_wrapper;
    empty_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    expect_empty_reference(&empty_wrapper);

    QueryPlan::Node multiple_wrapper;
    multiple_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    multiple_wrapper.children = {&leaf, &leaf};
    expect_empty_reference(&multiple_wrapper);

    QueryPlan::Node null_producer_wrapper;
    null_producer_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    null_producer_wrapper.children = {nullptr};
    expect_empty_reference(&null_producer_wrapper);
}

TEST(DataPropertyDerivation, MappingInvalidCommonReferencesAreEmpty)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);
    QueryPlan::Node leaf;
    leaf.step = std::make_unique<TestSourceStep>(shared_header);
    QueryPlan::Node common_subplan;
    common_subplan.step = std::make_unique<CommonSubplanStep>(shared_header);
    common_subplan.children = {&leaf};

    QueryPlan::Node count_mismatch;
    count_mismatch.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &common_subplan, ColumnIdentifiers{});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(count_mismatch).empty());

    QueryPlan::Node name_mismatch;
    name_mismatch.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &common_subplan, ColumnIdentifiers{"missing"});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(name_mismatch).empty());

    Block wrong_type_header;
    addColumn(wrong_type_header, "id", std::make_shared<DataTypeUInt32>());
    QueryPlan::Node type_mismatch;
    type_mismatch.step = std::make_unique<CommonSubplanReferenceStep>(
        std::make_shared<const Block>(wrong_type_header), &common_subplan, ColumnIdentifiers{"id"});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(type_mismatch).empty());
}

TEST(DataPropertyDerivation, OrdinaryCommonAndMixedCyclesFailClosed)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    QueryPlan::Node ordinary_cycle;
    ordinary_cycle.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    ordinary_cycle.children = {&ordinary_cycle};
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(ordinary_cycle).empty());

    QueryPlan::Node first_reference;
    QueryPlan::Node second_reference;
    QueryPlan::Node first_wrapper;
    first_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    first_wrapper.children = {&second_reference};
    QueryPlan::Node second_wrapper;
    second_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    second_wrapper.children = {&first_reference};
    first_reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &first_wrapper, ColumnIdentifiers{"id"});
    second_reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &second_wrapper, ColumnIdentifiers{"id"});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(first_reference).empty());

    QueryPlan::Node mixed_root;
    QueryPlan::Node mixed_reference;
    QueryPlan::Node mixed_wrapper;
    mixed_root.step = std::make_unique<LimitStep>(shared_header, 10, 0);
    mixed_root.children = {&mixed_reference};
    mixed_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    mixed_wrapper.children = {&mixed_root};
    mixed_reference.step = std::make_unique<CommonSubplanReferenceStep>(shared_header, &mixed_wrapper, ColumnIdentifiers{"id"});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(mixed_root).empty());

    QueryPlan::Node invalid_mapping_reference;
    QueryPlan::Node invalid_mapping_wrapper;
    invalid_mapping_wrapper.step = std::make_unique<CommonSubplanStep>(shared_header);
    invalid_mapping_wrapper.children = {&invalid_mapping_reference};
    invalid_mapping_reference.step
        = std::make_unique<CommonSubplanReferenceStep>(shared_header, &invalid_mapping_wrapper, ColumnIdentifiers{"missing"});
    EXPECT_TRUE(deriveDataPropertiesForPlanDAG(invalid_mapping_reference).empty());
}

TEST(DataPropertyDerivation, DeepPlanTraversalRemainsIterative)
{
    Block header;
    addColumn(header, "id", std::make_shared<DataTypeUInt64>());
    auto shared_header = std::make_shared<const Block>(header);

    constexpr size_t depth = 4096;
    std::vector<std::unique_ptr<QueryPlan::Node>> nodes;
    nodes.reserve(depth + 1);
    nodes.push_back(std::make_unique<QueryPlan::Node>());
    nodes.back()->step = std::make_unique<TestSourceStep>(shared_header);
    for (size_t index = 0; index < depth; ++index)
    {
        auto * child = nodes.back().get();
        nodes.push_back(std::make_unique<QueryPlan::Node>());
        nodes.back()->step = std::make_unique<LimitStep>(shared_header, 10, 0);
        nodes.back()->children = {child};
    }

    EXPECT_EQ(deriveDataPropertiesForPlanDAG(*nodes.back()).nonNullColumns(), (DataPropertyColumnSet{{0, "id"}}));
}
