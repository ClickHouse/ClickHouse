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
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
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

}
