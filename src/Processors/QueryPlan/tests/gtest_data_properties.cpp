#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/DataProperties.h>

#include <unordered_set>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(DataProperties, EmptySetContainsNoAffirmativeFacts)
{
    DataPropertySet properties;

    EXPECT_TRUE(properties.empty());
    EXPECT_TRUE(properties.dump().starts_with("unique_keys=[]"));
}

TEST(DataProperties, ColumnSetsAreNormalizedByPosition)
{
    ColumnSet columns{{2, "c"}, {0, "a"}, {2, "c"}, {1, "b"}};

    ASSERT_TRUE(normalizeColumnSet(columns));
    EXPECT_EQ(columns, (ColumnSet{{0, "a"}, {1, "b"}, {2, "c"}}));
}

TEST(DataProperties, ConflictingNamesForOnePositionAreRejected)
{
    ColumnSet columns{{0, "left"}, {0, "left"}, {1, "other"}, {0, "right"}, {0, "right"}};

    EXPECT_FALSE(normalizeColumnSet(columns));

    DataPropertySet properties;
    EXPECT_FALSE(properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration(columns)));
    EXPECT_TRUE(properties.uniqueKeys().empty());

    EXPECT_TRUE(properties.addNonNullColumn({0, "left"}));
    EXPECT_FALSE(properties.addNonNullColumn({0, "right"}));
    EXPECT_EQ(properties.nonNullColumns(), (ColumnSet{{0, "left"}}));
}

TEST(DataProperties, ColumnReferenceEqualityAndHashAgree)
{
    const PlanColumnRef first{3, "id"};
    const PlanColumnRef same{3, "id"};
    const PlanColumnRef different_name{3, "other"};

    EXPECT_EQ(first, same);
    EXPECT_EQ(first, different_name);
    EXPECT_EQ(PlanColumnRefHash{}(first), PlanColumnRefHash{}(same));
    EXPECT_EQ(PlanColumnRefHash{}(first), PlanColumnRefHash{}(different_name));

    std::unordered_set<PlanColumnRef, PlanColumnRefHash> columns{first, same, different_name};
    EXPECT_EQ(columns.size(), 1u);
}

TEST(DataProperties, FactsAreNormalizedAndCoalesced)
{
    DataPropertySet properties;
    const UniqueKeyFact key = UniqueKeyFact::fromStorageDeclaration({{1, "b"}, {0, "a"}});
    const FunctionalDependencyFact dependency{{{1, "b"}, {0, "a"}}, {{2, "c"}, {2, "c"}}, DataPropertyDependencyKind::Exact, {}};
    const ColumnLineageFact lineage{
        {2, "c"}, {0, 0, "a"}, ColumnLineageKind::Identity, DataPropertyProvenance{}.transformed(DataPropertyTransformationKind::Identity)};

    EXPECT_TRUE(properties.addUniqueKey(key));
    EXPECT_FALSE(properties.addUniqueKey(key));
    EXPECT_TRUE(properties.addFunctionalDependency(dependency));
    EXPECT_FALSE(properties.addFunctionalDependency(dependency));
    EXPECT_TRUE(properties.addNonNullColumn({0, "a"}));
    EXPECT_FALSE(properties.addNonNullColumn({0, "a"}));
    EXPECT_TRUE(properties.addLineage(lineage));
    EXPECT_FALSE(properties.addLineage(lineage));

    ASSERT_EQ(properties.uniqueKeys().size(), 1u);
    EXPECT_EQ(properties.uniqueKeys().front().columns, (ColumnSet{{0, "a"}, {1, "b"}}));
    ASSERT_EQ(properties.functionalDependencies().size(), 1u);
    EXPECT_EQ(properties.functionalDependencies().front().dependents, (ColumnSet{{2, "c"}}));
    EXPECT_EQ(properties.nonNullColumns().size(), 1u);
    EXPECT_EQ(properties.columnLineage().size(), 1u);
}

TEST(DataProperties, FactOrderDoesNotAffectEquality)
{
    const FunctionalDependencyFact first_dependency{{{1, "b"}}, {{2, "c"}}, DataPropertyDependencyKind::Exact, {}};
    const FunctionalDependencyFact second_dependency{
        {{0, "a"}},
        {{1, "b"}},
        DataPropertyDependencyKind::Exact,
        DataPropertyProvenance{}.transformed(DataPropertyTransformationKind::FilterSubset)};
    const ColumnLineageFact first_lineage{
        {1, "b"}, {0, 1, "b"}, ColumnLineageKind::Identity, DataPropertyProvenance{}.transformed(DataPropertyTransformationKind::Identity)};
    const ColumnLineageFact second_lineage{
        {0, "a"},
        {1, 0, "a"},
        ColumnLineageKind::ValuePreserving,
        DataPropertyProvenance{}.transformed(DataPropertyTransformationKind::ValuePreservingExpression)};

    DataPropertySet first;
    first.addUniqueKey(UniqueKeyFact::fromStorageDeclaration({{1, "b"}}));
    first.addUniqueKey(UniqueKeyFact::fromAggregationGrouping({{0, "a"}}));
    first.addFunctionalDependency(first_dependency);
    first.addFunctionalDependency(second_dependency);
    first.addNonNullColumn({1, "b"});
    first.addNonNullColumn({0, "a"});
    first.addLineage(first_lineage);
    first.addLineage(second_lineage);

    DataPropertySet second;
    second.addLineage(second_lineage);
    second.addLineage(first_lineage);
    second.addNonNullColumn({0, "a"});
    second.addNonNullColumn({1, "b"});
    second.addFunctionalDependency(second_dependency);
    second.addFunctionalDependency(first_dependency);
    second.addUniqueKey(UniqueKeyFact::fromAggregationGrouping({{0, "a"}}));
    second.addUniqueKey(UniqueKeyFact::fromStorageDeclaration({{1, "b"}}));

    EXPECT_EQ(first, second);
}

TEST(DataProperties, ProvenanceSeparatesOriginConfidenceAndTransformationHistory)
{
    const auto storage = UniqueKeyFact::fromStorageDeclaration({{0, "id"}});
    EXPECT_EQ(storage.provenance.origin, DataPropertyOrigin::StorageDeclaration);
    EXPECT_EQ(storage.provenance.confidence, DataPropertyConfidence::DiagnosticOnly);
    EXPECT_FALSE(isProvenStrongBagKey(storage));

    const auto transformed = storage.remap(storage.columns, DataPropertyPreservingTransformationKind::FilterSubset);
    EXPECT_EQ(transformed.provenance.origin, DataPropertyOrigin::StorageDeclaration);
    EXPECT_EQ(transformed.provenance.confidence, DataPropertyConfidence::DiagnosticOnly);
    EXPECT_NE(transformed.provenance.history.value, 0u);
    EXPECT_EQ(
        dataPropertyProvenanceToString(transformed.provenance),
        "origin=storage-declaration, confidence=diagnostic-only, transformations=[filter-subset]");
    EXPECT_FALSE(isProvenStrongBagKey(transformed));

    const UniqueKeyFact header{.columns = {{0, "id"}}, .provenance = {}, .equality_mode = DataPropertyEqualityMode::Unsupported};
    EXPECT_FALSE(isProvenStrongBagKey(header));

    const auto aggregation = UniqueKeyFact::fromAggregationGrouping({{0, "id"}});
    EXPECT_TRUE(isProvenStrongBagKey(aggregation));
}

TEST(DataProperties, TransformationHistoryOverflowFailsClosed)
{
    auto key = UniqueKeyFact::fromAggregationGrouping({{0, "id"}});
    for (size_t index = 0; index < 16; ++index)
        key = key.remap(key.columns, DataPropertyPreservingTransformationKind::Identity);

    EXPECT_EQ(key.provenance.confidence, DataPropertyConfidence::Unknown);
    EXPECT_FALSE(isProvenStrongBagKey(key));
}

TEST(DataProperties, NameResolutionRejectsMissingAndAmbiguousOutputs)
{
    const std::vector<String> unique_output_names{"id", "value"};
    const std::vector<String> ambiguous_output_names{"id", "id", "value"};
    const std::vector<String> key_names{"id"};
    const std::vector<String> missing_names{"missing"};

    auto resolved = resolveColumnSetByName(unique_output_names, key_names);
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(*resolved, (ColumnSet{{0, "id"}}));
    EXPECT_FALSE(resolveColumnSetByName(ambiguous_output_names, key_names).has_value());
    EXPECT_FALSE(resolveColumnSetByName(unique_output_names, missing_names).has_value());
}

TEST(DataProperties, SortingPropertyPreservesSequenceDirectionNullsAndScope)
{
    SortDescription description;
    description.emplace_back("tenant", -1, 1);
    description.emplace_back("id", 1, -1);

    DataPropertySet properties;
    properties.setSorting({description, SortingScope::Global});
    EXPECT_FALSE(properties.empty());
    EXPECT_EQ(properties.sorting(), (SortingProperty{description, SortingScope::Global}));
    EXPECT_EQ(sortingPropertyToString(properties.sorting()), "global:[tenant DESC NULLS FIRST, id ASC NULLS FIRST]");

    std::swap(description[0], description[1]);
    EXPECT_NE(properties.sorting(), (SortingProperty{description, SortingScope::Global}));
    EXPECT_NE(properties.sorting(), (SortingProperty{properties.sorting().sort_description, SortingScope::Stream}));
}

TEST(DataProperties, DumpIsStableAndIncludesEvidence)
{
    DataPropertySet properties;
    properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration({{1, "tenant"}, {0, "id"}}));
    properties.addNonNullColumn({0, "id"});

    EXPECT_EQ(
        properties.dump(),
        "unique_keys=[[0:id, 1:tenant] (origin=storage-declaration, confidence=diagnostic-only, transformations=[]; "
        "equality=non-null-ordinary)], fds=[], non_null=[0:id], lineage=[], sorting=[]");
}
