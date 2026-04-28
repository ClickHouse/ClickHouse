#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexANN.h>
#include <Storages/IndicesDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ActionsDAG.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/Exception.h>

using namespace DB;

namespace
{

/// Build an IndexDescription with a given `type` on a given source column name. Sufficient for
/// the validator's coexistence check, which only inspects `index.type` and
/// `index.expression->getRequiredColumns()`.
IndexDescription makeIndexOnColumn(const String & type, const String & column_name)
{
    IndexDescription desc;
    desc.type = type;
    desc.column_names = {column_name};

    NamesAndTypesList inputs;
    inputs.emplace_back(column_name, std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    ActionsDAG dag(inputs);
    desc.expression = std::make_shared<ExpressionActions>(std::move(dag));
    return desc;
}

}

/// DEV-26: CREATE TABLE where the same column carries both `ann` and `vector_similarity` is
/// rejected. The validator walks `metadata.secondary_indices`, collects the underlying source
/// columns for each kind, and throws if the two sets intersect.
TEST(ANNSearchPassTest, DEV26RejectsSameColumnBothIndexes)
{
    StorageInMemoryMetadata metadata;
    metadata.secondary_indices.push_back(makeIndexOnColumn("ann", "emb"));
    metadata.secondary_indices.push_back(makeIndexOnColumn("vector_similarity", "emb"));

    EXPECT_THROW(validateNoCoexistingANNAndVectorSimilarity(metadata), Exception);
}

/// DEV-26: two vector indexes on different columns are accepted.
TEST(ANNSearchPassTest, DEV26AcceptsDifferentColumns)
{
    StorageInMemoryMetadata metadata;
    metadata.secondary_indices.push_back(makeIndexOnColumn("ann", "emb_a"));
    metadata.secondary_indices.push_back(makeIndexOnColumn("vector_similarity", "emb_b"));

    EXPECT_NO_THROW(validateNoCoexistingANNAndVectorSimilarity(metadata));
}

/// DEV-26: empty metadata (no vector indexes) is accepted.
TEST(ANNSearchPassTest, DEV26AcceptsEmpty)
{
    StorageInMemoryMetadata metadata;
    EXPECT_NO_THROW(validateNoCoexistingANNAndVectorSimilarity(metadata));
}

/// DEV-26: an `ann` index alone is accepted.
TEST(ANNSearchPassTest, DEV26AcceptsANNOnly)
{
    StorageInMemoryMetadata metadata;
    metadata.secondary_indices.push_back(makeIndexOnColumn("ann", "emb"));
    EXPECT_NO_THROW(validateNoCoexistingANNAndVectorSimilarity(metadata));
}

/// DEV-26: a `vector_similarity` index alone is accepted.
TEST(ANNSearchPassTest, DEV26AcceptsVSOnly)
{
    StorageInMemoryMetadata metadata;
    metadata.secondary_indices.push_back(makeIndexOnColumn("vector_similarity", "emb"));
    EXPECT_NO_THROW(validateNoCoexistingANNAndVectorSimilarity(metadata));
}

/// `getANNIndexColumnName` returns the source column of the single `ann` index, and the empty
/// string when no such index is present.
TEST(ANNSearchPassTest, ExtractANNColumnName)
{
    StorageInMemoryMetadata metadata;
    EXPECT_EQ(getANNIndexColumnName(metadata), "");

    metadata.secondary_indices.push_back(makeIndexOnColumn("vector_similarity", "vs_col"));
    EXPECT_EQ(getANNIndexColumnName(metadata), "");

    metadata.secondary_indices.push_back(makeIndexOnColumn("ann", "emb"));
    EXPECT_EQ(getANNIndexColumnName(metadata), "emb");
}

#endif
