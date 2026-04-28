#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/AlterCommands.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexANN.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Exception.h>

using namespace DB;

namespace
{

/// Build an `ASTFunction(name="equals", [ident(key), literal(value)])` used to feed named
/// arguments into the argument parser without running the full SQL parser.
ASTPtr makeKeyValueArg(const String & key, Field value)
{
    auto equals = make_intrusive<ASTFunction>();
    equals->name = "equals";
    equals->arguments = make_intrusive<ASTExpressionList>();
    equals->children.push_back(equals->arguments);

    auto ident = make_intrusive<ASTIdentifier>(key);
    auto literal = make_intrusive<ASTLiteral>(std::move(value));
    equals->arguments->children.push_back(ident);
    equals->arguments->children.push_back(literal);

    return equals;
}

ASTPtr makeArgumentsList(std::vector<std::pair<String, Field>> kvs)
{
    auto list = make_intrusive<ASTExpressionList>();
    for (auto & [k, v] : kvs)
        list->children.push_back(makeKeyValueArg(k, std::move(v)));
    return list;
}

/// Minimal `IndexDescription` sufficient for the validator / creator. We skip
/// `initExpressionInfo` because the validator only reads `column_names` / `data_types` /
/// `arguments`; `sample_block` stays empty, and the validator falls back to `data_types`.
IndexDescription makeIndexDescription(
    std::vector<std::pair<String, Field>> args,
    DataTypePtr column_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()),
    Names columns = {"emb"})
{
    IndexDescription index;
    index.name = "idx";
    index.type = "ann";
    index.arguments = makeArgumentsList(std::move(args));
    index.column_names = std::move(columns);
    index.data_types.push_back(std::move(column_type));
    index.granularity = 1;
    return index;
}

}

TEST(ANNIndexDDLTest, ValidatorRejectsMissingDim)
{
    auto index = makeIndexDescription({{"metric", Field("L2")}});
    EXPECT_THROW(annIndexValidator(index, /*attach*/ false), Exception);
}

TEST(ANNIndexDDLTest, ValidatorRejectsUnknownAlgorithm)
{
    auto index = makeIndexDescription({{"algorithm", Field("hnsw")}, {"dim", Field(UInt64{128})}});
    EXPECT_THROW(annIndexValidator(index, false), Exception);
}

TEST(ANNIndexDDLTest, ValidatorRejectsUnknownMetric)
{
    auto index = makeIndexDescription({{"metric", Field("Hamming")}, {"dim", Field(UInt64{128})}});
    EXPECT_THROW(annIndexValidator(index, false), Exception);
}

TEST(ANNIndexDDLTest, ValidatorRejectsDimOutOfRange)
{
    {
        auto index = makeIndexDescription({{"dim", Field(UInt64{0})}});
        EXPECT_THROW(annIndexValidator(index, false), Exception);
    }
    {
        auto index = makeIndexDescription({{"dim", Field(UInt64{100000})}});
        EXPECT_THROW(annIndexValidator(index, false), Exception);
    }
}

TEST(ANNIndexDDLTest, ValidatorRejectsUnknownKey)
{
    auto index = makeIndexDescription({{"dim", Field(UInt64{128})}, {"bogus", Field(UInt64{42})}});
    EXPECT_THROW(annIndexValidator(index, false), Exception);
}

TEST(ANNIndexDDLTest, ValidatorRejectsMultiColumn)
{
    auto index = makeIndexDescription(
        {{"dim", Field(UInt64{128})}},
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()),
        {"a", "b"});
    /// Add a matching entry to `data_types` as well so the validator inspects the column count.
    index.data_types.push_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    EXPECT_THROW(annIndexValidator(index, false), Exception);
}

TEST(ANNIndexDDLTest, ValidatorRejectsNonArrayFloat32Column)
{
    /// Array(Float64) is rejected.
    {
        auto index = makeIndexDescription(
            {{"dim", Field(UInt64{128})}},
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()));
        EXPECT_THROW(annIndexValidator(index, false), Exception);
    }
    /// String is rejected.
    {
        auto index = makeIndexDescription(
            {{"dim", Field(UInt64{128})}},
            std::make_shared<DataTypeString>());
        EXPECT_THROW(annIndexValidator(index, false), Exception);
    }
}

TEST(ANNIndexDDLTest, ValidatorAcceptsMinimalDefinition)
{
    auto index = makeIndexDescription({{"dim", Field(UInt64{128})}});
    EXPECT_NO_THROW(annIndexValidator(index, false));
}

TEST(ANNIndexDDLTest, ValidatorAcceptsFullDefinition)
{
    auto index = makeIndexDescription({
        {"algorithm", Field("diskann")},
        {"metric", Field("Cosine")},
        {"dim", Field(UInt64{256})},
        {"max_degree", Field(UInt64{64})},
        {"alpha", Field(Float64{1.2})},
        {"hash_seed", Field(UInt64{42})},
    });
    EXPECT_NO_THROW(annIndexValidator(index, false));
}

TEST(ANNIndexDDLTest, CreatorProducesShapeFingerprint)
{
    auto index = makeIndexDescription({{"dim", Field(UInt64{128})}});
    auto created = annIndexCreator(index);
    ASSERT_NE(created, nullptr);

    const auto * ann_index = dynamic_cast<const MergeTreeIndexANN *>(created.get());
    ASSERT_NE(ann_index, nullptr);

    const auto & def = ann_index->getDefinition();
    EXPECT_EQ(def.shape.dim, 128u);
    EXPECT_EQ(def.shape.metric, 0u);
    EXPECT_EQ(def.shape.algorithm, "diskann");
    EXPECT_EQ(def.vector_column_name, "emb");
}

TEST(ANNIndexDDLTest, CreatorProducesCosineMetric)
{
    auto index = makeIndexDescription({{"metric", Field("Cosine")}, {"dim", Field(UInt64{64})}});
    auto created = annIndexCreator(index);
    const auto * ann_index = dynamic_cast<const MergeTreeIndexANN *>(created.get());
    ASSERT_NE(ann_index, nullptr);
    EXPECT_EQ(ann_index->getDefinition().shape.metric, 1u);
}

TEST(ANNIndexDDLTest, CreatorParamsHashIsStable)
{
    auto idx_a = makeIndexDescription({
        {"dim", Field(UInt64{128})},
        {"max_degree", Field(UInt64{32})},
        {"alpha", Field(Float64{1.2})},
    });
    auto idx_b = makeIndexDescription({
        {"max_degree", Field(UInt64{32})},   /// Same values, different insertion order.
        {"alpha", Field(Float64{1.2})},
        {"dim", Field(UInt64{128})},
    });

    auto a = std::dynamic_pointer_cast<const MergeTreeIndexANN>(annIndexCreator(idx_a));
    auto b = std::dynamic_pointer_cast<const MergeTreeIndexANN>(annIndexCreator(idx_b));
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(a->getDefinition().shape.params_hash, b->getDefinition().shape.params_hash);

    /// Changing `max_degree` must change the hash.
    auto idx_c = makeIndexDescription({
        {"dim", Field(UInt64{128})},
        {"max_degree", Field(UInt64{64})},
        {"alpha", Field(Float64{1.2})},
    });
    auto c = std::dynamic_pointer_cast<const MergeTreeIndexANN>(annIndexCreator(idx_c));
    ASSERT_NE(c, nullptr);
    EXPECT_NE(a->getDefinition().shape.params_hash, c->getDefinition().shape.params_hash);
}

TEST(ANNIndexDDLTest, HasANNIndexDetectsType)
{
    StorageInMemoryMetadata metadata;
    /// Empty metadata: no ANN index.
    EXPECT_FALSE(AlterCommands::hasANNIndex(metadata));

    auto index = makeIndexDescription({{"dim", Field(UInt64{128})}});
    metadata.secondary_indices.emplace_back(std::move(index));
    EXPECT_TRUE(AlterCommands::hasANNIndex(metadata));
}

TEST(ANNIndexDDLTest, HasANNIndexIgnoresOtherTypes)
{
    StorageInMemoryMetadata metadata;
    IndexDescription other;
    other.name = "idx_other";
    other.type = "minmax";
    metadata.secondary_indices.emplace_back(std::move(other));
    EXPECT_FALSE(AlterCommands::hasANNIndex(metadata));
}

TEST(ANNIndexDDLTest, ExtractANNShapeMatchesCreator)
{
    auto index = makeIndexDescription({{"dim", Field(UInt64{96})}, {"metric", Field("L2")}});
    StorageInMemoryMetadata metadata;
    metadata.secondary_indices.emplace_back(std::move(index));

    ANNIndexShapeFingerprint shape;
    ASSERT_TRUE(extractANNShapeFromMetadata(metadata, shape));
    EXPECT_EQ(shape.dim, 96u);
    EXPECT_EQ(shape.metric, 0u);
    EXPECT_EQ(shape.algorithm, "diskann");
    EXPECT_EQ(getANNIndexColumnName(metadata), "emb");
}

TEST(ANNIndexDDLTest, IndexFrontIsNoOp)
{
    auto index = makeIndexDescription({{"dim", Field(UInt64{128})}});
    auto created = annIndexCreator(index);
    ASSERT_NE(created, nullptr);

    /// The DDL front does not participate in per-granule storage / filtering; downstream
    /// code relies on these invariants when dispatching index descriptors.
    EXPECT_TRUE(created->getSubstreams().empty());
    EXPECT_FALSE(created->isVectorSimilarityIndex());
    EXPECT_FALSE(created->isTextIndex());
    EXPECT_EQ(created->createIndexGranule(), nullptr);
    EXPECT_EQ(created->createIndexAggregator(), nullptr);
}

#endif
