#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ITokenizer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>

#include <gtest/gtest.h>

#include <algorithm>


using namespace DB;

namespace
{

bool hasSubstream(const MergeTreeIndexSubstreams & substreams, MergeTreeIndexSubstream::Type type, const String & suffix, const String & extension)
{
    return std::any_of(substreams.begin(), substreams.end(), [&](const auto & s)
    {
        return s.type == type && s.suffix == suffix && s.extension == extension;
    });
}

}

/// `getPotentialSubstreams` is what mark-cache eviction enumerates, so it must cover every
/// substream any index version could have written -- it may not be narrowed to the current
/// index definition, and it must stay I/O-free (it takes neither a part nor a storage).
TEST(MergeTreeIndexPotentialSubstreams, MinMaxCoversLegacyExtension)
{
    IndexDescription description;
    description.name = "mm";
    description.type = "minmax";

    MergeTreeIndexMinMax index(nullptr, description);

    /// Writers emit `.idx2` (v2) only, but a part written by an older version holds `.idx` (v1)
    /// and `getPhysicalFormat` may return either, so both have to be evicted.
    EXPECT_TRUE(hasSubstream(index.getSubstreams(), MergeTreeIndexSubstream::Type::Regular, "", ".idx2"));
    EXPECT_FALSE(hasSubstream(index.getSubstreams(), MergeTreeIndexSubstream::Type::Regular, "", ".idx"));

    const auto potential = index.getPotentialSubstreams();
    EXPECT_TRUE(hasSubstream(potential, MergeTreeIndexSubstream::Type::Regular, "", ".idx2"));
    EXPECT_TRUE(hasSubstream(potential, MergeTreeIndexSubstream::Type::Regular, "", ".idx"));
    EXPECT_EQ(potential.size(), 2);
}

TEST(MergeTreeIndexPotentialSubstreams, TextCoversPositionsWhenDefinitionHasNone)
{
    IndexDescription description;
    description.name = "txt";
    description.type = TEXT_INDEX_NAME;
    description.column_names = {"s"};
    description.data_types = {std::make_shared<DataTypeString>()};

    auto expression_list = make_intrusive<ASTExpressionList>();
    expression_list->children.push_back(make_intrusive<ASTIdentifier>("s"));
    description.expression_list_ast = expression_list;
    description.expression = std::make_shared<ExpressionActions>(ActionsDAG());

    /// `positions = 0`: the index definition asks for no positions, but a part written under an
    /// earlier definition still holds `.pos` marks, so eviction must still cover them.
    MergeTreeIndexTextParams params;
    params.positions = 0;

    MergeTreeIndexText index(nullptr, description, params, nullptr, nullptr);

    EXPECT_FALSE(hasSubstream(index.getSubstreams(), MergeTreeIndexSubstream::Type::TextIndexPositions, ".pos", ".idx"));

    const auto potential = index.getPotentialSubstreams();
    EXPECT_TRUE(hasSubstream(potential, MergeTreeIndexSubstream::Type::TextIndexPositions, ".pos", ".idx"));
    EXPECT_TRUE(hasSubstream(potential, MergeTreeIndexSubstream::Type::Regular, "", ".idx"));
    EXPECT_TRUE(hasSubstream(potential, MergeTreeIndexSubstream::Type::TextIndexDictionary, ".dct", ".idx"));
    EXPECT_TRUE(hasSubstream(potential, MergeTreeIndexSubstream::Type::TextIndexPostings, ".pst", ".idx"));
    EXPECT_EQ(potential.size(), 4);
}
