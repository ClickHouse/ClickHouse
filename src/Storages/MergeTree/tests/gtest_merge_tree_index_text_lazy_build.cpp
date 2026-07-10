#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
}

using namespace DB;

namespace
{

MergeTreeIndexPtr createTextIndex(const String & definition)
{
    auto context = getContext().context;

    ParserIndexDeclaration parser;
    ASTPtr ast = parseQuery(parser, definition, 0, 0, 0);

    ColumnsDescription columns{NamesAndTypesList{{"str", std::make_shared<DataTypeString>()}}};

    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->setColumns(columns);

    IndicesDescription indices;
    indices.push_back(IndexDescription::getIndexFromAST(ast, columns, false, false, context));
    metadata->setSecondaryIndices(indices);

    /// The index object keeps a reference to the description, so pass the metadata-owned one.
    MergeTreeSettings settings(context->getMergeTreeSettings());
    return MergeTreeIndexFactory::instance().get(metadata, metadata->getSecondaryIndices().front(), settings);
}

}

/// Constructing a text index object must not analyze the preprocessor and postprocessor expressions.
/// Metadata-only paths construct index objects per data part, where such analysis is too expensive.
TEST(MergeTreeIndexTextTest, LazyPreprocessorBuild)
{
    tryRegisterFunctions();

    auto index = createTextIndex(
        "idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str)) GRANULARITY 1");

    EXPECT_NO_THROW(index->createIndexAggregator());

    /// An expression with an unknown function fails only on first real use of the index,
    /// not when the index object is constructed for metadata inspection.
    auto broken_index = createTextIndex(
        "idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = unknown_function_12345(str)) GRANULARITY 1");

    EXPECT_FALSE(broken_index->getFileName().empty());

    auto expect_unknown_function = [](auto && callback)
    {
        try
        {
            callback();
            ADD_FAILURE() << "Expected UNKNOWN_FUNCTION exception";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_FUNCTION);
        }
    };

    expect_unknown_function([&] { broken_index->createIndexAggregator(); });
    /// The null predicate is never used because the lazy build throws first.
    expect_unknown_function([&] { broken_index->createIndexCondition(nullptr, getContext().context); });
}
