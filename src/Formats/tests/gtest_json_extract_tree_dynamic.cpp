#include <gtest/gtest.h>

#include "config.h"

#if USE_SIMDJSON

#include <Columns/ColumnDynamic.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/JSONExtractTree.h>

using namespace DB;

namespace
{

/// Inserts one JSON element into a `Dynamic` column through the JSON extract tree
/// (the path taken by JSON input formats and `JSONExtract` functions).
void insertJSONElementIntoDynamic(IColumn & column, const String & json, const JSONExtractInsertSettings & insert_settings)
{
    SimdJSONParser parser;
    SimdJSONParser::Element element;
    ASSERT_TRUE(parser.parse(json, element));

    auto node = buildJSONExtractTree<SimdJSONParser>(std::make_shared<DataTypeDynamic>(), "test");
    FormatSettings format_settings;
    String error;
    ASSERT_TRUE(node->insertResultToColumn(column, element, insert_settings, format_settings, error)) << error;
}

}

/// `JSON(...)` variants must stay partitioned by exact storage type: a JSON object element
/// (inferred as plain `JSON`) must not be routed into an existing `JSON(...)` variant with a
/// different storage type, neither by the try-existing-variants fast path nor by the
/// merge-compatible variant lookup of the generic path.
TEST(JSONExtractTreeDynamic, InsertObjectRequiresExactStorageFastPath)
{
    auto column = ColumnDynamic::create(254);
    auto parameterized_type = DataTypeFactory::instance().get("JSON(max_dynamic_paths=0)");
    ASSERT_TRUE(column->addNewVariant(parameterized_type));

    JSONExtractInsertSettings insert_settings;
    insert_settings.try_existing_variants_in_dynamic_first = true;
    insertJSONElementIntoDynamic(*column, "{\"a\":1}", insert_settings);

    ASSERT_EQ(column->getTypeNameAt(0), "JSON");
    const auto & variant_info = column->getVariantInfo();
    auto parameterized_discr = variant_info.variant_name_to_discriminator.at(parameterized_type->getName());
    ASSERT_TRUE(column->getVariantColumn().getVariantByGlobalDiscriminator(parameterized_discr).empty());
}

TEST(JSONExtractTreeDynamic, InsertObjectRequiresExactStorageGenericPath)
{
    auto column = ColumnDynamic::create(254);
    auto parameterized_type = DataTypeFactory::instance().get("JSON(max_dynamic_paths=0)");
    ASSERT_TRUE(column->addNewVariant(parameterized_type));

    JSONExtractInsertSettings insert_settings;
    insert_settings.try_existing_variants_in_dynamic_first = false;
    insertJSONElementIntoDynamic(*column, "{\"a\":1}", insert_settings);

    ASSERT_EQ(column->getTypeNameAt(0), "JSON");
    const auto & variant_info = column->getVariantInfo();
    auto parameterized_discr = variant_info.variant_name_to_discriminator.at(parameterized_type->getName());
    ASSERT_TRUE(column->getVariantColumn().getVariantByGlobalDiscriminator(parameterized_discr).empty());
}

/// The guard must not stop an element from reusing a storage-compatible `JSON` variant.
TEST(JSONExtractTreeDynamic, InsertObjectReusesStorageCompatibleVariant)
{
    auto column = ColumnDynamic::create(254);
    auto plain_json_type = DataTypeFactory::instance().get("JSON");
    ASSERT_TRUE(column->addNewVariant(plain_json_type));

    JSONExtractInsertSettings insert_settings;
    insert_settings.try_existing_variants_in_dynamic_first = true;
    insertJSONElementIntoDynamic(*column, "{\"a\":1}", insert_settings);

    ASSERT_EQ(column->getTypeNameAt(0), "JSON");
    const auto & variant_info = column->getVariantInfo();
    auto plain_json_discr = variant_info.variant_name_to_discriminator.at(plain_json_type->getName());
    ASSERT_EQ(column->getVariantColumn().getVariantByGlobalDiscriminator(plain_json_discr).size(), 1);
}

#endif
