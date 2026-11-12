#include <gtest/gtest.h>

#include "config.h"

#if USE_AVRO

#include <Common/re2.h>
#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <filesystem>

using namespace DB;

namespace
{

String generateCatalogMetadataFileName(Int32 base_version, const String & base_metadata_path, CompressionMethod compression_method = CompressionMethod::None)
{
    FileNamesGenerator generator("s3://bucket/table", /* use_uuid_in_metadata */ true, compression_method, "Parquet");
    generator.setBaseMetadataFile(base_version, base_metadata_path);
    return std::filesystem::path(generator.generateMetadataPathWithInfo().path.serialize()).filename();
}

}

/// Iceberg Java (`BaseMetastoreTableOperations.parseVersion`) and pyiceberg (`_parse_metadata_version`) parse the version
/// only from `<digits>-<uuid>.metadata.json`. Any other name makes them restart the table version at 0.
TEST(IcebergMetadataFileNames, CatalogTableNamesFollowTheMetastoreConvention)
{
    const re2::RE2 metastore_name(R"(\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json)");

    for (const auto & base : {"s3://bucket/table/metadata/v2.metadata.json", "s3://bucket/table/metadata/00002-94da4c75-1b9c-4a4f-8a3e-2f6e1c0d9b11.metadata.json"})
    {
        auto name = generateCatalogMetadataFileName(2, base);
        EXPECT_TRUE(re2::RE2::FullMatch(name, metastore_name)) << name;
        EXPECT_TRUE(name.starts_with("00003-")) << name;
        EXPECT_EQ(Iceberg::getMetadataFileAndVersion("s3://bucket/table/metadata/" + name).version, 3);
    }

    auto compressed = generateCatalogMetadataFileName(41, "s3://bucket/table/metadata/00041-x.gz.metadata.json", CompressionMethod::Gzip);
    EXPECT_TRUE(compressed.starts_with("00042-") && compressed.ends_with(".gz.metadata.json")) << compressed;
    EXPECT_EQ(Iceberg::getMetadataFileAndVersion("s3://bucket/table/metadata/" + compressed).version, 42);
}

/// Writers that commit on top of the same base must pick the same name, so that only one of them can write it.
/// A commit on top of any other base must get a new name, even if the version number is the same.
TEST(IcebergMetadataFileNames, CatalogTableNameIsUniquePerBase)
{
    const String base = "s3://bucket/table/metadata/00000-8cd3628e-0f0a-4d55-bd1d-6c4a9e3b2a10.metadata.json";
    const String other_base = "s3://bucket/table/metadata/00000-99c11667-2c8e-4b0a-9d6f-0e5b7a8c1d22.metadata.json";

    EXPECT_EQ(generateCatalogMetadataFileName(0, base), generateCatalogMetadataFileName(0, base));
    EXPECT_NE(generateCatalogMetadataFileName(0, base), generateCatalogMetadataFileName(0, other_base));
    EXPECT_NE(generateCatalogMetadataFileName(0, "s3://bucket/table/metadata/v0.metadata.json"), generateCatalogMetadataFileName(0, base));
}

TEST(IcebergMetadataFileNames, TableWithoutCatalogKeepsVersionedNames)
{
    FileNamesGenerator generator("s3://bucket/table", /* use_uuid_in_metadata */ false, CompressionMethod::None, "Parquet");
    generator.setVersion(3);
    EXPECT_EQ(generator.generateMetadataPathWithInfo().path.serialize(), "s3://bucket/table/metadata/v3.metadata.json");
}

#endif
