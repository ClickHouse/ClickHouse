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

/// Without a catalog the name is the commit lock, so every writer of a version must aim at the same name.
TEST(IcebergMetadataFileNames, TableWithoutCatalogContinuesTheNamingOfTheBase)
{
    auto next_name = [](const String & base)
    {
        FileNamesGenerator generator("s3://bucket/table", /* use_uuid_in_metadata */ false, CompressionMethod::None, "Parquet");
        generator.setBaseMetadataFile(4, "s3://bucket/table/metadata/" + base);
        return String(std::filesystem::path(generator.generateMetadataPathWithInfo().path.serialize()).filename());
    };
    EXPECT_EQ(next_name("v4.metadata.json"), "v5.metadata.json");
    const String numbered = next_name("00004-3f2b6c1e-8a43-4d5e-9b7f-0c1d2e3f4a5b.metadata.json");
    EXPECT_TRUE(numbered.starts_with("00005-") && numbered.ends_with(".metadata.json") && numbered.size() == 56) << numbered;
    EXPECT_FALSE(Iceberg::isVersionNumberedCommitScheme(numbered));
    EXPECT_EQ(numbered, next_name("00004-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d.metadata.json"));
    const String versioned = next_name("v4-3f2b6c1e-8a43-4d5e-9b7f-0c1d2e3f4a5b.metadata.json");
    EXPECT_TRUE(versioned.starts_with("v5-") && versioned.ends_with(".metadata.json") && versioned.size() == 53) << versioned;
    EXPECT_FALSE(Iceberg::isVersionNumberedCommitScheme(versioned));
    EXPECT_EQ(versioned, next_name("v4-0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d.metadata.json"));
}

#endif
