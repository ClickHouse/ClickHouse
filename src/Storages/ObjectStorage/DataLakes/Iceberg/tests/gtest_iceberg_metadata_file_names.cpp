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

String generateCatalogMetadataFileName(Int32 base_version, CompressionMethod compression_method = CompressionMethod::None)
{
    FileNamesGenerator generator("s3://bucket/table", /* use_uuid_in_metadata */ true, compression_method, "Parquet");
    generator.setVersion(base_version + 1);
    return std::filesystem::path(generator.generateMetadataPathWithInfo().path.serialize()).filename();
}

}

/// Iceberg Java (`BaseMetastoreTableOperations.parseVersion`) and pyiceberg (`_parse_metadata_version`) parse the version
/// only from `<digits>-<uuid>.metadata.json`. Any other name makes them restart the table version at 0.
TEST(IcebergMetadataFileNames, CatalogTableNamesFollowTheMetastoreConvention)
{
    const re2::RE2 metastore_name(R"(\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json)");

    auto name = generateCatalogMetadataFileName(2);
    EXPECT_TRUE(re2::RE2::FullMatch(name, metastore_name)) << name;
    EXPECT_TRUE(name.starts_with("00003-")) << name;
    EXPECT_EQ(Iceberg::getMetadataFileAndVersion("s3://bucket/table/metadata/" + name).version, 3);

    auto compressed = generateCatalogMetadataFileName(41, CompressionMethod::Gzip);
    EXPECT_TRUE(compressed.starts_with("00042-") && compressed.ends_with(".gz.metadata.json")) << compressed;
    EXPECT_EQ(Iceberg::getMetadataFileAndVersion("s3://bucket/table/metadata/" + compressed).version, 42);
}

/// A file that a failed commit left behind must not take the name of the next commit on top of the same base.
TEST(IcebergMetadataFileNames, CatalogTableNameIsUniquePerCommit)
{
    EXPECT_NE(generateCatalogMetadataFileName(0), generateCatalogMetadataFileName(0));
}

TEST(IcebergMetadataFileNames, TableWithoutCatalogKeepsVersionedNames)
{
    FileNamesGenerator generator("s3://bucket/table", /* use_uuid_in_metadata */ false, CompressionMethod::None, "Parquet");
    generator.setVersion(3);
    EXPECT_EQ(generator.generateMetadataPathWithInfo().path.serialize(), "s3://bucket/table/metadata/v3.metadata.json");
}

#endif
