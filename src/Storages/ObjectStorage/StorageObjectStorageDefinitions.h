#pragma once

namespace DB
{

/// Important note.
/// `storage_type_name` is not the type of the object storage the function
/// works on top of.
/// This is the name of ClickHouse's storage engine.

struct AzureDefinition
{
    static constexpr auto name = "azureBlobStorage";
    static constexpr auto storage_type_name = "AzureBlobStorage";
};

struct S3Definition
{
    static constexpr auto name = "s3";
    static constexpr auto storage_type_name = "S3";
};

struct GCSDefinition
{
    static constexpr auto name = "gcs";
    static constexpr auto storage_type_name = "GCS";
};

struct COSNDefinition
{
    static constexpr auto name = "cosn";
    static constexpr auto storage_type_name = "COSN";
};

struct OSSDefinition
{
    static constexpr auto name = "oss";
    static constexpr auto storage_type_name = "OSS";
};

struct HDFSDefinition
{
    static constexpr auto name = "hdfs";
    static constexpr auto storage_type_name = "HDFS";
};

struct IcebergDefinition
{
    static constexpr auto name = "iceberg";
    static constexpr auto storage_type_name = "Iceberg";
};

struct IcebergS3Definition
{
    static constexpr auto name = "icebergS3";
    static constexpr auto storage_type_name = "IcebergS3";
};

struct IcebergAzureDefinition
{
    static constexpr auto name = "icebergAzure";
    static constexpr auto storage_type_name = "IcebergAzure";
};

struct IcebergLocalDefinition
{
    static constexpr auto name = "icebergLocal";
    static constexpr auto storage_type_name = "IcebergLocal";
};

struct IcebergHDFSDefinition
{
    static constexpr auto name = "icebergHDFS";
    static constexpr auto storage_type_name = "IcebergHDFS";
};

struct DeltaLakeDefinition
{
    static constexpr auto name = "deltaLake";
    static constexpr auto storage_type_name = "DeltaLake";
};

struct DeltaLakeS3Definition
{
    static constexpr auto name = "deltaLakeS3";
    static constexpr auto storage_type_name = "DeltaLakeS3";
};

struct DeltaLakeAzureDefinition
{
    static constexpr auto name = "deltaLakeAzure";
    static constexpr auto storage_type_name = "DeltaLakeAzure";
};

struct DeltaLakeLocalDefinition
{
    static constexpr auto name = "deltaLakeLocal";
    static constexpr auto storage_type_name = "DeltaLakeLocal";
};

struct HudiDefinition
{
    static constexpr auto name = "hudi";
    static constexpr auto storage_type_name = "Hudi";
};


/// Cluster functions

struct AzureClusterDefinition
{
    static constexpr auto name = "azureBlobStorageCluster";
    static constexpr auto storage_type_name = "AzureBlobStorageCluster";
    static constexpr auto non_clustered_storage_type_name = AzureDefinition::storage_type_name;
};

struct S3ClusterDefinition
{
    static constexpr auto name = "s3Cluster";
    static constexpr auto storage_type_name = "S3Cluster";
    static constexpr auto non_clustered_storage_type_name = S3Definition::storage_type_name;
};

struct HDFSClusterDefinition
{
    static constexpr auto name = "hdfsCluster";
    static constexpr auto storage_type_name = "HDFSCluster";
    static constexpr auto non_clustered_storage_type_name = HDFSDefinition::storage_type_name;
};

struct IcebergS3ClusterDefinition
{
    static constexpr auto name = "icebergS3Cluster";
    static constexpr auto storage_type_name = "IcebergS3Cluster";
    static constexpr auto non_clustered_storage_type_name = IcebergS3Definition::storage_type_name;
};

struct IcebergAzureClusterDefinition
{
    static constexpr auto name = "icebergAzureCluster";
    static constexpr auto storage_type_name = "IcebergAzureCluster";
    static constexpr auto non_clustered_storage_type_name = IcebergAzureDefinition::storage_type_name;
};

struct IcebergHDFSClusterDefinition
{
    static constexpr auto name = "icebergHDFSCluster";
    static constexpr auto storage_type_name = "IcebergHDFSCluster";
    static constexpr auto non_clustered_storage_type_name = IcebergHDFSDefinition::storage_type_name;
};

struct DeltaLakeClusterDefinition
{
    static constexpr auto name = "deltaLakeCluster";
    static constexpr auto storage_type_name = "DeltaLakeS3Cluster";
    static constexpr auto non_clustered_storage_type_name = DeltaLakeDefinition::storage_type_name;
};

struct HudiClusterDefinition
{
    static constexpr auto name = "hudiCluster";
    static constexpr auto storage_type_name = "HudiS3Cluster";
    static constexpr auto non_clustered_storage_type_name = HudiDefinition::storage_type_name;
};

}
