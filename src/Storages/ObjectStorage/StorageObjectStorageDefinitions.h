#pragma once

namespace DB
{

/// Important note:
/// `storage_engine_name` is not the type of the object storage the function works on top of.
/// This is the name of ClickHouse's storage engine.

struct AzureDefinition
{
    static constexpr auto name = "azureBlobStorage";
    static constexpr auto storage_engine_name = "AzureBlobStorage";
    static constexpr auto object_storage_type = "azure";
};

struct S3Definition
{
    static constexpr auto name = "s3";
    static constexpr auto storage_engine_name = "S3";
    static constexpr auto object_storage_type = "s3";
};

struct GCSDefinition
{
    static constexpr auto name = "gcs";
    static constexpr auto storage_engine_name = "GCS";
    static constexpr auto object_storage_type = "gcs";
};

struct COSNDefinition
{
    static constexpr auto name = "cosn";
    static constexpr auto storage_engine_name = "COSN";
    static constexpr auto object_storage_type = "cosn";
};

struct OSSDefinition
{
    static constexpr auto name = "oss";
    static constexpr auto storage_engine_name = "OSS";
    static constexpr auto object_storage_type = "oss";
};

struct HDFSDefinition
{
    static constexpr auto name = "hdfs";
    static constexpr auto storage_engine_name = "HDFS";
    static constexpr auto object_storage_type = "hdfs";
};

struct IcebergDefinition
{
    static constexpr auto name = "iceberg";
    static constexpr auto storage_engine_name = "Iceberg";
    static constexpr auto object_storage_type = "s3";
};

struct IcebergS3Definition
{
    static constexpr auto name = "icebergS3";
    static constexpr auto storage_engine_name = "IcebergS3";
    static constexpr auto object_storage_type = "s3";
};

struct IcebergAzureDefinition
{
    static constexpr auto name = "icebergAzure";
    static constexpr auto storage_engine_name = "IcebergAzure";
    static constexpr auto object_storage_type = "azure";
};

struct IcebergLocalDefinition
{
    static constexpr auto name = "icebergLocal";
    static constexpr auto storage_engine_name = "IcebergLocal";
    static constexpr auto object_storage_type = "azure";
};

struct IcebergHDFSDefinition
{
    static constexpr auto name = "icebergHDFS";
    static constexpr auto storage_engine_name = "IcebergHDFS";
    static constexpr auto object_storage_type = "hdfs";
};

struct DeltaLakeDefinition
{
    static constexpr auto name = "deltaLake";
    static constexpr auto storage_engine_name = "DeltaLake";
    static constexpr auto object_storage_type = "s3";
};

struct DeltaLakeS3Definition
{
    static constexpr auto name = "deltaLakeS3";
    static constexpr auto storage_engine_name = "DeltaLakeS3";
    static constexpr auto object_storage_type = "s3";
};

struct DeltaLakeAzureDefinition
{
    static constexpr auto name = "deltaLakeAzure";
    static constexpr auto storage_engine_name = "DeltaLakeAzure";
    static constexpr auto object_storage_type = "azure";
};

struct DeltaLakeLocalDefinition
{
    static constexpr auto name = "deltaLakeLocal";
    static constexpr auto storage_engine_name = "DeltaLakeLocal";
    static constexpr auto object_storage_type = "local";
};

struct HudiDefinition
{
    static constexpr auto name = "hudi";
    static constexpr auto storage_engine_name = "Hudi";
    static constexpr auto object_storage_type = "s3";
};

struct PaimonDefinition
{
    static constexpr auto name = "paimon";
    static constexpr auto storage_engine_name = S3Definition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct PaimonS3Definition
{
    static constexpr auto name = "paimonS3";
    static constexpr auto storage_engine_name = S3Definition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};


struct PaimonAzureDefinition
{
    static constexpr auto name = "paimonAzure";
    static constexpr auto storage_engine_name = AzureDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "azure";
};


struct PaimonHDFSDefinition
{
    static constexpr auto name = "paimonHDFS";
    static constexpr auto storage_engine_name = HDFSDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "hdfs";
};
struct PaimonLocalDefinition
{
    static constexpr auto name = "paimonLocal";
    static constexpr auto storage_engine_name = "File";
    static constexpr auto object_storage_type = "local";
};

/// Cluster functions

struct AzureClusterDefinition
{
    static constexpr auto name = "azureBlobStorageCluster";
    static constexpr auto storage_engine_name = "AzureBlobStorageCluster";
    static constexpr auto non_clustered_storage_engine_name = AzureDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "azure";
};

struct S3ClusterDefinition
{
    static constexpr auto name = "s3Cluster";
    static constexpr auto storage_engine_name = "S3Cluster";
    static constexpr auto non_clustered_storage_engine_name = S3Definition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct HDFSClusterDefinition
{
    static constexpr auto name = "hdfsCluster";
    static constexpr auto storage_engine_name = "HDFSCluster";
    static constexpr auto non_clustered_storage_engine_name = HDFSDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "hdfs";
};

struct IcebergClusterDefinition
{
    static constexpr auto name = "icebergCluster";
    static constexpr auto storage_engine_name = "IcebergCluster";
    static constexpr auto non_clustered_storage_engine_name = IcebergDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct IcebergS3ClusterDefinition
{
    static constexpr auto name = "icebergS3Cluster";
    static constexpr auto storage_engine_name = "IcebergS3Cluster";
    static constexpr auto non_clustered_storage_engine_name = IcebergS3Definition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct IcebergAzureClusterDefinition
{
    static constexpr auto name = "icebergAzureCluster";
    static constexpr auto storage_engine_name = "IcebergAzureCluster";
    static constexpr auto non_clustered_storage_engine_name = IcebergAzureDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "azure";
};

struct IcebergHDFSClusterDefinition
{
    static constexpr auto name = "icebergHDFSCluster";
    static constexpr auto storage_engine_name = "IcebergHDFSCluster";
    static constexpr auto non_clustered_storage_engine_name = IcebergHDFSDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "hdfs";
};

struct DeltaLakeClusterDefinition
{
    static constexpr auto name = "deltaLakeCluster";
    static constexpr auto storage_engine_name = "DeltaLakeCluster";
    static constexpr auto non_clustered_storage_engine_name = DeltaLakeDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct DeltaLakeS3ClusterDefinition
{
    static constexpr auto name = "deltaLakeS3Cluster";
    static constexpr auto storage_engine_name = "DeltaLakeS3Cluster";
    static constexpr auto non_clustered_storage_engine_name = DeltaLakeS3Definition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct DeltaLakeAzureClusterDefinition
{
    static constexpr auto name = "deltaLakeAzureCluster";
    static constexpr auto storage_engine_name = "DeltaLakeAzureCluster";
    static constexpr auto non_clustered_storage_engine_name = DeltaLakeAzureDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "azure";
};

struct HudiClusterDefinition
{
    static constexpr auto name = "hudiCluster";
    static constexpr auto storage_engine_name = "HudiS3Cluster";
    static constexpr auto non_clustered_storage_engine_name = HudiDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct PaimonClusterDefinition
{
    static constexpr auto name = "paimonCluster";
    static constexpr auto storage_engine_name = "PaimonCluster";
    static constexpr auto non_clustered_storage_engine_name = PaimonDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct PaimonS3ClusterDefinition
{
    static constexpr auto name = "paimonS3Cluster";
    static constexpr auto storage_engine_name = "PaimonS3Cluster";
    static constexpr auto non_clustered_storage_engine_name = PaimonS3Definition::storage_engine_name;
    static constexpr auto object_storage_type = "s3";
};

struct PaimonAzureClusterDefinition
{
    static constexpr auto name = "paimonAzureCluster";
    static constexpr auto storage_engine_name = "PaimonAzureCluster";
    static constexpr auto non_clustered_storage_engine_name = PaimonAzureDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "azure";
};

struct PaimonHDFSClusterDefinition
{
    static constexpr auto name = "paimonHDFSCluster";
    static constexpr auto storage_engine_name = "PaimonHDFSCluster";
    static constexpr auto non_clustered_storage_engine_name = PaimonHDFSDefinition::storage_engine_name;
    static constexpr auto object_storage_type = "hdfs";
};

}
