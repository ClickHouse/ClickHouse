#pragma once
#include <Interpreters/Context_fwd.h>
#include <Core/Settings.h>
#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageAzureThreads;
    extern const Metric ObjectStorageAzureThreadsActive;
    extern const Metric ObjectStorageAzureThreadsScheduled;

    extern const Metric ObjectStorageS3Threads;
    extern const Metric ObjectStorageS3ThreadsActive;
    extern const Metric ObjectStorageS3ThreadsScheduled;
}

namespace DB
{

struct StorageObjectStorageSettings
{
    bool truncate_on_insert;
    bool create_new_file_on_insert;
    bool schema_inference_use_cache;
    SchemaInferenceMode schema_inference_mode;
};

struct S3StorageSettings
{
    static StorageObjectStorageSettings create(const Settings & settings)
    {
        return StorageObjectStorageSettings{
            .truncate_on_insert = settings.s3_truncate_on_insert,
            .create_new_file_on_insert = settings.s3_create_new_file_on_insert,
            .schema_inference_use_cache = settings.schema_inference_use_cache_for_s3,
            .schema_inference_mode = settings.schema_inference_mode,
        };
    }

    static constexpr auto SCHEMA_CACHE_MAX_ELEMENTS_CONFIG_SETTING = "schema_inference_cache_max_elements_for_s3";

    static CurrentMetrics::Metric ObjectStorageThreads() { return CurrentMetrics::ObjectStorageS3Threads; } /// NOLINT
    static CurrentMetrics::Metric ObjectStorageThreadsActive() { return CurrentMetrics::ObjectStorageS3ThreadsActive; } /// NOLINT
    static CurrentMetrics::Metric ObjectStorageThreadsScheduled() { return CurrentMetrics::ObjectStorageS3ThreadsScheduled; } /// NOLINT
};

struct AzureStorageSettings
{
    static StorageObjectStorageSettings create(const Settings & settings)
    {
        return StorageObjectStorageSettings{
            .truncate_on_insert = settings.azure_truncate_on_insert,
            .create_new_file_on_insert = settings.azure_create_new_file_on_insert,
            .schema_inference_use_cache = settings.schema_inference_use_cache_for_azure,
            .schema_inference_mode = settings.schema_inference_mode,
        };
    }

    static constexpr auto SCHEMA_CACHE_MAX_ELEMENTS_CONFIG_SETTING = "schema_inference_cache_max_elements_for_azure";

    static CurrentMetrics::Metric ObjectStorageThreads() { return CurrentMetrics::ObjectStorageAzureThreads; } /// NOLINT
    static CurrentMetrics::Metric ObjectStorageThreadsActive() { return CurrentMetrics::ObjectStorageAzureThreadsActive; } /// NOLINT
    static CurrentMetrics::Metric ObjectStorageThreadsScheduled() { return CurrentMetrics::ObjectStorageAzureThreadsScheduled; } /// NOLINT
};

struct HDFSStorageSettings
{
    static StorageObjectStorageSettings create(const Settings & settings)
    {
        return StorageObjectStorageSettings{
            .truncate_on_insert = settings.hdfs_truncate_on_insert,
            .create_new_file_on_insert = settings.hdfs_create_new_file_on_insert,
            .schema_inference_use_cache = settings.schema_inference_use_cache_for_hdfs,
            .schema_inference_mode = settings.schema_inference_mode,
        };
    }

    static constexpr auto SCHEMA_CACHE_MAX_ELEMENTS_CONFIG_SETTING = "schema_inference_cache_max_elements_for_hdfs";

    /// TODO: s3 -> hdfs
    static CurrentMetrics::Metric ObjectStorageThreads() { return CurrentMetrics::ObjectStorageS3Threads; } /// NOLINT
    static CurrentMetrics::Metric ObjectStorageThreadsActive() { return CurrentMetrics::ObjectStorageS3ThreadsActive; } /// NOLINT
    static CurrentMetrics::Metric ObjectStorageThreadsScheduled() { return CurrentMetrics::ObjectStorageS3ThreadsScheduled; } /// NOLINT
};

}
