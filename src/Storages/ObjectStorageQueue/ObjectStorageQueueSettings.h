#pragma once

#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettingsDeclaration.h>
#include <Core/SettingsEnums.h>


namespace DB
{
class ASTStorage;


#define OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(M, ALIAS) \
    M(ObjectStorageQueueMode, \
      mode, \
      ObjectStorageQueueMode::ORDERED, \
      "With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKepeer." \
      "With ordered mode, only the max name of the successfully consumed file stored.", \
      0) \
    M(ObjectStorageQueueAction, after_processing, ObjectStorageQueueAction::KEEP, "Delete or keep file in after successful processing", 0) \
    M(String, keeper_path, "", "Zookeeper node path", 0) \
    M(UInt32, loading_retries, 10, "Retry loading up to specified number of times", 0) \
    M(UInt32, processing_threads_num, 1, "Number of processing threads", 0) \
    M(UInt32, enable_logging_to_queue_log, 1, "Enable logging to system table system.(s3/azure_)queue_log", 0) \
    M(String, last_processed_path, "", "For Ordered mode. Files that have lexicographically smaller file name are considered already processed", 0) \
    M(UInt32, tracked_file_ttl_sec, 0, "Maximum number of seconds to store processed files in ZooKeeper node (store forever by default)", 0) \
    M(UInt32, polling_min_timeout_ms, 1000, "Minimal timeout before next polling", 0) \
    M(UInt32, polling_max_timeout_ms, 10000, "Maximum timeout before next polling", 0) \
    M(UInt32, polling_backoff_ms, 1000, "Polling backoff", 0) \
    M(UInt32, tracked_files_limit, 1000, "For unordered mode. Max set size for tracking processed files in ZooKeeper", 0) \
    M(UInt32, cleanup_interval_min_ms, 60000, "For unordered mode. Polling backoff min for cleanup", 0) \
    M(UInt32, cleanup_interval_max_ms, 60000, "For unordered mode. Polling backoff max for cleanup", 0) \
    M(UInt32, buckets, 0, "Number of buckets for Ordered mode parallel processing", 0) \
    M(UInt32, max_processed_files_before_commit, 100, "Number of files which can be processed before being committed to keeper", 0) \
    M(UInt32, max_processed_rows_before_commit, 0, "Number of rows which can be processed before being committed to keeper", 0) \
    M(UInt32, max_processed_bytes_before_commit, 0, "Number of bytes which can be processed before being committed to keeper", 0) \
    M(UInt32, max_processing_time_sec_before_commit, 0, "Timeout in seconds after which to commit files committed to keeper", 0) \

#define LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS(M, ALIAS) \
    OBJECT_STORAGE_QUEUE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(ObjectStorageQueueSettingsTraits, LIST_OF_OBJECT_STORAGE_QUEUE_SETTINGS)


struct ObjectStorageQueueSettings : public BaseSettings<ObjectStorageQueueSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
