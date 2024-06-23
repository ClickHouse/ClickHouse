#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>


namespace DB
{
class ASTStorage;


#define S3QUEUE_RELATED_SETTINGS(M, ALIAS) \
    M(S3QueueMode, \
      mode, \
      S3QueueMode::ORDERED, \
      "With unordered mode, the set of all already processed files is tracked with persistent nodes in ZooKepeer." \
      "With ordered mode, only the max name of the successfully consumed file stored.", \
      0) \
    M(S3QueueAction, after_processing, S3QueueAction::KEEP, "Delete or keep file in S3 after successful processing", 0) \
    M(String, keeper_path, "", "Zookeeper node path", 0) \
    M(UInt32, s3queue_loading_retries, 0, "Retry loading up to specified number of times", 0) \
    M(UInt32, s3queue_processing_threads_num, 1, "Number of processing threads", 0) \
    M(UInt32, s3queue_enable_logging_to_s3queue_log, 1, "Enable logging to system table system.s3queue_log", 0) \
    M(String, s3queue_last_processed_path, "", "For Ordered mode. Files that have lexicographically smaller file name are considered already processed", 0) \
    M(UInt32, s3queue_tracked_file_ttl_sec, 0, "Maximum number of seconds to store processed files in ZooKeeper node (store forever by default)", 0) \
    M(UInt32, s3queue_polling_min_timeout_ms, 1000, "Minimal timeout before next polling", 0) \
    M(UInt32, s3queue_polling_max_timeout_ms, 10000, "Maximum timeout before next polling", 0) \
    M(UInt32, s3queue_polling_backoff_ms, 1000, "Polling backoff", 0) \
    M(UInt32, s3queue_tracked_files_limit, 1000, "For unordered mode. Max set size for tracking processed files in ZooKeeper", 0) \
    M(UInt32, s3queue_cleanup_interval_min_ms, 60000, "For unordered mode. Polling backoff min for cleanup", 0) \
    M(UInt32, s3queue_cleanup_interval_max_ms, 60000, "For unordered mode. Polling backoff max for cleanup", 0) \
    M(UInt32, s3queue_buckets, 0, "Number of buckets for Ordered mode parallel processing", 0) \

#define LIST_OF_S3QUEUE_SETTINGS(M, ALIAS) \
    S3QUEUE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(S3QueueSettingsTraits, LIST_OF_S3QUEUE_SETTINGS)


struct S3QueueSettings : public BaseSettings<S3QueueSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
