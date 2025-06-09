#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>

#include <cstdint>
#include <functional>
#include <thread>

namespace BuzzHouse
{

const auto trueOrFalse = [](RandomGenerator & rg) { return rg.nextBool() ? "1" : "0"; };

const auto zeroOneTwo = [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 2)); };

const auto zeroToThree = [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); };

const auto probRange = [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); };

const auto highRange = [](RandomGenerator & rg)
{
    const auto val = rg.randomInt<uint32_t>(0, 25);
    return std::to_string(val == UINT32_C(0) ? UINT32_C(0) : (UINT32_C(1) << (val - UINT32_C(1))));
};

const auto rowsRange = [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.7, 0, UINT32_C(8192))); };

const auto bytesRange = [](RandomGenerator & rg)
{ return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); };

const auto threadSetting = CHSetting(
    [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, std::thread::hardware_concurrency())); },
    {"0", "1", std::to_string(std::thread::hardware_concurrency())},
    false);

const auto probRangeSetting = CHSetting(probRange, {"0", "0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999", "1.0"}, false);

const auto trueOrFalseSetting = CHSetting(trueOrFalse, {"0", "1"}, false);

const auto trueOrFalseSettingNoOracle = CHSetting(trueOrFalse, {}, false);

extern std::unordered_map<String, CHSetting> hotSettings;

extern std::unordered_map<String, CHSetting> serverSettings;

extern std::unordered_map<String, CHSetting> performanceSettings;

extern std::unordered_map<String, CHSetting> queryOracleSettings;

extern std::unordered_map<String, CHSetting> formatSettings;

const std::unordered_map<String, CHSetting> memoryTableSettings
    = {{"min_bytes_to_keep", CHSetting(bytesRange, {}, false)},
       {"max_bytes_to_keep", CHSetting(bytesRange, {}, false)},
       {"min_rows_to_keep", CHSetting(rowsRange, {}, false)},
       {"max_rows_to_keep", CHSetting(rowsRange, {}, false)},
       {"compress", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> setTableSettings = {{"persistent", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> joinTableSettings = {{"persistent", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> embeddedRocksDBTableSettings = {
    {"optimize_for_bulk_insert", CHSetting(trueOrFalse, {}, false)},
    {"bulk_insert_block_size", CHSetting(highRange, {}, false)},
};

const std::unordered_map<String, CHSetting> mySQLTableSettings
    = {{"connection_pool_size",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 7)); }, {}, false)},
       {"connection_max_tries", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1, 16)); }, {}, false)},
       {"connection_auto_close", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> fileTableSettings
    = {{"engine_file_allow_create_multiple_files", CHSetting(trueOrFalse, {}, false)},
       {"engine_file_empty_if_not_exists", CHSetting(trueOrFalse, {}, false)},
       {"engine_file_skip_empty_files", CHSetting(trueOrFalse, {}, false)},
       {"engine_file_truncate_on_insert", CHSetting(trueOrFalse, {}, false)},
       {"storage_file_read_method",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'read'", "'pread'", "'mmap'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)}};

const std::unordered_map<String, CHSetting> s3QueueTableSettings
    = {{"after_processing",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"''", "'keep'", "'delete'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"enable_logging_to_s3queue_log", CHSetting(trueOrFalse, {}, false)},
       {"parallel_inserts", CHSetting(trueOrFalse, {}, false)},
       {"processing_threads_num", threadSetting}};

const std::unordered_map<String, CHSetting> distributedTableSettings
    = {{"background_insert_batch", CHSetting(trueOrFalse, {}, false)},
       {"background_insert_split_batch_on_failure", CHSetting(trueOrFalse, {}, false)},
       {"flush_on_detach", CHSetting(trueOrFalse, {}, false)},
       {"fsync_after_insert", CHSetting(trueOrFalse, {}, false)},
       {"fsync_directories", CHSetting(trueOrFalse, {}, false)},
       {"skip_unavailable_shards", CHSetting(trueOrFalse, {}, false)}};

extern std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allTableSettings;

const std::unordered_map<String, CHSetting> mergeTreeColumnSettings
    = {{"min_compress_block_size", CHSetting(highRange, {"4", "8", "32", "64", "1024", "4096", "1000000"}, false)},
       {"max_compress_block_size", CHSetting(highRange, {"4", "8", "32", "64", "1024", "4096", "1000000"}, false)}};

const std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allColumnSettings
    = {{MergeTree, mergeTreeColumnSettings},
       {ReplacingMergeTree, mergeTreeColumnSettings},
       {CoalescingMergeTree, mergeTreeColumnSettings},
       {SummingMergeTree, mergeTreeColumnSettings},
       {AggregatingMergeTree, mergeTreeColumnSettings},
       {CollapsingMergeTree, mergeTreeColumnSettings},
       {VersionedCollapsingMergeTree, mergeTreeColumnSettings},
       {File, {}},
       {Null, {}},
       {Set, {}},
       {Join, {}},
       {Memory, {}},
       {StripeLog, {}},
       {Log, {}},
       {TinyLog, {}},
       {EmbeddedRocksDB, {}},
       {Buffer, {}},
       {MySQL, {}},
       {PostgreSQL, {}},
       {SQLite, {}},
       {MongoDB, {}},
       {Redis, {}},
       {S3, {}},
       {S3Queue, {}},
       {Hudi, {}},
       {DeltaLake, {}},
       {IcebergS3, {}},
       {IcebergAzure, {}},
       {IcebergLocal, {}},
       {Merge, {}},
       {Distributed, {}},
       {Dictionary, {}},
       {GenerateRandom, {}},
       {AzureBlobStorage, {}},
       {AzureQueue, {}},
       {URL, {}},
       {KeeperMap, {}},
       {ExternalDistributed, {}},
       {MaterializedPostgreSQL, {}}};

const std::unordered_map<String, CHSetting> backupSettings
    = {{"allow_azure_native_copy", CHSetting(trueOrFalse, {}, false)},
       {"allow_backup_broken_projections", CHSetting(trueOrFalse, {}, false)},
       {"allow_checksums_from_remote_paths", CHSetting(trueOrFalse, {}, false)},
       {"allow_s3_native_copy", CHSetting(trueOrFalse, {}, false)},
       {"async", CHSetting(trueOrFalse, {}, false)},
       {"azure_attempt_to_create_container", CHSetting(trueOrFalse, {}, false)},
       {"check_parts", CHSetting(trueOrFalse, {}, false)},
       {"check_projection_parts", CHSetting(trueOrFalse, {}, false)},
       {"decrypt_files_from_encrypted_disks", CHSetting(trueOrFalse, {}, false)},
       {"deduplicate_files", CHSetting(trueOrFalse, {}, false)},
       {"experimental_lightweight_snapshot", CHSetting(trueOrFalse, {}, false)},
       {"internal", CHSetting(trueOrFalse, {}, false)},
       {"read_from_filesystem_cache", CHSetting(trueOrFalse, {}, false)},
       {"s3_storage_class", CHSetting([](RandomGenerator &) { return "'STANDARD'"; }, {}, false)},
       {"structure_only", CHSetting(trueOrFalse, {}, false)},
       {"write_access_entities_dependents", CHSetting(trueOrFalse, {}, false)}};

extern std::unordered_map<String, CHSetting> restoreSettings;

extern std::unique_ptr<SQLType> size_tp, null_tp;

extern std::unordered_map<String, DB::Strings> systemTables;

extern std::unordered_map<DictionaryLayouts, std::unordered_map<String, CHSetting>> allDictionaryLayoutSettings;

void loadFuzzerServerSettings(const FuzzConfig & fc);
void loadFuzzerTableSettings(const FuzzConfig & fc);
void loadSystemTables(FuzzConfig & fc);

}
