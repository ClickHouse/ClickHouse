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

const RandomSettingParameter trueOrFalse = [](RandomGenerator & rg) { return rg.nextBool() ? "1" : "0"; };

const RandomSettingParameter zeroOneTwo
    = [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 2)); };

const RandomSettingParameter zeroToThree
    = [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); };

extern std::unordered_map<String, CHSetting> performanceSettings;

extern std::unordered_map<String, CHSetting> serverSettings;

const std::unordered_map<String, CHSetting> memoryTableSettings
    = {{"min_bytes_to_keep",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"max_bytes_to_keep",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"min_rows_to_keep",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"max_rows_to_keep",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"compress", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> setTableSettings = {{"persistent", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> joinTableSettings = {{"persistent", CHSetting(trueOrFalse, {}, false)}};

const std::unordered_map<String, CHSetting> embeddedRocksDBTableSettings = {
    {"optimize_for_bulk_insert", CHSetting(trueOrFalse, {}, false)},
    {"bulk_insert_block_size",
     CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
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
                return rg.pickRandomlyFromVector(choices);
            },
            {},
            false)}};

const std::unordered_map<String, CHSetting> s3QueueTableSettings = {
    {"after_processing",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"''", "'keep'", "'delete'"};
             return rg.pickRandomlyFromVector(choices);
         },
         {},
         false)},
    {"enable_logging_to_s3queue_log", CHSetting(trueOrFalse, {}, false)},
    {"processing_threads_num",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1, std::thread::hardware_concurrency())); }, {}, false)}};

const std::unordered_map<String, CHSetting> distributedTableSettings
    = {{"background_insert_batch", CHSetting(trueOrFalse, {}, false)},
       {"background_insert_split_batch_on_failure", CHSetting(trueOrFalse, {}, false)},
       {"flush_on_detach", CHSetting(trueOrFalse, {}, false)},
       {"fsync_after_insert", CHSetting(trueOrFalse, {}, false)},
       {"fsync_directories", CHSetting(trueOrFalse, {}, false)},
       {"skip_unavailable_shards", CHSetting(trueOrFalse, {}, false)}};

extern std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allTableSettings;

const std::unordered_map<String, CHSetting> mergeTreeColumnSettings
    = {{"min_compress_block_size",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"max_compress_block_size",
        CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)}};

const std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allColumnSettings
    = {{MergeTree, mergeTreeColumnSettings},
       {ReplacingMergeTree, mergeTreeColumnSettings},
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
       {Merge, {}},
       {Distributed, {}}};

extern std::unique_ptr<SQLType> size_tp, null_tp;

extern std::unordered_map<String, DB::Strings> systemTables;

void loadFuzzerServerSettings(const FuzzConfig & fc);
void loadFuzzerTableSettings(const FuzzConfig & fc);
void loadSystemTables(const FuzzConfig & fc);

}
