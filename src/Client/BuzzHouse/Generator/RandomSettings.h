#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <cstdint>
#include <functional>
#include <thread>

namespace BuzzHouse
{

const std::function<void(RandomGenerator &, std::string &)> trueOrFalse
    = [](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "1" : "0"; };

const std::function<void(RandomGenerator &, std::string &)> zeroOneTwo
    = [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 2)); };

const std::function<void(RandomGenerator &, std::string &)> zeroToThree
    = [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 3)); };

extern std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> serverSettings;

extern std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mergeTreeTableSettings;

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> memoryTableSettings
    = {{"min_bytes_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"max_bytes_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"min_rows_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"max_rows_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"compress", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> setTableSettings = {{"persistent", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> joinTableSettings = {{"persistent", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> embeddedRocksDBTableSettings = {
    {"optimize_for_bulk_insert", trueOrFalse},
    {"bulk_insert_block_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mySQLTableSettings
    = {{"connection_pool_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 7)); }},
       {"connection_max_tries", [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 16)); }},
       {"connection_auto_close", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> fileTableSettings
    = {{"engine_file_allow_create_multiple_files", trueOrFalse},
       {"engine_file_empty_if_not_exists", trueOrFalse},
       {"engine_file_skip_empty_files", trueOrFalse},
       {"engine_file_truncate_on_insert", trueOrFalse},
       {"storage_file_read_method",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"read", "pread", "mmap"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> s3QueueTableSettings
    = {{"after_processing",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"", "keep", "delete"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"enable_logging_to_s3queue_log", trueOrFalse},
       {"processing_threads_num",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.RandomInt<uint32_t>(1, std::thread::hardware_concurrency())); }}};

extern std::map<TableEngineValues, std::map<std::string, std::function<void(RandomGenerator &, std::string &)>>> allTableSettings;

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mergeTreeColumnSettings
    = {{"min_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"max_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }}};

const std::map<TableEngineValues, std::map<std::string, std::function<void(RandomGenerator &, std::string &)>>> allColumnSettings
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
       {Merge, {}}};

void setRandomSetting(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    std::string & ret,
    SetValue * set);
void loadFuzzerSettings(const FuzzConfig & fc);

}
