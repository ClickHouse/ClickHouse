#pragma once

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <cstdint>
#include <functional>
#include <thread>

namespace BuzzHouse
{

struct CHSetting
{
public:
    const std::function<void(RandomGenerator &, std::string &)> random_func;
    const std::set<std::string> oracle_values;
    const bool changes_behavior;

    CHSetting(const std::function<void(RandomGenerator &, std::string &)> & rf, const std::set<std::string> & ov, const bool cb)
        : random_func(rf), oracle_values(ov), changes_behavior(cb)
    {
    }

    constexpr CHSetting(const CHSetting & rhs) = default;
    constexpr CHSetting(CHSetting && rhs) = default;
};

const std::function<void(RandomGenerator &, std::string &)> trueOrFalse
    = [](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "1" : "0"; };

const std::function<void(RandomGenerator &, std::string &)> zeroOneTwo
    = [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.randomInt<uint32_t>(0, 2)); };

const std::function<void(RandomGenerator &, std::string &)> zeroToThree
    = [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.randomInt<uint32_t>(0, 3)); };

extern std::map<std::string, CHSetting> serverSettings;

const std::map<std::string, CHSetting> memoryTableSettings
    = {{"min_bytes_to_keep",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"max_bytes_to_keep",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"min_rows_to_keep",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"max_rows_to_keep",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
       {"compress", CHSetting(trueOrFalse, {}, false)}};

const std::map<std::string, CHSetting> setTableSettings = {{"persistent", CHSetting(trueOrFalse, {}, false)}};

const std::map<std::string, CHSetting> joinTableSettings = {{"persistent", CHSetting(trueOrFalse, {}, false)}};

const std::map<std::string, CHSetting> embeddedRocksDBTableSettings = {
    {"optimize_for_bulk_insert", CHSetting(trueOrFalse, {}, false)},
    {"bulk_insert_block_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
};

const std::map<std::string, CHSetting> mySQLTableSettings
    = {{"connection_pool_size",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 7)); }, {}, false)},
       {"connection_max_tries",
        CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.randomInt<uint32_t>(1, 16)); }, {}, false)},
       {"connection_auto_close", CHSetting(trueOrFalse, {}, false)}};

const std::map<std::string, CHSetting> fileTableSettings
    = {{"engine_file_allow_create_multiple_files", CHSetting(trueOrFalse, {}, false)},
       {"engine_file_empty_if_not_exists", CHSetting(trueOrFalse, {}, false)},
       {"engine_file_skip_empty_files", CHSetting(trueOrFalse, {}, false)},
       {"engine_file_truncate_on_insert", CHSetting(trueOrFalse, {}, false)},
       {"storage_file_read_method",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret)
            {
                const std::vector<std::string> & choices = {"read", "pread", "mmap"};
                ret += "'";
                ret += rg.pickRandomlyFromVector(choices);
                ret += "'";
            },
            {},
            false)}};

const std::map<std::string, CHSetting> s3QueueTableSettings
    = {{"after_processing",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret)
            {
                const std::vector<std::string> & choices = {"", "keep", "delete"};
                ret += "'";
                ret += rg.pickRandomlyFromVector(choices);
                ret += "'";
            },
            {},
            false)},
       {"enable_logging_to_s3queue_log", CHSetting(trueOrFalse, {}, false)},
       {"processing_threads_num",
        CHSetting(
            [](RandomGenerator & rg, std::string & ret)
            { ret += std::to_string(rg.randomInt<uint32_t>(1, std::thread::hardware_concurrency())); },
            {},
            false)}};

extern std::map<TableEngineValues, std::map<std::string, CHSetting>> allTableSettings;

const std::map<std::string, CHSetting> mergeTreeColumnSettings = {
    {"min_compress_block_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)},
    {"max_compress_block_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {}, false)}};

const std::map<TableEngineValues, std::map<std::string, CHSetting>> allColumnSettings
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

void loadFuzzerServerSettings(const FuzzConfig & fc);
void loadFuzzerTableSettings(const FuzzConfig & fc);

}
