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

<<<<<<< HEAD
const auto trueOrFalse = [](RandomGenerator & rg) { return rg.nextBool() ? "1" : "0"; };

const auto zeroOneTwo = [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 2)); };

const auto zeroToThree = [](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); };

const auto probRange = [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); };

const auto highRange = [](RandomGenerator & rg)
=======
extern const std::unordered_set<String> blockSizes;

const auto trueOrFalse = [](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "1" : "0"; };

const auto zeroOneTwo = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 2)); };

const auto zeroToThree = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); };

const auto probRange = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 1.0)); };

const auto probRangeNoZero
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.001, 0.999)); };

const auto highRange = [](RandomGenerator & rg, FuzzConfig &)
>>>>>>> origin/master
{
    const auto val = rg.randomInt<uint32_t>(0, 25);
    return std::to_string(val == UINT32_C(0) ? UINT32_C(0) : (UINT32_C(1) << (val - UINT32_C(1))));
};

/// Like highRange but never produces 0 — for NonZeroUInt64 settings
const auto highRangeNonZero = [](RandomGenerator & rg, FuzzConfig &)
{
    const auto val = rg.randomInt<uint32_t>(1, 25);
    return std::to_string(UINT32_C(1) << (val - UINT32_C(1)));
};

const auto columnsRange
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10))); };

const auto rowsRange
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(8192))); };

/// Like rowsRange but never produces 0 — for NonZeroUInt64 settings
const auto rowsRangeNonZero
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, UINT32_C(8192))); };

const auto bytesRange = [](RandomGenerator & rg, FuzzConfig &)
{ return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); };

/// Like bytesRange but never produces 0 — for NonZeroUInt64 settings
const auto bytesRangeNonZero = [](RandomGenerator & rg, FuzzConfig &)
{ return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); };

const auto threadSetting = CHSetting(
    [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, std::thread::hardware_concurrency())); },
    {"0", "1", "2", std::to_string(std::thread::hardware_concurrency())},
    false);

const auto inline probRangeSetting = CHSetting(probRange, {"0", "0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999", "1.0"}, false);

const auto inline probRangeNoZeroSetting = CHSetting(probRangeNoZero, {"0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999"}, false);

const auto inline trueOrFalseSetting = CHSetting(trueOrFalse, {"0", "1"}, false);

const auto inline trueOrFalseSettingNoOracle = CHSetting(trueOrFalse, {}, false);

extern std::unordered_map<String, CHSetting> hotSettings;

extern std::unordered_map<String, CHSetting> serverSettings;

extern std::unordered_map<String, CHSetting> performanceSettings;

extern std::unordered_map<String, CHSetting> queryOracleSettings;

extern std::unordered_map<String, CHSetting> formatSettings;

extern std::unordered_map<String, CHSetting> allDatabaseSettings;

extern std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allTableSettings;

extern std::unordered_map<TableEngineValues, std::unordered_map<String, CHSetting>> allColumnSettings;

<<<<<<< HEAD
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
       {Distributed, {}},
       {Dictionary, {}},
       {GenerateRandom, {}}};

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
=======
extern std::unordered_map<String, CHSetting> backupSettings;
>>>>>>> origin/master

extern std::unordered_map<String, CHSetting> restoreSettings;

extern std::unordered_map<String, CHSetting> projectionSettings;

extern std::unordered_map<String, CHSetting> refreshSettings;

extern std::unique_ptr<SQLType> size_tp;

extern std::unique_ptr<SQLType> null_tp;

extern std::unique_ptr<SQLType> string_tp;

extern std::unique_ptr<SQLType> uint8_tp;

extern std::vector<SystemTable> systemTables;

extern std::unordered_map<DictionaryLayouts, std::unordered_map<String, CHSetting>> allDictionaryLayoutSettings;

String settingCombinations(RandomGenerator & rg, DB::Strings && choices);
String generateNextCodecString(RandomGenerator & rg);
String generateNextCodecStringForType(RandomGenerator & rg, const SQLType * tp);
String getNextIcebergTimestamp(RandomGenerator & rg, FuzzConfig & fc);
String getNextIcebergExpireTimestamp(RandomGenerator & rg, FuzzConfig & fc);
void loadFuzzerServerSettings(const FuzzConfig & fc);
void loadFuzzerTableSettings(const FuzzConfig & fc);
<<<<<<< HEAD
void loadSystemTables(const FuzzConfig & fc);
=======
void loadSystemTables(FuzzConfig & fc);
>>>>>>> origin/master

}
