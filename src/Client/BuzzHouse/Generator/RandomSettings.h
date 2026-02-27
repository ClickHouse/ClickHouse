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

extern const std::unordered_set<String> blockSizes;

const auto trueOrFalse = [](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "1" : "0"; };

const auto zeroOneTwo = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 2)); };

const auto zeroToThree = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); };

const auto probRange = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 1.0)); };

const auto probRangeNoZero
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.001, 0.999)); };

const auto highRange = [](RandomGenerator & rg, FuzzConfig &)
{
    const auto val = rg.randomInt<uint32_t>(0, 25);
    return std::to_string(val == UINT32_C(0) ? UINT32_C(0) : (UINT32_C(1) << (val - UINT32_C(1))));
};

const auto columnsRange
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10))); };

const auto rowsRange
    = [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(8192))); };

const auto bytesRange = [](RandomGenerator & rg, FuzzConfig &)
{ return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); };

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

extern std::unordered_map<String, CHSetting> backupSettings;

extern std::unordered_map<String, CHSetting> restoreSettings;

extern std::unique_ptr<SQLType> size_tp;

extern std::unique_ptr<SQLType> null_tp;

extern std::unique_ptr<SQLType> string_tp;

extern std::vector<SystemTable> systemTables;

extern std::unordered_map<DictionaryLayouts, std::unordered_map<String, CHSetting>> allDictionaryLayoutSettings;

String generateNextCodecString(RandomGenerator & rg);
void loadFuzzerServerSettings(const FuzzConfig & fc);
void loadFuzzerTableSettings(const FuzzConfig & fc);
void loadSystemTables(FuzzConfig & fc);

}
