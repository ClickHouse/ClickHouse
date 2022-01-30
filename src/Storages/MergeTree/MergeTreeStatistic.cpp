#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <algorithm>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include "base/types.h"
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <fmt/core.h>
#include <memory>
#include <numeric>
#include <Parsers/ASTExpressionList.h>
#include <Poco/Logger.h>
#include <Storages/MergeTree/MergeTreeStatisticTDigest.h>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

String generateFileNameForStatistics() {
    return fmt::format("{}_{}.{}",
        PART_STATS_FILE_NAME,
        std::uniform_int_distribution<UInt64>()(thread_local_rng),
        PART_STATS_FILE_EXT);
}

bool MergeTreeColumnDistributionStatistics::empty() const
{
    return column_to_stats.empty();
}

void MergeTreeColumnDistributionStatistics::merge(const std::shared_ptr<IColumnDistributionStatistics> & other)
{
    const auto merge_tree_stats = std::dynamic_pointer_cast<MergeTreeColumnDistributionStatistics>(other);
    if (!merge_tree_stats)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");
    
    for (const auto & [column, stat] : merge_tree_stats->column_to_stats)
    {
        Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
            "MERGE" + column + " ");
        if (column_to_stats.contains(column))
        {
            column_to_stats.at(column)->merge(stat);
            Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
            "MERGEd" + column + " ");
        }
        else
        {
            column_to_stats[column]->merge(stat);
            Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
            "created" + column + " ");
        }
    }
}

std::optional<double> MergeTreeColumnDistributionStatistics::estimateProbability(const String& column, const Field& lower, const Field& upper) const
{
    Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("estimateProbability");
    if (!column_to_stats.contains(column)) {
        Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("no column " + column);
        return std::nullopt;
    }
    Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("column " + column);
    const auto & stat = column_to_stats.at(column);
    if (stat->empty()) {
        return 1;
    }
    return stat->estimateProbability(lower, upper);
}

void MergeTreeColumnDistributionStatistics::add(const String & name, const IColumnDistributionStatisticPtr & stat) {
    if (stat == nullptr)
    {
        column_to_stats.erase(name);
        return;
    }
    column_to_stats[name] = stat;
}

void MergeTreeColumnDistributionStatistics::remove(const String & name) {
    column_to_stats.erase(name);
}

bool MergeTreeStatistics::empty() const
{
    return column_distributions->empty();
}

void MergeTreeStatistics::merge(const std::shared_ptr<IStatistics>& other)
{
    const auto merge_tree_stats = std::dynamic_pointer_cast<MergeTreeStatistics>(other);
    if (!merge_tree_stats)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    Poco::Logger::get("MergeTreeStatistics").information(
            "MERGE start ");
    if (!other)
        return;
    Poco::Logger::get("MergeTreeStatistics").information(
            "MERGE start column_distributions");
    column_distributions->merge(merge_tree_stats->column_distributions);
}

// Serialization:
// <Count:u64>
// <TYPE&VERSION:u64><Count:u64>
//      <Column/Name:string><DataSizeBytes:u64><data:...>
//      <Column/Name:string><DataSizeBytes:u64><data:...>
//      ...
// <TYPE&VERSION:u64><Count:u64>
//      <Column/Name:string><DataSizeBytes:u64><data:...>
//      <Column/Name:string><DataSizeBytes:u64><data:...>
//      ...
// ...
void MergeTreeStatistics::serializeBinary(WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->serializeBinary(1, ostr);
    // TODO: support versions and multiple distrs
    column_distributions->serializeBinary(ostr);
}

void MergeTreeStatistics::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field field;
    size_serialization->deserializeBinary(field, istr);
    const auto stats_count = field.get<size_t>();
    Poco::Logger::get("MergeTreeStatistics").information("COUNT " + std::to_string(stats_count));
    if (stats_count != 1)
        throw Exception("Deserialization error: stats count in file not equal to 1", ErrorCodes::LOGICAL_ERROR);
    column_distributions->deserializeBinary(istr);
}

void MergeTreeStatistics::setColumnDistributionStatistics(IColumnDistributionStatisticsPtr && stat)
{
    auto merge_tree_stat = std::dynamic_pointer_cast<MergeTreeColumnDistributionStatistics>(stat);
    if (!merge_tree_stat)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    column_distributions = std::move(merge_tree_stat);
}

void MergeTreeColumnDistributionStatistics::serializeBinary(WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    const auto & str_type = DataTypePtr(std::make_shared<DataTypeString>());
    auto str_serialization = str_type->getDefaultSerialization();

    size_serialization->serializeBinary(static_cast<size_t>(StatisticType::COLUMN_DISRIBUTION), ostr);
    size_serialization->serializeBinary(column_to_stats.size(), ostr);
    for (const auto & [column, stat] : column_to_stats)
    {
        str_serialization->serializeBinary(column, ostr);
        stat->serializeBinary(ostr);
    }
}

void MergeTreeColumnDistributionStatistics::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    const auto & str_type = DataTypePtr(std::make_shared<DataTypeString>());
    auto str_serialization = str_type->getDefaultSerialization();

    Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("START");
    Field field;
    size_serialization->deserializeBinary(field, istr);
    if (field.get<size_t>() != static_cast<size_t>(StatisticType::COLUMN_DISRIBUTION))
        throw Exception("Unknown statistic type", ErrorCodes::LOGICAL_ERROR);
    Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
        "TYPE {}" + std::to_string(static_cast<size_t>(StatisticType::COLUMN_DISRIBUTION)));

    size_serialization->deserializeBinary(field, istr);
    const auto stats_count = field.get<size_t>();
    Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
        "COLS {}" + std::to_string(column_to_stats.size()));

    for (size_t index = 0; index < stats_count; ++index)
    {
        Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
            "RUN {}" + std::to_string(index));
        str_serialization->deserializeBinary(field, istr);
        const auto column = field.get<String>();
        Poco::Logger::get("MergeTreeColumnDistributionStatistics").information(
            "CLNM {}" + column);
        auto it = column_to_stats.find(column);
        if (it == std::end(column_to_stats))
        {
            Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("UNKNOWN CLMN");
            size_serialization->deserializeBinary(field, istr);
            const auto data_count = field.get<size_t>();
            Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("SKIP " + std::to_string(data_count));
            istr.ignore(data_count);
        }
        else
        {
            Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("DESERIALIZE CLMN");
            it->second->deserializeBinary(istr);
        }
    }
    Poco::Logger::get("MergeTreeColumnDistributionStatistics").information("FINISH");
}

IConstColumnDistributionStatisticsPtr MergeTreeStatistics::getColumnDistributionStatistics() const
{
    return column_distributions;
}

void MergeTreeStatisticFactory::registerCreators(
    const std::string & stat_type,
    StatCreator creator,
    CollectorCreator collector,
    Validator validator)
{
    if (!creators.emplace(stat_type, std::move(creator)).second)
        throw Exception("MergeTreeStatisticFactory: the statistic creator type '" + stat_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
    if (!collectors.emplace(stat_type, std::move(collector)).second)
        throw Exception("MergeTreeStatisticFactory: the statistic collector creator type '" + stat_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
    if (!validators.emplace(stat_type, std::move(validator)).second)
        throw Exception("MergeTreeStatisticFactory: the statistic validator type '" + stat_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

void MergeTreeStatisticFactory::validate(
    const std::vector<StatisticDescription> & stats,
    const ColumnsDescription & columns) const
{
    for (const auto & stat_description : stats) {
        for (const auto & column : stat_description.column_names) {
            for (const auto & stat : getSplittedStatistics(stat_description, columns.get(column))) {
                auto it = validators.find(stat.type);
                if (it == validators.end())
                    throw Exception(
                            "Unknown Stat type '" + stat.type + "'. Available statistic types: " +
                            std::accumulate(validators.cbegin(), validators.cend(), std::string{},
                                    [] (auto && left, const auto & right) -> std::string
                                    {
                                        if (left.empty())
                                            return right.first;
                                        else
                                            return left + ", " + right.first;
                                    }),
                            ErrorCodes::INCORRECT_QUERY);
                it->second(stat, columns.get(column));
            }
        }
    }
}

IColumnDistributionStatisticPtr MergeTreeStatisticFactory::getColumnDistributionStatistic(
    const StatisticDescription & stat,
    const ColumnDescription & column) const
{
    auto it = creators.find(stat.type);
    if (it == creators.end())
        throw Exception(
                "Unknown Stat type '" + stat.type + "'. Available statistic types: " +
                std::accumulate(creators.cbegin(), creators.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);

    return {it->second(stat, column)};
}

MergeTreeStatisticsPtr MergeTreeStatisticFactory::get(
    const std::vector<StatisticDescription> & stats,
    const ColumnsDescription & columns) const
{
    auto column_distribution_stats = std::make_shared<MergeTreeColumnDistributionStatistics>();
    Poco::Logger::get("MergeTreeStatisticFactory").information("STAT CREATE NEW");
    for (const auto & stat_description : stats) {
        // move to params
        for (const auto & column : stat_description.column_names) {
            for (const auto & stat : getSplittedStatistics(stat_description, columns.get(column))) {
                column_distribution_stats->add(column, getColumnDistributionStatistic(stat, columns.get(column)));
                Poco::Logger::get("MergeTreeStatisticFactory").information("STAT CREATE name = " + stat.name + " column = " + column);
            }
        }
    }

    auto result = std::make_shared<MergeTreeStatistics>();
    result->setColumnDistributionStatistics(std::move(column_distribution_stats));
    return result;
}

IMergeTreeColumnDistributionStatisticCollectorPtr MergeTreeStatisticFactory::getColumnDistributionStatisticCollector(
    const StatisticDescription & stat, const ColumnDescription & column) const
{
    auto it = collectors.find(stat.type);
    if (it == collectors.end())
        throw Exception(
                "Unknown Stat type '" + stat.type + "'. Available statistic types: " +
                std::accumulate(collectors.cbegin(), collectors.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);

    return {it->second(stat, column)};
}

IMergeTreeColumnDistributionStatisticCollectorPtrs MergeTreeStatisticFactory::getColumnDistributionStatisticCollectors(
    const std::vector<StatisticDescription> & stats,
    const ColumnsDescription & columns) const
{
    IMergeTreeColumnDistributionStatisticCollectorPtrs result;
    for (const auto & stat_description : stats) {
        for (const auto & column : stat_description.column_names) {
            for (const auto & stat : getSplittedStatistics(stat_description, columns.get(column))) {
                result.emplace_back(getColumnDistributionStatisticCollector(stat, columns.get(column)));
            }
        }
    }
    return result;
}

std::vector<StatisticDescription> MergeTreeStatisticFactory::getSplittedStatistics(
    const StatisticDescription & stat, const ColumnDescription & column) const
{
    if (stat.type != "auto") {
        return {stat};
    } else {
        /// let's select stats for column by ourselfs
        std::vector<StatisticDescription> result;
        if (column.type->isValueRepresentedByNumber() && !column.type->isNullable()) {
            result.emplace_back();
            result.back().column_names = {column.name};
            result.back().data_types = {column.type};
            result.back().definition_ast = nullptr;
            result.back().expression_list_ast = nullptr;
            result.back().name = stat.name;
            result.back().type = "tdigest";
        }
        return result;
    }
}

MergeTreeStatisticFactory::MergeTreeStatisticFactory() {
    registerCreators(
        "tdigest",
        creatorColumnDistributionStatisticTDigest,
        creatorColumnDistributionStatisticCollectorTDigest,
        validatorColumnDistributionStatisticTDigest);
}

MergeTreeStatisticFactory & MergeTreeStatisticFactory::instance()
{
    static MergeTreeStatisticFactory instance;
    return instance;
}

}
