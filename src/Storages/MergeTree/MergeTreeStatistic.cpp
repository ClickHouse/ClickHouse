#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include "Common/Exception.h"
#include "base/defines.h"
#include <algorithm>
#include <memory>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

bool MergeTreeColumnDistributionStatistics::empty() const
{
    return column_to_stats.empty();
}

double MergeTreeColumnDistributionStatistics::estimateProbability(const String& column, const Field& lower, const Field& upper) const
{
    return column_to_stats.at(column)->estimateProbability(lower, upper);
}

void MergeTreeColumnDistributionStatistics::add(const String & name, const IMergeTreeColumnDistributionStatisticPtr & stat) {
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
    return column_distributions.empty();
}

void MergeTreeStatistics::merge(const std::shared_ptr<MergeTreeStatistics>& /*other*/)
{
    //if (!other)
    //    return;
    // Do nothing at current time
}

// Serialization:
// <Count:u64>
// <TYPE:u64><Count:u64><Column/Name:string><DataSizeBytes:u64><data:...><Column/Name:string><DataSizeBytes:u64><data:...>...
// <TYPE:u64><Count:u64><Column/Name:string><DataSizeBytes:u64><data:...><Column/Name:string><DataSizeBytes:u64><data:...>...
// ...
void MergeTreeStatistics::serializeBinary(WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->serializeBinary(1, ostr);
    column_distributions.serializeBinary(ostr);
}

void MergeTreeStatistics::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field field;
    size_serialization->deserializeBinary(field, istr);
    const auto stats_count = field.get<size_t>();
    if (stats_count != 1)
        throw Exception("Deserialization error: stats count in file not equal to 1", ErrorCodes::LOGICAL_ERROR);

    column_distributions.deserializeBinary(istr);
}

void MergeTreeStatistics::setColumnDistributionStatistics(MergeTreeColumnDistributionStatistics && stat)
{
    column_distributions = std::move(stat);
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

    Field field;
    size_serialization->deserializeBinary(field, istr);
    if (field.get<size_t>() != static_cast<size_t>(StatisticType::COLUMN_DISRIBUTION))
        throw Exception("Unknown statistic type", ErrorCodes::LOGICAL_ERROR);

    size_serialization->deserializeBinary(field, istr);
    const auto stats_count = field.get<size_t>();

    for (size_t index = 0; index < stats_count; ++index)
    {
        str_serialization->deserializeBinary(field, istr);
        const auto column = field.get<String>();
        auto it = column_to_stats.find(column);
        if (it == std::end(column_to_stats))
        {
            size_serialization->deserializeBinary(field, istr);
            const auto data_count = field.get<size_t>();
            istr.ignore(data_count);
        }
        else
        {
            it->second->deserializeBinary(istr);
        }
    }
}

const MergeTreeColumnDistributionStatistics & MergeTreeStatistics::getColumnDistributionStatistics() const
{
    return column_distributions;
}

void MergeTreeStatisticFactory::registerCreators(
    const std::string & stat_type, StatCreator creator, CollectorCreator collector)
{
    if (!creators.emplace(stat_type, std::move(creator)).second)
        throw Exception("MergeTreeStatisticFactory: the statistic creator name '" + stat_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
    if (!collectors.emplace(stat_type, std::move(collector)).second)
        throw Exception("MergeTreeStatisticFactory: the statistic collector creator name '" + stat_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

IMergeTreeColumnDistributionStatisticPtr MergeTreeStatisticFactory::getColumnDistributionStatistic(
    const StatisticDescription & stat) const
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

    return it->second(stat);
}

MergeTreeStatisticsPtr MergeTreeStatisticFactory::get(
    const std::vector<StatisticDescription> & stats) const
{
    MergeTreeColumnDistributionStatistics column_distribution_stats;
    for (const auto & stat : stats)
        column_distribution_stats.add(stat.name, getColumnDistributionStatistic(stat));

    auto result = std::make_shared<MergeTreeStatistics>();
    result->setColumnDistributionStatistics(std::move(column_distribution_stats));
    return result;
}

IMergeTreeColumnDistributionStatisticCollectorPtr MergeTreeStatisticFactory::getColumnDistributionStatisticCollector(
    const StatisticDescription & stat) const
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

    return it->second(stat);
}

IMergeTreeColumnDistributionStatisticCollectors MergeTreeStatisticFactory::getColumnDistributionStatisticCollectors(
    const std::vector<StatisticDescription> & stats) const
{
    IMergeTreeColumnDistributionStatisticCollectors result;
    for (const auto & stat : stats)
        result.emplace_back(getColumnDistributionStatisticCollector(stat));
    return result;
}

MergeTreeStatisticFactory & MergeTreeStatisticFactory::instance()
{
    static MergeTreeStatisticFactory instance;
    return instance;
}

}
