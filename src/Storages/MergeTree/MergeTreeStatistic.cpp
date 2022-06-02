#include <algorithm>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <fmt/core.h>
#include <memory>
#include <numeric>
#include <Parsers/ASTExpressionList.h>
#include <Poco/Logger.h>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <Storages/MergeTree/MergeTreeStatisticGranuleStringHash.h>
#include <Storages/MergeTree/MergeTreeStatisticGranuleTDigest.h>
#include <Storages/MergeTree/MergeTreeStatisticTDigest.h>
#include <Storages/Statistics.h>
#include <string>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

String generateFileNameForStatistics(const String & name, const String & column)
{
    return fmt::format("{}_{}_{}.{}",
        PART_STATS_FILE_NAME,
        name,
        column,
        PART_STATS_FILE_EXT);
}

bool MergeTreeDistributionStatistics::empty() const
{
    for (const auto& [column, stat] : column_to_stats)
    {
        if (!stat->empty())
        {
            return false;
        }
    }
    return true;
}

void MergeTreeDistributionStatistics::merge(const std::shared_ptr<IDistributionStatistics> & other)
{
    if (!other)
        return;
    const auto merge_tree_stats = std::dynamic_pointer_cast<MergeTreeDistributionStatistics>(other);
    if (!merge_tree_stats)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    for (const auto & [column, stat] : merge_tree_stats->column_to_stats)
    {
        if (column_to_stats.contains(column))
        {
            column_to_stats.at(column)->merge(stat);
        }
        // Skip unknown columns.
        // This is valid because everything is merged into empty stats created from metadata.
        // Differences can be caused by alters.
    }
}

std::optional<double> MergeTreeDistributionStatistics::estimateProbability(const String& column, const Field& lower, const Field& upper) const
{
    if (!column_to_stats.contains(column))
    {
        return std::nullopt;
    }
    const auto & stat = column_to_stats.at(column);
    if (stat->empty())
    {
        return std::nullopt;
    }
    return stat->estimateProbability(lower, upper);
}

void MergeTreeDistributionStatistics::add(const String & column, const IDistributionStatisticPtr & stat)
{
    if (stat == nullptr)
    {
        column_to_stats.erase(column);
        return;
    }
    column_to_stats[column] = stat;
}

bool MergeTreeDistributionStatistics::has(const String & name) const
{
    return column_to_stats.contains(name);
}

Names MergeTreeDistributionStatistics::getStatisticsNames() const
{
    std::set<String> statistics;
    for (const auto & [column, stat] : column_to_stats)
    {
        statistics.insert(stat->name());
    }
    return Names(std::begin(statistics), std::end(statistics));
}

bool MergeTreeStatistics::empty() const
{
    return column_distributions->empty() && string_search->empty();
}

void MergeTreeStatistics::merge(const std::shared_ptr<IStatistics>& other)
{
    if (!other)
        return;
    const auto merge_tree_stats = std::dynamic_pointer_cast<MergeTreeStatistics>(other);
    if (!merge_tree_stats)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    column_distributions->merge(merge_tree_stats->column_distributions);
    string_search->merge(merge_tree_stats->string_search);
}

Names MergeTreeStatistics::getStatisticsNames() const
{
    Names result;
    const auto numeric_distributions = column_distributions->getStatisticsNames();
    result.insert(std::end(result), std::begin(numeric_distributions), std::end(numeric_distributions));
    const auto string_searches = string_search->getStatisticsNames();
    result.insert(std::end(result), std::begin(string_searches), std::end(string_searches));
    return result;
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
void MergeTreeStatistics::serializeBinary(const String & name, WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->serializeBinary(2, ostr);
    size_serialization->serializeBinary(static_cast<size_t>(StatisticType::NUMERIC_COLUMN_DISRIBUTION), ostr);
    column_distributions->serializeBinary(name, ostr);
    size_serialization->serializeBinary(static_cast<size_t>(StatisticType::STRING_SEARCH), ostr);
    string_search->serializeBinary(name, ostr);
}

void MergeTreeStatistics::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field field;
    size_serialization->deserializeBinary(field, istr);
    const auto stats_count = field.get<size_t>();
    if (stats_count > static_cast<size_t>(StatisticType::LAST))
        throw Exception("Deserialization error: too many stats in file", ErrorCodes::LOGICAL_ERROR);
    for (size_t stat_index = 0; stat_index < stats_count; ++stat_index)
    {
        size_serialization->deserializeBinary(field, istr);
        switch (field.get<size_t>())
        {
        case static_cast<size_t>(StatisticType::NUMERIC_COLUMN_DISRIBUTION):
            column_distributions->deserializeBinary(istr);
            break;
        case static_cast<size_t>(StatisticType::STRING_SEARCH):
            string_search->deserializeBinary(istr);
            break;
        default:
            throw Exception("Unknown statistic type", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void MergeTreeStatistics::setDistributionStatistics(IDistributionStatisticsPtr && stat)
{
    auto merge_tree_stat = std::dynamic_pointer_cast<MergeTreeDistributionStatistics>(stat);
    if (!merge_tree_stat)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    column_distributions = std::move(merge_tree_stat);
}

void MergeTreeStatistics::setStringSearchStatistics(IStringSearchStatisticsPtr && stat)
{
    auto merge_tree_stat = std::dynamic_pointer_cast<MergeTreeStringSearchStatistics>(stat);
    if (!merge_tree_stat)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    string_search = std::move(merge_tree_stat);
}

size_t MergeTreeStatistics::getSizeInMemory() const
{
    return column_distributions->getSizeInMemory() + string_search->getSizeInMemory();
}

size_t MergeTreeDistributionStatistics::getSizeInMemory() const
{
    size_t sum = 0;
    for (const auto & [_, statistic] : column_to_stats)
        sum += statistic->getSizeInMemory();
    return sum;
}

size_t MergeTreeDistributionStatistics::getSizeInMemoryByName(const String& name) const
{
    size_t sum = 0;
    for (const auto & [_, statistic] : column_to_stats)
    {
        if (statistic->name() == name)
            sum += statistic->getSizeInMemory();
    }
    return sum;
}

void MergeTreeDistributionStatistics::serializeBinary(const String & name, WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    const auto & str_type = DataTypePtr(std::make_shared<DataTypeString>());
    auto str_serialization = str_type->getDefaultSerialization();

    const size_t count = std::count_if(
        std::begin(column_to_stats), std::end(column_to_stats),
        [&name](const auto & elem) { return name == elem.second->name(); });
    size_serialization->serializeBinary(count, ostr);
    for (const auto & [column, statistic] : column_to_stats)
    {
        if (statistic->name() == name)
        {
            str_serialization->serializeBinary(column, ostr);
            statistic->serializeBinary(ostr);
        }
    }
}

void MergeTreeDistributionStatistics::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    const auto & str_type = DataTypePtr(std::make_shared<DataTypeString>());
    auto str_serialization = str_type->getDefaultSerialization();

    Field field;
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
            const auto data_type = field.get<size_t>();
            UNUSED(data_type);
            size_serialization->deserializeBinary(field, istr);
            const auto data_count = field.get<size_t>();
            istr.ignore(data_count);
        }
        else if (!it->second->validateTypeBinary(istr))
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

bool MergeTreeStringSearchStatistics::empty() const
{
    for (const auto& [column, stat] : column_to_stats)
    {
        if (!stat->empty())
            return false;
    }
    return true;
}

bool MergeTreeStringSearchStatistics::has(const String & name) const
{
    return column_to_stats.contains(name);
}

void MergeTreeStringSearchStatistics::merge(const std::shared_ptr<IStringSearchStatistics> & other)
{
    if (!other)
        return;
    const auto merge_tree_stats = std::dynamic_pointer_cast<MergeTreeStringSearchStatistics>(other);
    if (!merge_tree_stats)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatistics can not be merged with other statistics");

    for (const auto & [column, stat] : merge_tree_stats->column_to_stats)
    {
        if (column_to_stats.contains(column))
        {
            column_to_stats.at(column)->merge(stat);
        }
    }
}

Names MergeTreeStringSearchStatistics::getStatisticsNames() const
{
    std::set<String> statistics;
    for (const auto & [column, stat] : column_to_stats)
    {
        statistics.insert(stat->name());
    }
    return Names(std::begin(statistics), std::end(statistics));
}

void MergeTreeStringSearchStatistics::serializeBinary(const String & name, WriteBuffer & ostr) const
{
    // todo: get rid of copy-paste
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    const auto & str_type = DataTypePtr(std::make_shared<DataTypeString>());
    auto str_serialization = str_type->getDefaultSerialization();

    const size_t count = std::count_if(
        std::begin(column_to_stats), std::end(column_to_stats),
        [&name](const auto & elem) { return name == elem.second->name(); });
    size_serialization->serializeBinary(count, ostr);
    for (const auto & [column, statistic] : column_to_stats)
    {
        if (statistic->name() == name)
        {
            str_serialization->serializeBinary(column, ostr);
            statistic->serializeBinary(ostr);
        }
    }
}

void MergeTreeStringSearchStatistics::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    const auto & str_type = DataTypePtr(std::make_shared<DataTypeString>());
    auto str_serialization = str_type->getDefaultSerialization();

    Field field;
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
            const auto data_type = field.get<size_t>();
            UNUSED(data_type);
            size_serialization->deserializeBinary(field, istr);
            const auto data_count = field.get<size_t>();
            istr.ignore(data_count);
        }
        else if (!it->second->validateTypeBinary(istr))
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

std::optional<double> MergeTreeStringSearchStatistics::estimateStringProbability(const String & column, const String& needle) const
{
    if (!column_to_stats.contains(column))
    {
        Poco::Logger::get("MergeTreeStringSearchStatistics").information("NOT FOUND");
        return std::nullopt;
    }
    const auto & stat = column_to_stats.at(column);
    if (stat->empty())
    {
        Poco::Logger::get("MergeTreeStringSearchStatistics").information("EMPTY");
        return std::nullopt;
    }
    return stat->estimateStringProbability(needle);
}

std::optional<double> MergeTreeStringSearchStatistics::estimateSubstringsProbability(const String & column, const Strings& needles) const
{
    if (!column_to_stats.contains(column))
    {
        return std::nullopt;
    }
    const auto & stat = column_to_stats.at(column);
    if (stat->empty())
    {
        return std::nullopt;
    }
    return stat->estimateSubstringsProbability(needles);
}

void MergeTreeStringSearchStatistics::add(const String & column, const IStringSearchStatisticPtr & stat)
{
    // TODO: MergeTreeStatisticsBase to get rid of copy paste
    if (stat == nullptr)
    {
        column_to_stats.erase(column);
        return;
    }
    column_to_stats[column] = stat;
}

size_t MergeTreeStringSearchStatistics::getSizeInMemory() const
{
    size_t sum = 0;
    for (const auto & [_, statistic] : column_to_stats)
        sum += statistic->getSizeInMemory();
    return sum;
}

size_t MergeTreeStringSearchStatistics::getSizeInMemoryByName(const String& name) const
{
    size_t sum = 0;
    for (const auto & [_, statistic] : column_to_stats)
    {
        if (statistic->name() == name)
            sum += statistic->getSizeInMemory();
    }
    return sum;
}

IConstDistributionStatisticsPtr MergeTreeStatistics::getDistributionStatistics() const
{
    return column_distributions;
}

IConstStringSearchStatisticsPtr MergeTreeStatistics::getStringSearchStatistics() const
{
    return string_search;
}

void MergeTreeStatisticFactory::registerCreators(
    const std::string & stat_type,
    StatisticsCreator creator,
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
    const std::vector<StatisticDescription> & statistics,
    const ColumnsDescription & columns) const
{
    std::unordered_map<String, String> used_columns;
    for (const auto & stat_description : statistics)
    {
        for (const auto & column : stat_description.column_names)
        {
            if (!used_columns.emplace(column, stat_description.name).second)
                throw Exception("Column `" + column + "` was already used by statistic `" + used_columns.at(column) + "`.", ErrorCodes::INCORRECT_QUERY);

            for (const auto & stat : getSplittedStatistics(stat_description, columns.get(column)))
            {
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

IStatisticPtr MergeTreeStatisticFactory::getStatistic(
    const StatisticDescription & statistic,
    const ColumnDescription & column) const
{
    auto it = creators.find(statistic.type);
    if (it == creators.end())
        throw Exception(
                "Unknown Stat type '" + statistic.type + "'. Available statistic types: " +
                std::accumulate(creators.cbegin(), creators.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);

    return {it->second(statistic, column)};
}

MergeTreeStatisticsPtr MergeTreeStatisticFactory::get(
    const std::vector<StatisticDescription> & statistics,
    const ColumnsDescription & columns) const
{
    auto column_distribution_stats = std::make_shared<MergeTreeDistributionStatistics>();
    auto string_search_stats = std::make_shared<MergeTreeStringSearchStatistics>();
    for (const auto & statistics_description : statistics)
    {
        for (const auto & column : statistics_description.column_names)
        {
            for (const auto & statistic_description : getSplittedStatistics(statistics_description, columns.get(column)))
            {
                auto statistic = getStatistic(statistic_description, columns.get(column));
                switch (statistic->statisticType())
                {
                    case StatisticType::NUMERIC_COLUMN_DISRIBUTION:
                        column_distribution_stats->add(column, std::dynamic_pointer_cast<IDistributionStatistic>(statistic));
                        break;
                    case StatisticType::STRING_SEARCH:
                        string_search_stats->add(column, std::dynamic_pointer_cast<IStringSearchStatistic>(statistic));
                        break;
                    default:
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistic collector type.");
                }
            }
        }
    }

    auto result = std::make_shared<MergeTreeStatistics>();
    result->setDistributionStatistics(std::move(column_distribution_stats));
    result->setStringSearchStatistics(std::move(string_search_stats));
    return result;
}

IMergeTreeStatisticCollectorPtr MergeTreeStatisticFactory::getStatisticCollector(
    const StatisticDescription & statistic, const ColumnDescription & column) const
{
    auto it = collectors.find(statistic.type);
    if (it == collectors.end())
        throw Exception(
                "Unknown Stat type '" + statistic.type + "'. Available statistic types: " +
                std::accumulate(collectors.cbegin(), collectors.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);

    return {it->second(statistic, column)};
}

IMergeTreeStatisticCollectorPtrs MergeTreeStatisticFactory::getStatisticCollectors(
    const std::vector<StatisticDescription> & statistics,
    const ColumnsDescription & columns,
    const NamesAndTypesList & columns_for_collection) const
{
    NameSet columns_names_for_collection;
    for (const auto & column_for_collection : columns_for_collection)
    {
        columns_names_for_collection.insert(column_for_collection.name);
    }

    IMergeTreeStatisticCollectorPtrs result;
    for (const auto & statictic_description : statistics)
    {
        for (const auto & column : statictic_description.column_names)
        {
            if (columns_names_for_collection.contains(column))
            {
                for (const auto & statistic : getSplittedStatistics(statictic_description, columns.get(column)))
                {
                    result.emplace_back(getStatisticCollector(statistic, columns.get(column)));
                }
            }
        }
    }
    return result;
}

std::vector<StatisticDescription> MergeTreeStatisticFactory::getSplittedStatistics(
    const StatisticDescription & statistic, const ColumnDescription & column)
{
    if (statistic.type != "auto")
    {
        return {statistic};
    }
    else
    {
        /// let's select stats for column by ourselves
        std::vector<StatisticDescription> result;
        if (column.type->isValueRepresentedByNumber() && !column.type->isNullable())
        {
            result.emplace_back();
            result.back().column_names = {column.name};
            result.back().data_types = {column.type};
            result.back().definition_ast = statistic.definition_ast->clone();
            result.back().name = statistic.name;
            result.back().type = "granule_tdigest";
        }
        else
        {
            throw Exception("Unsupported statistic '" + statistic.type + "' for column '" + column.name + "'", ErrorCodes::INCORRECT_QUERY);
        }
        return result;
    }
}

MergeTreeStatisticFactory::MergeTreeStatisticFactory()
{
    registerCreators(
        "tdigest",
        creatorColumnDistributionStatisticTDigest,
        creatorColumnDistributionStatisticCollectorTDigest,
        validatorColumnDistributionStatisticTDigest);
    registerCreators(
        "granule_tdigest",
        creatorGranuleDistributionStatisticTDigest,
        creatorGranuleDistributionStatisticCollectorTDigest,
        validatorGranuleDistributionStatisticTDigest);
    registerCreators(
        "granule_string_hash",
        creatorGranuleStringHashStatistic,
        creatorGranuleStringHashStatisticCollector,
        validatorGranuleStringHashStatistic);
}

MergeTreeStatisticFactory & MergeTreeStatisticFactory::instance()
{
    static MergeTreeStatisticFactory instance;
    return instance;
}

}
