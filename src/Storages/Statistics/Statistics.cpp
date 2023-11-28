#include <optional>
#include <numeric>

#include <DataTypes/DataTypeNullable.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/TDigestStatistic.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int ILLEGAL_STATISTIC;
}

void MergeTreeStatisticsFactory::registerCreator(StatisticType stat_type, Creator creator)
{
    if (!creators.emplace(stat_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistic creator type {} is not unique", stat_type);
}

void MergeTreeStatisticsFactory::registerValidator(StatisticType stat_type, Validator validator)
{
    if (!validators.emplace(stat_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeStatisticsFactory: the statistic validator type {} is not unique", stat_type);

}

StatisticPtr TDigestCreator(const StatisticDescription & stat)
{
    return StatisticPtr(new TDigestStatistic(stat));
}

void TDigestValidator(const StatisticDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTIC, "TDigest does not support type {}", data_type->getName());
}


MergeTreeStatisticsFactory::MergeTreeStatisticsFactory()
{
    registerCreator(TDigest, TDigestCreator);
    registerValidator(TDigest, TDigestValidator);
}

MergeTreeStatisticsFactory & MergeTreeStatisticsFactory::instance()
{
    static MergeTreeStatisticsFactory instance;
    return instance;
}

void MergeTreeStatisticsFactory::validate(const StatisticDescription & stat, DataTypePtr data_type) const
{
    auto it = validators.find(stat.type);
    if (it == validators.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Statistic type '{}'", stat.type);
    }
    it->second(stat, data_type);
}

StatisticPtr MergeTreeStatisticsFactory::get(const StatisticDescription & stat) const
{
    auto it = creators.find(stat.type);
    if (it == creators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Unknown Statistic type '{}'. Available types: tdigest", stat.type);
    }
    return std::make_shared<TDigestStatistic>(stat);
}

Statistics MergeTreeStatisticsFactory::getMany(const ColumnsDescription & columns) const
{
    Statistics result;
    for (const auto & col : columns)
        if (col.stat)
            result.push_back(get(*col.stat));
    return result;
}

}
