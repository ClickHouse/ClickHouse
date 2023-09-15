#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

Statistics Statistics::unknown(const Names & column_names)
{
    Statistics statistics;
    statistics.setOutputRowSize(1.0);

    for (const auto & column_name : column_names)
    {
        statistics.addColumnStatistics(column_name, ColumnStatistics::unknown());
    }
    return statistics;
}

Statistics Statistics::clone() const
{
    Statistics statistics;
    statistics.setOutputRowSize(output_row_size);

    for (const auto & column_stats : columns_stats_map)
    {
        statistics.addColumnStatistics(column_stats.first, column_stats.second->clone());
    }

    return statistics;
}


void Statistics::setOutputRowSize(Float64 row_size)
{
    output_row_size = std::max(1.0, row_size);
}

Float64 Statistics::getOutputRowSize() const
{
    return output_row_size;
}

void Statistics::addColumnStatistics(const String & column_name, ColumnStatisticsPtr column_stats)
{
    columns_stats_map.insert({column_name, column_stats});
}

void Statistics::removeColumnStatistics(const String & column_name)
{
    columns_stats_map.erase(column_name);
}

ColumnStatisticsPtr Statistics::getColumnStatistics(const String & column_name) const
{
    if (columns_stats_map.contains(column_name))
        return columns_stats_map.at(column_name);
    return {};
}

const ColumnStatisticsMap & Statistics::getColumnStatisticsMap() const
{
    return columns_stats_map;
}

bool Statistics::hasUnknownColumn() const
{
    for (auto column_stats : columns_stats_map)
    {
        if (column_stats.second->isUnKnown())
            return true;
    }
    return false;
}

void Statistics::adjustStatistics()
{
    for (auto column_stats_entry : columns_stats_map)
    {
        auto & column_stats = column_stats_entry.second;
        if (!column_stats->isUnKnown() && column_stats->getNdv() > output_row_size)
            column_stats->setNdv(output_row_size);
    }
}

void Statistics::mergeColumnByUnion(const String & column_name, ColumnStatisticsPtr other)
{
    chassert(other);
    auto & my = columns_stats_map.at(column_name);

    if (!my)
        columns_stats_map.insert({column_name, other});

    if (my->isUnKnown() || other->isUnKnown())
        columns_stats_map.insert({column_name, ColumnStatistics::unknown()});

    my->setMinValue(std::min(my->getMinValue(), other->getMinValue()));
    my->setMaxValue(std::max(my->getMaxValue(), other->getMaxValue()));

    auto ndv = std::min(my->getNdv(), other->getNdv());
    ndv = ndv + (std::max(my->getNdv(), other->getNdv()) - ndv) * 0.5; /// TODO add to settings
    my->setNdv(ndv);
}

}
