#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>

#include <base/types.h>

namespace DB
{

enum StatisticType : UInt8
{
    TDigest = 0,
    Uniq = 1,

    UnknownStatistics = 63,
};

class ColumnsDescription;

struct StatisticDescription
{
    /// the type of statistic, right now it's only tdigest.
    StatisticType type;

    ASTPtr ast;

    String getTypeName() const;

    StatisticDescription() = default;

    bool operator==(const StatisticDescription & other) const
    {
        return type == other.type; //&& column_name == other.column_name;
    }
};

struct ColumnDescription;

struct StatisticsDescription
{
    std::unordered_map<StatisticType, StatisticDescription> stats;

    bool operator==(const StatisticsDescription & other) const
    {
        for (const auto & iter : stats)
        {
            if (!other.stats.contains(iter.first))
                return false;
            if (!(iter.second == other.stats.at(iter.first)))
                return false;
        }
        return stats.size() == other.stats.size();
    }

    bool empty() const
    {
        return stats.empty();
    }

    bool contains(const String & stat_type) const;

    void merge(const StatisticsDescription & other, const ColumnDescription & column, bool if_not_exists);

    void modify(const StatisticsDescription & other);

    void clear();

    void add(StatisticType stat_type, const StatisticDescription & desc);

    ASTPtr getAST() const;

    String column_name;
    DataTypePtr data_type;

    static std::vector<StatisticsDescription> getStatisticsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);
    static StatisticsDescription getStatisticFromColumnDeclaration(const ASTColumnDeclaration & column);

};

}
