#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

enum StatisticType
{
    TDigest = 0,
};

struct StatisticDescription
{
    /// the type of statistic, right now it's only tdigest.
    StatisticType type;

    /// Names of statistic columns
    String column_name;

    /// Data types of statistic columns
    DataTypePtr data_type;

    StatisticDescription() = default;

    static StatisticType stringToType(String type);
};

struct StatisticsDescriptions : public std::vector<StatisticDescription>
{
    std::vector<ASTPtr> definition_asts;
    /// Stat with name exists
    bool has(const String & name) const;
    /// merge with other Statistics
    void merge(const StatisticsDescriptions & other);
    /// Convert description to string
    String toString() const;
    /// Parse description from string
    static StatisticsDescriptions getStatisticsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context);
};

}
