#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct StatisticDescription
{
    /// Definition AST of statistic
    ASTPtr definition_ast;

    /// the type of statistic, right now it's only tdigest.
    String type;

    /// Names of statistic columns
    String column_name;

    /// Data types of statistic columns
    DataTypePtr data_type;

    static StatisticDescription getStatisticFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context);

    StatisticDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    StatisticDescription(const StatisticDescription & other);
    StatisticDescription & operator=(const StatisticDescription & other);
};

struct StatisticsDescriptions : public std::vector<StatisticDescription>
{
    /// Stat with name exists
    bool has(const String & name) const;
    /// Convert description to string
    String toString() const;
    /// Parse description from string
    static StatisticsDescriptions parse(const String & str, const ColumnsDescription & columns, ContextPtr context);
};

}
