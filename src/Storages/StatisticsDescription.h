#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <base/types.h>

namespace DB
{

enum StatisticType
{
    TDigest = 0,
};

class ColumnsDescription;

struct StatisticDescription
{
    /// the type of statistic, right now it's only tdigest.
    StatisticType type;

    /// Names of statistic columns
    String column_name;

    ASTPtr ast;

    String getTypeName() const;

    StatisticDescription() = default;

    bool operator==(const StatisticDescription & other) const
    {
        return type == other.type && column_name == other.column_name;
    }

    static StatisticDescription getStatisticFromColumnDeclaration(const ASTColumnDeclaration & column);

    static std::vector<StatisticDescription> getStatisticsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns);
};

}
