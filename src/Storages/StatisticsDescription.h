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

    /// List of expressions for statistic calculation
    /// Contains only columns (col1, col2, ...)
    //ASTPtr expression_list_ast;

    /// Statistic name
    String name;

    String type;

    /// Names of statistic columns
    Names column_names;

    /// Data types of statistic  columns
    DataTypes data_types;

    static StatisticDescription getStatisticFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context);

    StatisticDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    StatisticDescription(const StatisticDescription & other);
    StatisticDescription & operator=(const StatisticDescription & other);
};

struct StatisticDescriptions : public std::vector<StatisticDescription>
{
    /// Stat with name exists
    bool has(const String & name) const;
    /// Convert description to string
    String toString() const;
    /// Parse description from string
    // TODO: replication
    static StatisticDescriptions parse(const String & str, const ColumnsDescription & columns, ContextPtr context);
};

}
