#pragma once

#include <Core/Names.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class IDataType;
class ASTFunction;
class ASTIdentifier;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Visits AST node to rewrite alias columns in query
/// Currently works only 3 kind ways below

/// For example:
//    CREATE TABLE test_table
//    (
//     `timestamp` DateTime,
//     `value` UInt64,
//     `day` Date ALIAS toDate(timestamp),
//     `day1` Date ALIAS day + 1,
//     `day2` Date ALIAS day1 + 1,
//     `time` DateTime ALIAS timestamp
//    )ENGINE = MergeTree
//    PARTITION BY toYYYYMMDD(timestamp)
//    ORDER BY timestamp SETTINGS index_granularity = 1;

/// 1. Rewrite the filters in query when enable optimize_respect_aliases
///  this could help with `optimize_trivial_count`, Partition Prune in `KeyCondition` and secondary indexes.
///  eg: select max(value) from test_table where day2 = today(), filters will be: ((toDate(timestamp) + 1) + 1) = today() .

/// 2. Alias on alias for `required_columns` extracted in `InterpreterSelectQuery.cpp`, it could help get all dependent physical columns for query.
///  eg: select day2 from test_table. `required_columns` can got require columns from the temporary rewritten AST `((toDate(timestamp) + 1) + 1)`.

/// 3. Help with `optimize_aggregation_in_order` and `optimize_read_in_order` in `ReadInOrderOptimizer.cpp`:
///    For queries with alias columns in `orderBy` and `groupBy`, these ASTs will not change.
///    But we generate temporary asts and generate temporary Actions to get the `InputOrderInfo`
///  eg: select day1 from test_table order by day1;


class ColumnAliasesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ColumnAliasesMatcher, false, true>;

    struct Data
    {
        const ColumnsDescription & columns;

        /// columns from array_join_result_to_source cannot be expanded.
        NameSet array_join_result_columns;
        NameSet array_join_source_columns;
        ContextPtr context;

        const std::unordered_set<IAST *> & excluded_nodes;

        /// private_aliases are from lambda, so these are local names.
        NameSet private_aliases;

        /// Check if query is changed by this visitor.
        bool changed = false;

        Data(const ColumnsDescription & columns_, const NameToNameMap & array_join_result_columns_, ContextPtr context_, const std::unordered_set<IAST *> & excluded_nodes_)
            : columns(columns_), context(context_), excluded_nodes(excluded_nodes_)
        {
            for (const auto & [result, source] : array_join_result_columns_)
            {
                array_join_result_columns.insert(result);
                array_join_source_columns.insert(source);
            }
        }
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child, const Data & data);

private:
    static void visit(ASTIdentifier & node, ASTPtr & ast, Data & data);
    static void visit(ASTFunction & node, ASTPtr & ast, Data & data);
};

using ColumnAliasesVisitor = ColumnAliasesMatcher::Visitor;

}
