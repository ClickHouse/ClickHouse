#pragma once

#include <memory>
#include <unordered_map>
#include <set>
#include <Core/Block.h>


namespace Poco { class Logger; }

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class ASTSelectQuery;
class ASTFunction;
class MergeTreeData;

using IdentifierNameSet = std::set<std::string>;


/** Identifies WHERE expressions that can be placed in PREWHERE by calculating respective
 *  sizes of columns used in particular expression and identifying "good" conditions of
 *  form "column_name = constant", where "constant" is outside some `threshold` specified in advance.
 *
 *  If there are "good" conditions present in WHERE, the one with minimal summary column size is
 *  transferred to PREWHERE.
 *  Otherwise any condition with minimal summary column size can be transferred to PREWHERE, if only
 *  its relative size (summary column size divided by query column size) is less than `max_columns_relative_size`.
 */
class MergeTreeWhereOptimizer
{
public:
    MergeTreeWhereOptimizer(const MergeTreeWhereOptimizer&) = delete;
    MergeTreeWhereOptimizer& operator=(const MergeTreeWhereOptimizer&) = delete;

    MergeTreeWhereOptimizer(
        ASTPtr & query, const Context & context, const MergeTreeData & data, const Names & column_names,
        Poco::Logger * log);

private:
    void optimize(ASTSelectQuery & select) const;

    void calculateColumnSizes(const MergeTreeData & data, const Names & column_names);

    void optimizeConjunction(ASTSelectQuery & select, ASTFunction * const fun) const;

    void optimizeArbitrary(ASTSelectQuery & select) const;

    std::size_t getIdentifiersColumnSize(const IdentifierNameSet & identifiers) const;

    bool isConditionGood(const IAST * condition) const;

    static void collectIdentifiersNoSubqueries(const IAST * const ast, IdentifierNameSet & set);

    bool hasPrimaryKeyAtoms(const IAST * ast) const;

    bool isPrimaryKeyAtom(const IAST * const ast) const;

    bool isConstant(const ASTPtr & expr) const;

    bool isSubsetOfTableColumns(const IdentifierNameSet & identifiers) const;

    /** ARRAY JOIN'ed columns as well as arrayJoin() result cannot be used in PREWHERE, therefore expressions
      *    containing said columns should not be moved to PREWHERE at all.
      *    We assume all AS aliases have been expanded prior to using this class
      *
      * Also, disallow moving expressions with GLOBAL [NOT] IN.
      */
    bool cannotBeMoved(const IAST * ptr) const;

    void determineArrayJoinedNames(ASTSelectQuery & select);

    using string_set_t = std::unordered_set<std::string>;

    const string_set_t primary_key_columns;
    const string_set_t table_columns;
    const Block block_with_constants;
    Poco::Logger * log;
    std::unordered_map<std::string, std::size_t> column_sizes{};
    std::size_t total_column_size{};
    NameSet array_joined_names;
};


}
