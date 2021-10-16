#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <set>
#include <unordered_map>


namespace Poco { class Logger; }

namespace DB
{

class ASTSelectQuery;
class ASTFunction;
class MergeTreeData;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/** Identifies WHERE expressions that can be placed in PREWHERE by calculating respective
 *  sizes of columns used in particular expression and identifying "good" conditions of
 *  form "column_name = constant", where "constant" is outside some `threshold` specified in advance.
 *
 *  If there are "good" conditions present in WHERE, the one with minimal summary column size is transferred to PREWHERE.
 *  Otherwise any condition with minimal summary column size can be transferred to PREWHERE.
 *  If column sizes are unknown (in compact parts), the number of columns, participating in condition is used instead.
 */
class MergeTreeWhereOptimizer : private boost::noncopyable
{
public:
    MergeTreeWhereOptimizer(
        SelectQueryInfo & query_info,
        ContextPtr context,
        std::unordered_map<std::string, UInt64> column_sizes_,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & queried_columns_,
        Poco::Logger * log_);

private:
    void optimize(ASTSelectQuery & select) const;

    struct Condition
    {
        ASTPtr node;
        UInt64 columns_size = 0;
        NameSet identifiers;

        /// Can condition be moved to prewhere?
        bool viable = false;

        /// Does the condition presumably have good selectivity?
        bool good = false;

        auto tuple() const
        {
            return std::make_tuple(!viable, !good, columns_size, identifiers.size());
        }

        /// Is condition a better candidate for moving to PREWHERE?
        bool operator< (const Condition & rhs) const
        {
            return tuple() < rhs.tuple();
        }
    };

    using Conditions = std::list<Condition>;

    void analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final) const;

    /// Transform conjunctions chain in WHERE expression to Conditions list.
    Conditions analyze(const ASTPtr & expression, bool is_final) const;

    /// Transform Conditions list to WHERE or PREWHERE expression.
    static ASTPtr reconstruct(const Conditions & conditions);

    void optimizeConjunction(ASTSelectQuery & select, ASTFunction * const fun) const;

    void optimizeArbitrary(ASTSelectQuery & select) const;

    UInt64 getIdentifiersColumnSize(const NameSet & identifiers) const;

    bool hasPrimaryKeyAtoms(const ASTPtr & ast) const;

    bool isPrimaryKeyAtom(const ASTPtr & ast) const;

    bool isSortingKey(const String & column_name) const;

    bool isConstant(const ASTPtr & expr) const;

    bool isSubsetOfTableColumns(const NameSet & identifiers) const;

    /** ARRAY JOIN'ed columns as well as arrayJoin() result cannot be used in PREWHERE, therefore expressions
      *    containing said columns should not be moved to PREWHERE at all.
      *    We assume all AS aliases have been expanded prior to using this class
      *
      * Also, disallow moving expressions with GLOBAL [NOT] IN.
      */
    bool cannotBeMoved(const ASTPtr & ptr, bool is_final) const;

    void determineArrayJoinedNames(ASTSelectQuery & select);

    using StringSet = std::unordered_set<std::string>;

    String first_primary_key_column;
    const StringSet table_columns;
    const Names queried_columns;
    const NameSet sorting_key_names;
    const Block block_with_constants;
    Poco::Logger * log;
    std::unordered_map<std::string, UInt64> column_sizes;
    UInt64 total_size_of_queried_columns = 0;
    NameSet array_joined_names;
};


}
