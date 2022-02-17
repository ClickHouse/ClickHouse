#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <boost/noncopyable.hpp>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include "base/types.h"

#include <memory>
#include <set>
#include <tuple>
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
        const Settings & settings,
        std::unordered_map<std::string, UInt64> column_sizes_,
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePtr & storage_,
        const Names & queried_columns_,
        Poco::Logger * log_);

private:
    void optimize(ASTSelectQuery & select) const;

    void optimizeBySize(ASTSelectQuery & select) const;
    void optimizeByRanks(ASTSelectQuery & select) const;

    // Description of simple expression (ident <=> lit).
    // Used by new planner.
    // TODO: support tuples
    // TODO: support monotonic functions
    struct ConditionDescription
    {
        enum class Type {
            LESS_OR_EQUAL,
            GREATER_OR_EQUAL,
            EQUAL,
            NOT_EQUAL,
        };

        String identifier;
        Type type;
        Field constant;
    };

    // Conditions 
    struct Condition
    {
        ASTPtr node;
        UInt64 columns_size = 0;
        
        NameSet identifiers;

        // TODO: support many identifiers (OR)
        std::optional<ConditionDescription> description;

        /// Can condition be moved to prewhere?
        bool viable = false;

        /// Does the condition presumably have good selectivity?
        /// old algorithm
        bool good = false;

        /// New algorithm
        double rank = 0;
        double selectivity = 0;

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

    bool tryAnalyzeTupleEquals(Conditions & res, const ASTFunction * func, bool is_final) const;
    bool tryAnalyzeTupleCompare(Conditions & res, const ASTFunction * func, bool is_final) const;
    void analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final) const;

    std::optional<ConditionDescription> parseCondition(const ASTPtr & condition) const;

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

    struct ColumnWithRank {
        ColumnWithRank(
            double rank_,
            double selectivity_,
            std::string name_)
            : rank(rank_)
            , selectivity(selectivity_)
            , name(name_) {
        }

        bool operator<(const ColumnWithRank & other) const;
        bool operator==(const ColumnWithRank & other) const;

        double rank;
        double selectivity;
        std::string name;
    };

    std::vector<ColumnWithRank> getSimpleColumns(const std::unordered_map<std::string, Conditions> & column_to_simple_conditions) const;
    double scoreSelectivity(const std::optional<ConditionDescription> & condition_description) const;

    const std::unordered_map<ConditionDescription::Type, ConditionDescription::Type>& getCompareFuncsSwaps() const;
    const std::unordered_map<ConditionDescription::Type, std::string>& getCompareTypeToString() const;

    using StringSet = std::unordered_set<std::string>;

    String first_primary_key_column;
    const StringSet table_columns;
    const Names queried_columns;
    const NameSet sorting_key_names;
    const Block block_with_constants;
    Poco::Logger * log;
    std::unordered_map<std::string, UInt64> column_sizes;
    IStatisticsPtr stats;
    bool use_new_scoring;
    UInt64 total_size_of_queried_columns = 0;
    NameSet array_joined_names;
};


}
