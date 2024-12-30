#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

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
        std::unordered_map<std::string, UInt64> column_sizes_,
        const StorageMetadataPtr & metadata_snapshot,
        const ConditionSelectivityEstimator & estimator_,
        const Names & queried_columns_,
        const std::optional<NameSet> & supported_columns_,
        LoggerPtr log_);

    void optimize(SelectQueryInfo & select_query_info, const ContextPtr & context) const;

    struct FilterActionsOptimizeResult
    {
        std::unordered_set<const ActionsDAG::Node *> prewhere_nodes;
        std::list<const ActionsDAG::Node *> prewhere_nodes_list; /// Keep insertion order of moved prewhere_nodes
        bool fully_moved_to_prewhere = false;
    };

    FilterActionsOptimizeResult optimize(const ActionsDAG & filter_dag,
        const std::string & filter_column_name,
        const ContextPtr & context,
        bool is_final);

private:
    struct Condition
    {
        explicit Condition(RPNBuilderTreeNode node_)
            : node(std::move(node_))
        {}

        RPNBuilderTreeNode node;

        UInt64 columns_size = 0;
        NameSet table_columns;

        /// Can condition be moved to prewhere?
        bool viable = false;

        /// Does the condition presumably have good selectivity?
        bool good = false;

        /// the lower the better
        Float64 estimated_row_count = 0;

        /// Does the condition contain primary key column?
        /// If so, it is better to move it further to the end of PREWHERE chain depending on minimal position in PK of any
        /// column in this condition because this condition have bigger chances to be already satisfied by PK analysis.
        Int64 min_position_in_primary_key = std::numeric_limits<Int64>::max() - 1;

        auto tuple() const
        {
            return std::make_tuple(!viable, !good, -min_position_in_primary_key, estimated_row_count, columns_size, table_columns.size());
        }

        /// Is condition a better candidate for moving to PREWHERE?
        bool operator< (const Condition & rhs) const
        {
            return tuple() < rhs.tuple();
        }
    };

    using Conditions = std::list<Condition>;

    struct WhereOptimizerContext
    {
        ContextPtr context;
        NameSet array_joined_names;
        bool move_all_conditions_to_prewhere = false;
        bool move_primary_key_columns_to_end_of_prewhere = false;
        bool allow_reorder_prewhere_conditions = false;
        bool is_final = false;
        bool use_statistics = false;
    };

    struct OptimizeResult
    {
        Conditions where_conditions;
        Conditions prewhere_conditions;
    };

    std::optional<OptimizeResult> optimizeImpl(const RPNBuilderTreeNode & node, const WhereOptimizerContext & where_optimizer_context) const;

    void analyzeImpl(Conditions & res, const RPNBuilderTreeNode & node, const WhereOptimizerContext & where_optimizer_context, std::set<Int64> & pk_positions) const;

    /// Transform conjunctions chain in WHERE expression to Conditions list.
    Conditions analyze(const RPNBuilderTreeNode & node, const WhereOptimizerContext & where_optimizer_context) const;

    /// Reconstruct AST from conditions
    static ASTPtr reconstructAST(const Conditions & conditions);

    void optimizeArbitrary(ASTSelectQuery & select) const;

    UInt64 getColumnsSize(const NameSet & columns) const;

    bool columnsSupportPrewhere(const NameSet & columns) const;

    bool isExpressionOverSortingKey(const RPNBuilderTreeNode & node) const;

    bool isSortingKey(const String & column_name) const;

    bool isConstant(const ASTPtr & expr) const;

    bool isSubsetOfTableColumns(const NameSet & columns) const;

    /** ARRAY JOIN'ed columns as well as arrayJoin() result cannot be used in PREWHERE, therefore expressions
      *    containing said columns should not be moved to PREWHERE at all.
      *    We assume all AS aliases have been expanded prior to using this class
      *
      * Also, disallow moving expressions with GLOBAL [NOT] IN.
      */
    bool cannotBeMoved(const RPNBuilderTreeNode & node, const WhereOptimizerContext & where_optimizer_context) const;

    static NameSet determineArrayJoinedNames(const ASTSelectQuery & select);

    const ConditionSelectivityEstimator estimator;

    const NameSet table_columns;
    const Names queried_columns;
    const std::optional<NameSet> supported_columns;
    const NameSet sorting_key_names;
    const NameToIndexMap primary_key_names_positions;
    LoggerPtr log;
    std::unordered_map<std::string, UInt64> column_sizes;
    UInt64 total_size_of_queried_columns = 0;
};


}
