#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Field.h>

#include <Parsers/SelectUnionMode.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Analyzer/RecursiveCTE.h>

#include <Interpreters/Context_fwd.h>

namespace DB
{

/** Union node represents union of queries in query tree.
  * Union node must be initialized with normalized union mode.
  *
  * Example: (SELECT id FROM test_table) UNION ALL (SELECT id FROM test_table_2);
  * Example: (SELECT id FROM test_table) UNION DISTINCT (SELECT id FROM test_table_2);
  * Example: (SELECT id FROM test_table) EXCEPT ALL (SELECT id FROM test_table_2);
  * Example: (SELECT id FROM test_table) EXCEPT DISTINCT (SELECT id FROM test_table_2);
  * Example: (SELECT id FROM test_table) INTERSECT ALL (SELECT id FROM test_table_2);
  * Example: (SELECT id FROM test_table) INTERSECT DISTINCT (SELECT id FROM test_table_2);
  *
  * Union node can be used as CTE.
  * Example: WITH cte_subquery AS ((SELECT id FROM test_table) UNION ALL (SELECT id FROM test_table_2)) SELECT * FROM cte_subquery;
  *
  * Union node can be used as scalar subquery.
  * Example: SELECT (SELECT 1 UNION DISTINCT SELECT 1);
  *
  * During query analysis pass union node queries must be resolved.
  */
class UnionNode;
using UnionNodePtr = std::shared_ptr<UnionNode>;

class UnionNode final : public IQueryTreeNode
{
public:
    /// Construct union node with context and normalized union mode
    explicit UnionNode(ContextMutablePtr context_, SelectUnionMode union_mode_);

    /// Get context
    ContextPtr getContext() const
    {
        return context;
    }

    /// Get mutable context
    const ContextMutablePtr & getMutableContext() const
    {
        return context;
    }

    /// Get mutable context
    ContextMutablePtr & getMutableContext()
    {
        return context;
    }

    /// Returns true if union node is subquery, false otherwise
    bool isSubquery() const
    {
        return is_subquery;
    }

    /// Set union node is subquery value
    void setIsSubquery(bool is_subquery_value)
    {
        is_subquery = is_subquery_value;
    }

    /// Returns true if union node is CTE, false otherwise
    bool isCTE() const
    {
        return is_cte;
    }

    /// Set union node is CTE
    void setIsCTE(bool is_cte_value)
    {
        is_cte = is_cte_value;
    }

    /// Returns true if union node CTE is specified in WITH RECURSIVE, false otherwise
    bool isRecursiveCTE() const
    {
        return is_recursive_cte;
    }

    /// Set union node is recursive CTE value
    void setIsRecursiveCTE(bool is_recursive_cte_value)
    {
        is_recursive_cte = is_recursive_cte_value;
    }

    /// Returns true if union node has recursive CTE table, false otherwise
    bool hasRecursiveCTETable() const
    {
        return recursive_cte_table.has_value();
    }

    /// Returns optional recursive CTE table
    const std::optional<RecursiveCTETable> & getRecursiveCTETable() const
    {
        return recursive_cte_table;
    }

    /// Returns optional recursive CTE table
    std::optional<RecursiveCTETable> & getRecursiveCTETable()
    {
        return recursive_cte_table;
    }

    /// Set union node recursive CTE table value
    void setRecursiveCTETable(RecursiveCTETable recursive_cte_table_value)
    {
        recursive_cte_table.emplace(std::move(recursive_cte_table_value));
    }

    /// Get union node CTE name
    const std::string & getCTEName() const
    {
        return cte_name;
    }

    /// Set union node CTE name
    void setCTEName(std::string cte_name_value)
    {
        cte_name = std::move(cte_name_value);
    }

    /// Get union mode
    SelectUnionMode getUnionMode() const
    {
        return union_mode;
    }

    /// Get union node queries
    const ListNode & getQueries() const
    {
        return children[queries_child_index]->as<const ListNode &>();
    }

    /// Get union node queries
    ListNode & getQueries()
    {
        return children[queries_child_index]->as<ListNode &>();
    }

    /// Get union node queries node
    const QueryTreeNodePtr & getQueriesNode() const
    {
        return children[queries_child_index];
    }

    /// Get union node queries node
    QueryTreeNodePtr & getQueriesNode()
    {
        return children[queries_child_index];
    }

    /// Compute union node projection columns
    NamesAndTypes computeProjectionColumns() const;

    /// Remove unused projection columns
    void removeUnusedProjectionColumns(const std::unordered_set<std::string> & used_projection_columns);

    /// Remove unused projection columns
    void removeUnusedProjectionColumns(const std::unordered_set<size_t> & used_projection_columns_indexes);

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::UNION;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState &, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    bool is_subquery = false;
    bool is_cte = false;
    bool is_recursive_cte = false;
    std::optional<RecursiveCTETable> recursive_cte_table;
    std::string cte_name;
    ContextMutablePtr context;
    SelectUnionMode union_mode;

    static constexpr size_t queries_child_index = 0;
    static constexpr size_t children_size = queries_child_index + 1;
};

}
