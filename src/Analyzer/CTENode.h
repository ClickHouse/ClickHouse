#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Core/NamesAndTypes.h>
#include "Interpreters/Context_fwd.h"
namespace DB
{
class QueryNode;
class UnionNode;

class CTENode : public IQueryTreeNode
{
public:
    static constexpr auto CTE_PREFIX = "__cte_";

    CTENode() = delete;

    explicit CTENode(QueryTreeNodePtr query_tree_node);

    QueryTreeNodeType getNodeType() const override { return QueryTreeNodeType::CTE; }


    /// Get query node CTE name
    const std::string & getCTEName() const
    {
        return cte_name;
    }

    /// Get query node unique CTE name
    const std::string & getUniqueCTEName() const
    {
        return unique_cte_name;
    }

    /// Set query node CTE name
    void setCTEName(std::string cte_name_value, size_t cte_index)
    {
        if (!cte_name_value.empty())
        {
            cte_name = std::move(cte_name_value);
            unique_cte_name = CTE_PREFIX + cte_name + "_" + std::to_string(cte_index);
        }
    }

    /// Returns true if the query node is a materialized CTE
    bool isMaterializedCTE() const
    {
        return is_materialized_cte;
    }

    /// Set query node is materialized CTE
    void setIsMaterializedCTE(bool is_materialized_cte_value)
    {
        is_materialized_cte = is_materialized_cte_value;
    }

    /// Set storage engine for the holder if query node is a materialized CTE
    void setMaterializedCTEEngine(ASTPtr materialized_cte_engine_value)
    {
        materialized_cte_engine = std::move(materialized_cte_engine_value);
    }

    /// Get storage engine for the holder
    ASTPtr getMaterializedCTEEngine() const
    {
        return materialized_cte_engine;
    }

    /// Returns true if the query node is a recursive CTE
    bool isRecursiveCTE() const
    {
        return is_recursive_cte;
    }

    /// Set query node is recursive CTE
    void setIsRecursiveCTE(bool is_recursive_cte_value)
    {
        is_recursive_cte = is_recursive_cte_value;
    }

    /// Get CTE reference counter
    size_t getCTEReferenceCounter() const
    {
        return cte_ref.use_count();
    }

    /// Get the query node
    QueryTreeNodePtr getQueryNode() const
    {
        return children[0];
    }

    /// Get projection column
    NamesAndTypes getProjectionColumns() const;

    /// Get mutable context
    ContextMutablePtr getMutableContext() const;

    /// Set future table
    void setFutureTable(FutureTableFromCTEPtr future_table_value)
    {
        future_table = std::move(future_table_value);
    }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions &) const override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

private:

    QueryTreeNodePtr query_tree_node;
    const QueryNode * query_node = nullptr;
    const UnionNode * union_node = nullptr;

    FutureTableFromCTEPtr future_table;

    static constexpr auto children_size = 1;

    bool is_materialized_cte = false;
    bool is_recursive_cte = false;
    std::shared_ptr<bool> cte_ref = std::make_shared<bool>();
    std::string cte_name;
    std::string unique_cte_name;
    ASTPtr materialized_cte_engine;

};

}
