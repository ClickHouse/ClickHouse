#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Field.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

#include <Parsers/SelectUnionMode.h>

namespace DB
{

/** Union node represents union of queries in query tree.
  */
class UnionNode;
using UnionNodePtr = std::shared_ptr<UnionNode>;

class UnionNode final : public IQueryTreeNode
{
public:
    explicit UnionNode();

    bool isSubquery() const
    {
        return is_subquery;
    }

    void setIsSubquery(bool is_subquery_value)
    {
        is_subquery = is_subquery_value;
    }

    bool isCTE() const
    {
        return is_cte;
    }

    void setIsCTE(bool is_cte_value)
    {
        is_cte = is_cte_value;
    }

    const std::string & getCTEName() const
    {
        return cte_name;
    }

    void setCTEName(std::string cte_name_value)
    {
        cte_name = std::move(cte_name_value);
    }

    SelectUnionMode getUnionMode() const
    {
        return union_mode;
    }

    void setUnionMode(SelectUnionMode union_mode_value)
    {
        union_mode = union_mode_value;
    }

    const SelectUnionModes & getUnionModes() const
    {
        return union_modes;
    }

    void setUnionModes(const SelectUnionModes & union_modes_value)
    {
        union_modes = union_modes_value;
        union_modes_set = SelectUnionModesSet(union_modes.begin(), union_modes.end());
    }

    const QueryTreeNodePtr & getQueriesNode() const
    {
        return children[queries_child_index];
    }

    QueryTreeNodePtr & getQueriesNode()
    {
        return children[queries_child_index];
    }

    const ListNode & getQueries() const
    {
        return children[queries_child_index]->as<const ListNode &>();
    }

    ListNode & getQueries()
    {
        return children[queries_child_index]->as<ListNode &>();
    }

    /// Compute union projection
    NamesAndTypes computeProjectionColumns() const;

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::UNION;
    }

    String getName() const override;

    DataTypePtr getResultType() const override
    {
        if (constant_value)
            return constant_value->getType();

        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getResultType is not supported for non scalar union node");
    }

    /// Perform constant folding for scalar union node
    void performConstantFolding(ConstantValuePtr constant_folded_value)
    {
        constant_value = std::move(constant_folded_value);
    }

    ConstantValuePtr getConstantValueOrNull() const override
    {
        return constant_value;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState &) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    bool is_subquery = false;
    bool is_cte = false;
    std::string cte_name;
    SelectUnionMode union_mode;
    SelectUnionModes union_modes;
    SelectUnionModesSet union_modes_set;
    ConstantValuePtr constant_value;

    static constexpr size_t queries_child_index = 0;
    static constexpr size_t children_size = queries_child_index + 1;
};

}
