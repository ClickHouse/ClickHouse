#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/IColumnSourceNode.h>
#include <Analyzer/ListNode.h>

using namespace DB;

class SourceNode final : public IColumnSourceNode
{
public:
    SourceNode() : IColumnSourceNode(0 /*children_size*/) {}

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::TABLE;
    }

    void dumpTreeImpl(WriteBuffer &, FormatState &, size_t) const override
    {
    }

    bool isEqualImpl(const IQueryTreeNode &, CompareOptions) const override
    {
        return true;
    }

    void updateTreeHashImpl(HashState &, CompareOptions) const override
    {
    }

    QueryTreeNodePtr cloneImpl() const override
    {
        return std::make_shared<SourceNode>();
    }

    ASTPtr toASTImpl(const ConvertToASTOptions & /* options */) const override
    {
        return nullptr;
    }
};

TEST(QueryTreeNode, Clone)
{
    {
        auto source_node = std::make_shared<SourceNode>();

        NameAndTypePair column_name_and_type("value", std::make_shared<DataTypeUInt64>());
        auto column_node = std::make_shared<ColumnNode>(column_name_and_type, source_node);

        ASSERT_EQ(column_node->getColumnSource().get(), source_node.get());

        auto cloned_column_node = column_node->clone();

        /// If in subtree source was not cloned, source pointer must remain same
        ASSERT_NE(column_node.get(), cloned_column_node.get());
        ASSERT_EQ(cloned_column_node->as<ColumnNode &>().getColumnSource().get(), source_node.get());
        ASSERT_EQ(cloned_column_node->as<ColumnNode &>().getColumnSourceId(), source_node->getColumnSourceId());
    }
    {
        auto root_node = std::make_shared<ListNode>();
        auto source_node = std::make_shared<SourceNode>();

        NameAndTypePair column_name_and_type("value", std::make_shared<DataTypeUInt64>());
        auto column_node = std::make_shared<ColumnNode>(column_name_and_type, source_node);

        root_node->getNodes().push_back(source_node);
        root_node->getNodes().push_back(column_node);

        ASSERT_EQ(column_node->getColumnSource().get(), source_node.get());

        auto cloned_root_node = std::static_pointer_cast<ListNode>(root_node->clone());
        auto cloned_source_node = cloned_root_node->getNodes()[0];
        auto cloned_column_node = std::static_pointer_cast<ColumnNode>(cloned_root_node->getNodes()[1]);

        /** If in subtree source was cloned.
          * Source pointer for node that was cloned must remain same.
          * Source pointer for cloned node must be updated.
          */
        ASSERT_NE(column_node.get(), cloned_column_node.get());
        ASSERT_NE(source_node.get(), cloned_source_node.get());
        ASSERT_EQ(column_node->getColumnSource().get(), source_node.get());
        ASSERT_EQ(cloned_column_node->getColumnSource().get(), cloned_source_node.get());

        /// Cloned source is a new column source instance with a fresh id, and the cloned column must follow it
        auto cloned_source_id = static_cast<SourceNode &>(*cloned_source_node).getColumnSourceId();
        ASSERT_NE(cloned_source_id, source_node->getColumnSourceId());
        ASSERT_EQ(column_node->getColumnSourceId(), source_node->getColumnSourceId());
        ASSERT_EQ(cloned_column_node->getColumnSourceId(), cloned_source_id);
    }
}

TEST(QueryTreeNode, ColumnSourceId)
{
    auto lhs_source_node = std::make_shared<SourceNode>();
    auto rhs_source_node = std::make_shared<SourceNode>();

    /// Each column source instance has its own id
    ASSERT_NE(lhs_source_node->getColumnSourceId(), INVALID_COLUMN_SOURCE_ID);
    ASSERT_NE(rhs_source_node->getColumnSourceId(), INVALID_COLUMN_SOURCE_ID);
    ASSERT_NE(lhs_source_node->getColumnSourceId(), rhs_source_node->getColumnSourceId());

    NameAndTypePair column_name_and_type("value", std::make_shared<DataTypeUInt64>());
    auto column_node = std::make_shared<ColumnNode>(column_name_and_type, lhs_source_node);
    ASSERT_EQ(column_node->getColumnSourceId(), lhs_source_node->getColumnSourceId());

    /// Column source id snapshot follows setColumnSource
    column_node->setColumnSource(rhs_source_node);
    ASSERT_EQ(column_node->getColumnSourceId(), rhs_source_node->getColumnSourceId());

    /// Column source id snapshot survives the death of the source
    auto source_id_before_death = rhs_source_node->getColumnSourceId();
    rhs_source_node.reset();
    ASSERT_EQ(column_node->getColumnSourceOrNull(), nullptr);
    ASSERT_EQ(column_node->getColumnSourceId(), source_id_before_death);

    /// And is copied by clone even after the source has expired
    auto cloned_column_node = column_node->clone();
    ASSERT_EQ(cloned_column_node->as<ColumnNode &>().getColumnSourceId(), source_id_before_death);
}
