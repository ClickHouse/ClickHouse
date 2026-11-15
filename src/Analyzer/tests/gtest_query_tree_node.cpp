#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ListNode.h>

using namespace DB;

class SourceNode final : public IQueryTreeNode
{
public:
    SourceNode() : IQueryTreeNode(0 /*children_size*/) {}

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
    }
}
