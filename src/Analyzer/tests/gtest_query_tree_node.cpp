#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ListNode.h>

using namespace DB;

class SourceNode final : public ITableExpressionNode
{
public:
    SourceNode() : ITableExpressionNode(0 /*children_size*/) {}

    [[nodiscard]] QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::TABLE;
    }

    void dumpTreeImpl(WriteBuffer &, FormatState &, size_t) const override
    {
    }

    [[nodiscard]] bool isEqualImpl(const IQueryTreeNode &, CompareOptions) const override
    {
        return true;
    }

    void updateTreeHashImpl(HashState &, CompareOptions) const override
    {
    }

    [[nodiscard]] QueryTreeNodePtr cloneImpl() const override
    {
        return std::make_shared<SourceNode>();
    }

    [[nodiscard]] ASTPtr toASTImpl(const ConvertToASTOptions & /* options */) const override
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

/// The correspondence between the column sources of two compared trees must be one-to-one,
/// so that `isEqual` agrees with `getTreeHash`.
TEST(QueryTreeNode, ColumnSourcesCorrespondenceInIsEqual)
{
    /// Two different column sources with the same content.
    auto source_a = std::make_shared<SourceNode>();
    auto source_b = std::make_shared<SourceNode>();

    NameAndTypePair column_name_and_type("value", std::make_shared<DataTypeUInt64>());

    auto make_column = [&](const std::shared_ptr<SourceNode> & source)
    {
        return std::make_shared<ColumnNode>(column_name_and_type, source);
    };

    auto make_list = [](QueryTreeNodes nodes)
    {
        auto list = std::make_shared<ListNode>();
        list->getNodes() = std::move(nodes);
        return list;
    };

    auto check_equal = [](const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
    {
        ASSERT_TRUE(lhs->isEqual(*rhs));
        ASSERT_TRUE(rhs->isEqual(*lhs));
        ASSERT_EQ(lhs->getTreeHash(), rhs->getTreeHash());
    };

    auto check_not_equal = [](const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
    {
        ASSERT_FALSE(lhs->isEqual(*rhs));
        ASSERT_FALSE(rhs->isEqual(*lhs));
        ASSERT_NE(lhs->getTreeHash(), rhs->getTreeHash());
    };

    /// A column of a different source with the same content is equal.
    check_equal(make_column(source_a), make_column(source_b));
    check_equal(
        make_list({make_column(source_a), make_column(source_a)}),
        make_list({make_column(source_b), make_column(source_b)}));
    check_equal(
        make_list({make_column(source_a), make_column(source_b)}),
        make_list({make_column(source_b), make_column(source_a)}));

    /// (a, a) and (a, b): no node is shared between the trees.
    check_not_equal(
        make_list({make_column(source_a), make_column(source_a)}),
        make_list({make_column(source_a), make_column(source_b)}));
    check_not_equal(
        make_list({make_column(source_a), make_column(source_a)}),
        make_list({make_column(source_b), make_column(source_a)}));

    /// (a, a) and (a, b): the column node of `a` is shared between the trees.
    {
        auto shared_column = make_column(source_a);

        check_not_equal(
            make_list({shared_column, shared_column}),
            make_list({shared_column, make_column(source_b)}));
        check_not_equal(
            make_list({shared_column, shared_column}),
            make_list({make_column(source_b), shared_column}));
        check_not_equal(
            make_list({shared_column, make_column(source_a)}),
            make_list({shared_column, make_column(source_b)}));
        check_not_equal(
            make_list({make_column(source_a), shared_column}),
            make_list({make_column(source_b), shared_column}));

        check_equal(
            make_list({shared_column, make_column(source_a)}),
            make_list({shared_column, make_column(source_a)}));
    }

    /// The same, but the shared node is a subtree that contains the column.
    {
        auto shared_subtree = make_list({make_column(source_a)});

        check_not_equal(
            make_list({shared_subtree, make_column(source_a)}),
            make_list({shared_subtree, make_column(source_b)}));
        check_not_equal(
            make_list({make_column(source_a), shared_subtree}),
            make_list({make_column(source_b), shared_subtree}));

        check_equal(
            make_list({shared_subtree, make_column(source_a)}),
            make_list({shared_subtree, make_column(source_a)}));
    }
}
