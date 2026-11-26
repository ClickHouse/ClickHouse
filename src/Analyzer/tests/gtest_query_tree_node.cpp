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

/// `isEqualLocal` compares the column sources by identity, except the column sources that are inside of the compared trees.
TEST(QueryTreeNode, LocalComparisonOfColumnSources)
{
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

    auto check_equal_local = [](const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
    {
        ASSERT_TRUE(lhs->isEqualLocal(*rhs));
        ASSERT_TRUE(rhs->isEqualLocal(*lhs));
        ASSERT_EQ(lhs->getTreeHashLocal(), rhs->getTreeHashLocal());
    };

    auto check_not_equal_local = [](const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
    {
        ASSERT_FALSE(lhs->isEqualLocal(*rhs));
        ASSERT_FALSE(rhs->isEqualLocal(*lhs));
    };

    /// Two different column sources with the same content, outside of the compared trees.
    auto source_a = std::make_shared<SourceNode>();
    auto source_b = std::make_shared<SourceNode>();

    /// Columns of the same column source are equal.
    check_equal_local(make_column(source_a), make_column(source_a));
    check_equal_local(
        make_list({make_column(source_a), make_column(source_b)}),
        make_list({make_column(source_a), make_column(source_b)}));

    /// Columns of different column sources are equal only by content.
    ASSERT_TRUE(make_column(source_a)->isEqual(*make_column(source_b)));
    check_not_equal_local(make_column(source_a), make_column(source_b));
    check_not_equal_local(
        make_list({make_column(source_a), make_column(source_b)}),
        make_list({make_column(source_b), make_column(source_a)}));

    /// A clone of an expression refers to the same column source.
    {
        auto expression = make_list({make_column(source_a), make_column(source_b)});
        check_equal_local(expression, expression->clone());
    }

    /// Column sources inside of the compared trees are different nodes, and they are paired with each other.
    {
        auto make_tree_with_sources = [&](bool second_column_from_second_source)
        {
            auto first_source = std::make_shared<SourceNode>();
            auto second_source = std::make_shared<SourceNode>();
            return make_list({
                first_source,
                second_source,
                make_column(first_source),
                make_column(second_column_from_second_source ? second_source : first_source)});
        };

        check_equal_local(make_tree_with_sources(true), make_tree_with_sources(true));
        check_equal_local(make_tree_with_sources(false), make_tree_with_sources(false));
        check_not_equal_local(make_tree_with_sources(true), make_tree_with_sources(false));

        /// A clone of a tree with its column sources.
        auto tree = make_tree_with_sources(true);
        check_equal_local(tree, tree->clone());
    }

    /// A column source inside of one tree is not equal to a column source outside of the other tree.
    {
        auto inner_source = std::make_shared<SourceNode>();
        check_not_equal_local(
            make_list({inner_source, make_column(inner_source)}),
            make_list({std::make_shared<SourceNode>(), make_column(source_a)}));
    }

    /// A destroyed column source is still different from another destroyed column source.
    {
        auto first_source = std::make_shared<SourceNode>();
        auto second_source = std::make_shared<SourceNode>();

        auto first_column = make_column(first_source);
        auto another_first_column = make_column(first_source);
        auto second_column = make_column(second_source);

        first_source.reset();
        second_source.reset();

        check_equal_local(first_column, another_first_column);
        check_not_equal_local(first_column, second_column);
    }
}
