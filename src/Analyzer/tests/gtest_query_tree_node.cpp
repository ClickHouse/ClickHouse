#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <Parsers/SelectUnionMode.h>

using namespace DB;

class SourceNode final : public ITableExpressionNode
{
public:
    SourceNode() : ITableExpressionNode(0 /*children_size*/) {}

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

namespace
{

ContextMutablePtr makeCanonicalQueryContext()
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    return context;
}

}

TEST(QueryNodeQueryContextLifetime, CloneReplacementAndRerootRefreshOwner)
{
    std::weak_ptr<Context> first_root_weak;
    std::weak_ptr<Context> second_root_weak;
    std::weak_ptr<Context> rerooted_context_weak;
    QueryNodePtr cloned_node;

    {
        auto first_root = makeCanonicalQueryContext();
        first_root_weak = first_root;

        auto original_context = Context::createCopy(first_root);
        auto original_node = std::make_shared<QueryNode>(original_context);
        original_context.reset();
        first_root.reset();

        ASSERT_FALSE(first_root_weak.expired());

        /// A same-root replacement must retain the canonical context.
        auto same_root_context = Context::createCopy(original_node->getContext());
        original_node->setContext(std::move(same_root_context));
        EXPECT_FALSE(first_root_weak.expired());

        cloned_node = std::static_pointer_cast<QueryNode>(original_node->clone());
        original_node.reset();
        EXPECT_FALSE(first_root_weak.expired());
    }

    {
        auto second_root = makeCanonicalQueryContext();
        second_root_weak = second_root;
        auto second_context = Context::createCopy(second_root);

        /// Replacing with another canonical root releases the old owner and pins the new one.
        cloned_node->setContext(second_context);
        second_root.reset();
        EXPECT_TRUE(first_root_weak.expired());
        EXPECT_FALSE(second_root_weak.expired());

        /// Re-root this node context in place, then refresh the owner through setContext with
        /// the same pointer. The old canonical root must no longer be retained by the node.
        rerooted_context_weak = second_context;
        second_context->makeQueryContext();
        cloned_node->setContext(second_context);
        EXPECT_TRUE(second_root_weak.expired());
        EXPECT_FALSE(rerooted_context_weak.expired());
    }

    cloned_node.reset();
    EXPECT_TRUE(rerooted_context_weak.expired());
}

TEST(QueryNodeQueryContextLifetime, UnionCloneCarriesBranchOwners)
{
    std::weak_ptr<Context> canonical_weak;
    QueryTreeNodePtr cloned_union_tree;

    {
        auto canonical = makeCanonicalQueryContext();
        canonical_weak = canonical;

        auto union_node = std::make_shared<UnionNode>(
            Context::createCopy(canonical), SelectUnionMode::UNION_ALL);
        union_node->setIsCTE(true);
        union_node->setCTEName("query_context_lifetime_cte");
        union_node->getQueries().getNodes().push_back(
            std::make_shared<QueryNode>(Context::createCopy(canonical)));
        union_node->getQueries().getNodes().push_back(
            std::make_shared<QueryNode>(Context::createCopy(canonical)));

        cloned_union_tree = union_node->clone();
        const auto * cloned_union_node = cloned_union_tree->as<UnionNode>();
        ASSERT_NE(cloned_union_node, nullptr);
        ASSERT_EQ(cloned_union_node->getQueries().getNodes().size(), 2u);
        union_node.reset();
        canonical.reset();

        /// The cloned branch QueryNodes must carry the canonical owner after the original
        /// union tree and external root have been released.
        ASSERT_FALSE(canonical_weak.expired());
    }

    cloned_union_tree.reset();
    EXPECT_TRUE(canonical_weak.expired());
}
