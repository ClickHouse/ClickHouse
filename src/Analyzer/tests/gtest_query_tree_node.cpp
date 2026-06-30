#include <gtest/gtest.h>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/HashUtils.h>
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

        /// clone() preserves column source ids: the cloned source is the same logical source,
        /// so its id equals the original and the cloned column follows it.
        auto cloned_source_id = static_cast<SourceNode &>(*cloned_source_node).getColumnSourceId();
        ASSERT_EQ(cloned_source_id, source_node->getColumnSourceId());
        ASSERT_EQ(column_node->getColumnSourceId(), source_node->getColumnSourceId());
        ASSERT_EQ(cloned_column_node->getColumnSourceId(), cloned_source_id);
    }
    {
        auto root_node = std::make_shared<ListNode>();
        auto source_node = std::make_shared<SourceNode>();

        NameAndTypePair column_name_and_type("value", std::make_shared<DataTypeUInt64>());
        auto column_node = std::make_shared<ColumnNode>(column_name_and_type, source_node);

        root_node->getNodes().push_back(source_node);
        root_node->getNodes().push_back(column_node);

        /// cloneWithFreshColumnSourceIds() mints a fresh id for the cloned source, and the cloned
        /// column follows it. The original keeps its id.
        auto cloned_root_node = std::static_pointer_cast<ListNode>(root_node->cloneWithFreshColumnSourceIds());
        auto cloned_source_node = cloned_root_node->getNodes()[0];
        auto cloned_column_node = std::static_pointer_cast<ColumnNode>(cloned_root_node->getNodes()[1]);

        auto cloned_source_id = static_cast<SourceNode &>(*cloned_source_node).getColumnSourceId();
        ASSERT_NE(cloned_source_id, source_node->getColumnSourceId());
        ASSERT_EQ(column_node->getColumnSourceId(), source_node->getColumnSourceId());
        ASSERT_EQ(cloned_column_node->getColumnSourceId(), cloned_source_id);
        ASSERT_EQ(cloned_column_node->getColumnSource().get(), cloned_source_node.get());
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

TEST(QueryTreeNode, LocalAndGlobalCompare)
{
    /// Two structurally equal sources that are distinct instances, e.g. a self join
    auto lhs_source_node = std::make_shared<SourceNode>();
    auto rhs_source_node = std::make_shared<SourceNode>();

    NameAndTypePair column_name_and_type("value", std::make_shared<DataTypeUInt64>());
    auto lhs_column_node = std::make_shared<ColumnNode>(column_name_and_type, lhs_source_node);
    auto rhs_column_node = std::make_shared<ColumnNode>(column_name_and_type, rhs_source_node);

    /// Locally the columns are different: they reference different column source instances
    ASSERT_FALSE(lhs_column_node->isEqualLocal(*rhs_column_node));
    ASSERT_NE(lhs_column_node->getTreeHashLocal(), rhs_column_node->getTreeHashLocal());

    /// Globally the columns are equal: the sources are equal by content
    ASSERT_TRUE(lhs_column_node->isEqualGlobal(*rhs_column_node));
    ASSERT_EQ(lhs_column_node->getTreeHashGlobal(), rhs_column_node->getTreeHashGlobal());

    /// Columns referencing the same source instance are equal both locally and globally
    auto lhs_column_node_same_source = std::make_shared<ColumnNode>(column_name_and_type, lhs_source_node);
    ASSERT_TRUE(lhs_column_node->isEqualLocal(*lhs_column_node_same_source));
    ASSERT_EQ(lhs_column_node->getTreeHashLocal(), lhs_column_node_same_source->getTreeHashLocal());
    ASSERT_TRUE(lhs_column_node->isEqualGlobal(*lhs_column_node_same_source));
    ASSERT_EQ(lhs_column_node->getTreeHashGlobal(), lhs_column_node_same_source->getTreeHashGlobal());

    /// A clone of a column keeps referencing the original source, so it stays locally equal
    auto cloned_lhs_column_node = lhs_column_node->clone();
    ASSERT_TRUE(lhs_column_node->isEqualLocal(*cloned_lhs_column_node));
    ASSERT_EQ(lhs_column_node->getTreeHashLocal(), cloned_lhs_column_node->getTreeHashLocal());
}

TEST(QueryTreeNode, InternalColumnSourceComparedByContent)
{
    /// A column source that is internal to the compared subtree (reachable through children, e.g. a
    /// lambda or a subquery used as an expression) is compared by content even locally, so two
    /// independently built but structurally equal copies are equal. This mirrors a GROUP BY key
    /// written as a full expression vs the equal projection expression. The criterion is positional
    /// (internal vs external), not the source node type.
    NameAndTypePair col("value", std::make_shared<DataTypeUInt64>());

    auto lhs_source = std::make_shared<SourceNode>();
    auto lhs_list = std::make_shared<ListNode>();
    lhs_list->getNodes().push_back(lhs_source);
    lhs_list->getNodes().push_back(std::make_shared<ColumnNode>(col, lhs_source));

    auto rhs_source = std::make_shared<SourceNode>();
    auto rhs_list = std::make_shared<ListNode>();
    rhs_list->getNodes().push_back(rhs_source);
    rhs_list->getNodes().push_back(std::make_shared<ColumnNode>(col, rhs_source));

    /// Distinct source instances, but internal to each list.
    ASSERT_NE(lhs_source->getColumnSourceId(), rhs_source->getColumnSourceId());

    /// Internal sources are compared by content, so the lists are equal locally and globally.
    ASSERT_TRUE(lhs_list->isEqualLocal(*rhs_list));
    ASSERT_EQ(lhs_list->getTreeHashLocal(), rhs_list->getTreeHashLocal());
    ASSERT_TRUE(lhs_list->isEqualGlobal(*rhs_list));
    ASSERT_EQ(lhs_list->getTreeHashGlobal(), rhs_list->getTreeHashGlobal());
}

TEST(QueryTreeNode, RepeatedColumnSourceGlobalConsistency)
{
    /// Content comparison must use a bijective source correspondence so it agrees with the
    /// per-source hashing: `List(col(s), col(s))` (one source twice) must not compare equal to
    /// `List(col(s1), col(s2))` (two distinct sources), and the hash must distinguish them too.
    NameAndTypePair col("value", std::make_shared<DataTypeUInt64>());
    auto s = std::make_shared<SourceNode>();
    auto s1 = std::make_shared<SourceNode>();
    auto s2 = std::make_shared<SourceNode>();

    auto make_pair_list = [&](const QueryTreeNodePtr & a, const QueryTreeNodePtr & b)
    {
        auto list = std::make_shared<ListNode>();
        list->getNodes().push_back(std::make_shared<ColumnNode>(col, a));
        list->getNodes().push_back(std::make_shared<ColumnNode>(col, b));
        return list;
    };

    auto same = make_pair_list(s, s);
    auto same_again = make_pair_list(s, s);
    auto distinct = make_pair_list(s1, s2);

    /// Same repetition pattern -> equal and hash-equal.
    ASSERT_TRUE(same->isEqualGlobal(*same_again));
    ASSERT_EQ(same->getTreeHashGlobal(), same_again->getTreeHashGlobal());

    /// One repeated source vs two distinct sources -> not equal, and hashes differ (consistent).
    ASSERT_FALSE(same->isEqualGlobal(*distinct));
    ASSERT_NE(same->getTreeHashGlobal(), distinct->getTreeHashGlobal());

    /// The hash-keyed container must keep distinct keys yet still find an equal one.
    QueryTreeNodePtrWithGlobalHashSet set;
    set.insert(QueryTreeNodePtrWithGlobalHash(same));
    set.insert(QueryTreeNodePtrWithGlobalHash(distinct));
    ASSERT_EQ(set.size(), 2u);
    ASSERT_TRUE(set.contains(QueryTreeNodePtrWithGlobalHash(same_again)));
}

TEST(QueryTreeNode, RepeatedInternalColumnSourceLocalConsistency)
{
    /// The same bijection applies to local comparison when the repeated source is internal to the
    /// compared subtree (here the source is a child of the list and is referenced by two columns).
    NameAndTypePair col("value", std::make_shared<DataTypeUInt64>());

    auto make_internal_repeat = [&]()
    {
        auto src = std::make_shared<SourceNode>();
        auto list = std::make_shared<ListNode>();
        list->getNodes().push_back(src);
        list->getNodes().push_back(std::make_shared<ColumnNode>(col, src));
        list->getNodes().push_back(std::make_shared<ColumnNode>(col, src));
        return list;
    };

    auto a = make_internal_repeat();
    auto b = make_internal_repeat();

    ASSERT_TRUE(a->isEqualLocal(*b));
    ASSERT_EQ(a->getTreeHashLocal(), b->getTreeHashLocal());
}
