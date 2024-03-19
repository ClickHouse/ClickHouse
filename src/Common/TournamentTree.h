#pragma once

#include <base/defines.h>

#include <utility>
#include <vector>
#include <iostream>
#include <optional>

#include <Common/BitHelpers.h>


namespace DB
{

/** Implementation of TournamentTree data structure
  * https://en.wikipedia.org/wiki/K-way_merge_algorithm
  */
template <typename Cursor, typename Compare = std::less<Cursor>>
class TournamentTree
{
public:
    TournamentTree() = default;

    explicit TournamentTree(const std::vector<Cursor> & cursors_, Compare compare_ = Compare())
        : compare(compare_)
    {
        for (auto & cursor : cursors_)
            cursors.emplace_back(cursor);

        if (cursors.empty())
        {
            tournament_tree.resize(1);
            return;
        }

        buildTree();
    }

    explicit TournamentTree(std::vector<Cursor> && cursors_, Compare compare_ = Compare())
        : compare(compare_)
    {
        for (auto & cursor : cursors_)
            cursors.emplace_back(std::move(cursor));

        if (cursors.empty())
        {
            tournament_tree.resize(1);
            return;
        }

        buildTree();
    }

    ALWAYS_INLINE bool isValid() const { return tournament_tree.front() != InvalidTreeNodeValue; }

    ALWAYS_INLINE Cursor & current()
    {
        assert(isValid());

        size_t cursor_index = cursorIndexFromTreeNodeValue(tournament_tree.front());
        return cursors[cursor_index];
    }

    ALWAYS_INLINE void next()
    {
        assert(isValid());

        size_t cursor_index = cursorIndexFromTreeNodeValue(tournament_tree.front());
        cursors[cursor_index].next();
        replayGames(cursor_index);
    }

    ALWAYS_INLINE Cursor & getCursor(size_t cursor_index) { return cursors[cursor_index]; }

    ALWAYS_INLINE void updateCursor(size_t cursor_index, Cursor cursor)
    {
        cursors[cursor_index] = std::move(cursor);
        replayGames(cursor_index);
    }

private:
    ALWAYS_INLINE void buildTree()
    {
        /** Build tree using cursors array.
          * Tree is placed in two arrays.
          * First array is full binary tree itself. In each node we have TreeNodeValue.
          * Second array is cursors array.
          *
          * Size of leaf layer of tournament tree array equals to nearest power of two of cursors array.
          * Size of other layers will be n - 1.
          * But we allocate array of size n * 2, root will be in 1 index to simplify index arithmetic.
          * Left child index calculated as index * 2. Right child index calculated as index * 2 + 1.
          *
          * TreeNodeValue - 0 if node is invalid. And if TreeNodeValue > 0, then it represents
          * index of cursors in cursors array + 1.
          *
          * We initialize leaf nodes of tournament tree using cursors.
          * Internal nodes of tournament tree contains compare result of left child and right child.
          * If both nodes are InvalidTreeNodeValue result is also InvalidTreeNodeValue.
          */
        size_t cursors_size = cursors.size();

        size_t nearest_power_of_two = roundUpToPowerOfTwo(cursors_size);
        size_t tree_size = nearest_power_of_two * 2;

        /// Assume InvalidTreeNodeValue = 0
        tournament_tree.resize(tree_size - 1);

        size_t current_level_child_start_index = (tree_size >> 1) - 1;

        /** Fill leaf nodes of tournament tree using cursors.
          * We iterate over cursors array and update leaf nodes from tree array.
          */
        for (size_t cursor_index = 0; cursor_index < cursors_size; ++cursor_index)
        {
            if (!cursors[cursor_index].isValid())
                continue;

            tournament_tree[current_level_child_start_index + cursor_index] = cursorIndexToTreeNodeValue(cursor_index);
        }

        current_level_child_start_index = tree_size;

        /** Fill internal nodes by level from bottom to top.
          * On each level we iterate tree array from level start to level end.
          * Next level array index start will be computed as previous_level_start / 2.
          */
        while (true)
        {
            size_t next_child_level_start_index = (current_level_child_start_index >> 1) - 1;

            if (next_child_level_start_index == 0)
                break;

            for (size_t child_start_index = next_child_level_start_index; child_start_index < current_level_child_start_index - 1;
                child_start_index += 2)
            {
                size_t parent_node_index = parentTreeNodeIndexFromChildTreeNodeIndex(child_start_index);
                tournament_tree[parent_node_index] = calculateParentTreeNodeValue(child_start_index, child_start_index + 1);
            }

            current_level_child_start_index = next_child_level_start_index + 1;
        }
    }

    ALWAYS_INLINE void replayGames(size_t cursor_index)
    {
        assert(cursor_index < cursors.size());

        TreeNodeValue update_value = cursors[cursor_index].isValid() ? cursorIndexToTreeNodeValue(cursor_index) : 0;
        size_t update_node_index = treeNodeIndexFromCursorIndex(cursor_index);
        tournament_tree[update_node_index] = update_value;

        /// Populate update for the node value through the tree from bottom to top.

        while (update_node_index != 0)
        {
            size_t parent_node_index = parentTreeNodeIndexFromChildTreeNodeIndex(update_node_index);
            update_value = calculateParentTreeNodeValue(parent_node_index * 2 + 1, parent_node_index * 2 + 2);
            tournament_tree[parent_node_index] = update_value;
            update_node_index = parent_node_index;
        }
    }

    using TreeNodeValue = size_t;

    ALWAYS_INLINE TreeNodeValue calculateParentTreeNodeValue(size_t lhs_tree_node_index, size_t rhs_tree_node_index)
    {
        TreeNodeValue lhs_tree_node_value = tournament_tree[lhs_tree_node_index];
        TreeNodeValue rhs_tree_node_value = tournament_tree[rhs_tree_node_index];

        size_t lhs_cursor_index = cursorIndexFromTreeNodeValue(lhs_tree_node_value);
        size_t rhs_cursor_index = cursorIndexFromTreeNodeValue(rhs_tree_node_value);

        TreeNodeValue result_value = lhs_tree_node_value | rhs_tree_node_value;

        if (lhs_tree_node_value != InvalidTreeNodeValue && rhs_tree_node_value != InvalidTreeNodeValue)
        {
            bool compare_result = compare(cursors[lhs_cursor_index], cursors[rhs_cursor_index]);
            result_value = compare_result ? rhs_tree_node_value : lhs_tree_node_value;
        }

        return result_value;
    }

    static ALWAYS_INLINE TreeNodeValue cursorIndexToTreeNodeValue(size_t cursor_index) { return cursor_index + 1; }

    static ALWAYS_INLINE size_t cursorIndexFromTreeNodeValue(TreeNodeValue node_value) { return node_value - 1; }

    ALWAYS_INLINE size_t treeNodeIndexFromCursorIndex(size_t cursor_index) { return ((tournament_tree.size() + 1) >> 1) - 1 + cursor_index; }

    static ALWAYS_INLINE size_t parentTreeNodeIndexFromChildTreeNodeIndex(size_t child_index) { return (child_index - 1) / 2; }

    static ALWAYS_INLINE size_t roundUpToPowerOfTwo(size_t value) { return roundUpToPowerOfTwoOrZero(value); }

    static constexpr TreeNodeValue InvalidTreeNodeValue = 0;

    std::vector<TreeNodeValue> tournament_tree;
    std::vector<Cursor> cursors;
    Compare compare;
};

}
