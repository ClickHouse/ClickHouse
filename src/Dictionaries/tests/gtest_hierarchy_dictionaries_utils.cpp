#include <gtest/gtest.h>

#include <Common/HashTable/HashMap.h>

#include <Dictionaries/HierarchyDictionariesUtils.h>

using namespace DB;

TEST(HierarchyDictionariesUtils, getHierarchy)
{
    {
        HashMap<UInt64, UInt64> child_to_parent;
        child_to_parent[1] = 0;
        child_to_parent[2] = 1;
        child_to_parent[3] = 1;
        child_to_parent[4] = 2;

        auto is_key_valid_func = [&](auto key) { return child_to_parent.find(key) != nullptr; };

        UInt64 hierarchy_null_value_key = 0;
        auto get_parent_key_func = [&](auto key)
        {
            std::optional<UInt64> result;
            auto it = child_to_parent.find(key);
            if (it == nullptr)
                return result;

            UInt64 parent_key = it->getMapped();
            if (parent_key == hierarchy_null_value_key)
                return result;

            result = parent_key;
            return result;
        };

        PaddedPODArray<UInt64> keys = {1, 2, 3, 4, 5};

        auto result = DB::detail::getHierarchy(
            keys,
            is_key_valid_func,
            get_parent_key_func);

        const auto & actual_elements = result.elements;
        const auto & actual_offsets = result.offsets;

        PaddedPODArray<UInt64> expected_elements = {1, 2, 1, 3, 1, 4, 2, 1};
        PaddedPODArray<IColumn::Offset> expected_offsets = {1, 3, 5, 8, 8};

        ASSERT_EQ(actual_elements, expected_elements);
        ASSERT_EQ(actual_offsets, expected_offsets);
    }
    {
        HashMap<UInt64, UInt64> child_to_parent;
        child_to_parent[1] = 2;
        child_to_parent[2] = 1;

        auto is_key_valid_func = [&](auto key) { return child_to_parent.find(key) != nullptr; };

        UInt64 hierarchy_null_value_key = 0;
        auto get_parent_key_func = [&](auto key)
        {
            std::optional<UInt64> result;
            auto it = child_to_parent.find(key);
            if (it == nullptr)
                return result;

            UInt64 parent_key = it->getMapped();
            if (parent_key == hierarchy_null_value_key)
                return result;

            result = parent_key;
            return result;
        };

        PaddedPODArray<UInt64> keys = {1, 2, 3};

        auto result = DB::detail::getHierarchy(
            keys,
            is_key_valid_func,
            get_parent_key_func);

        const auto & actual_elements = result.elements;
        const auto & actual_offsets = result.offsets;

        PaddedPODArray<UInt64> expected_elements = {1, 2, 2};
        PaddedPODArray<IColumn::Offset> expected_offsets = {2, 3, 3};

        ASSERT_EQ(actual_elements, expected_elements);
        ASSERT_EQ(actual_offsets, expected_offsets);
    }
}

TEST(HierarchyDictionariesUtils, getIsInHierarchy)
{
    {
        HashMap<UInt64, UInt64> child_to_parent;
        child_to_parent[1] = 0;
        child_to_parent[2] = 1;
        child_to_parent[3] = 1;
        child_to_parent[4] = 2;

        auto is_key_valid_func = [&](auto key) { return child_to_parent.find(key) != nullptr; };

        UInt64 hierarchy_null_value_key = 0;
        auto get_parent_key_func = [&](auto key)
        {
            std::optional<UInt64> result;
            auto it = child_to_parent.find(key);
            if (it == nullptr)
                return result;

            UInt64 parent_key = it->getMapped();
            if (parent_key == hierarchy_null_value_key)
                return result;

            result = parent_key;
            return result;
        };

        PaddedPODArray<UInt64> keys = {1, 2, 3, 4, 5};
        PaddedPODArray<UInt64> keys_in = {1, 1, 1, 2, 5};

        PaddedPODArray<UInt8> actual = DB::detail::getIsInHierarchy(
            keys,
            keys_in,
            is_key_valid_func,
            get_parent_key_func);

        PaddedPODArray<UInt8> expected = {1,1,1,1,0};

        ASSERT_EQ(actual, expected);
    }
    {
        HashMap<UInt64, UInt64> child_to_parent;
        child_to_parent[1] = 2;
        child_to_parent[2] = 1;

        auto is_key_valid_func = [&](auto key)
        {
            return child_to_parent.find(key) != nullptr;
        };

        UInt64 hierarchy_null_value_key = 0;
        auto get_parent_key_func = [&](auto key)
        {
            std::optional<UInt64> result;
            auto it = child_to_parent.find(key);
            if (it == nullptr)
                return result;

            UInt64 parent_key = it->getMapped();
            if (parent_key == hierarchy_null_value_key)
                return result;

            result = parent_key;
            return result;
        };

        PaddedPODArray<UInt64> keys = {1, 2, 3};
        PaddedPODArray<UInt64> keys_in = {1, 2, 3};

        PaddedPODArray<UInt8> actual = DB::detail::getIsInHierarchy(
            keys,
            keys_in,
            is_key_valid_func,
            get_parent_key_func);

        PaddedPODArray<UInt8> expected = {1, 1, 0};
        ASSERT_EQ(actual, expected);
    }
}

TEST(HierarchyDictionariesUtils, getDescendants)
{
    {
        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;
        parent_to_child[0].emplace_back(1);
        parent_to_child[1].emplace_back(2);
        parent_to_child[1].emplace_back(3);
        parent_to_child[2].emplace_back(4);

        auto parent_to_child_index = std::make_shared<DictionaryHierarchicalParentToChildIndex>(parent_to_child);

        PaddedPODArray<UInt64> keys = {0, 1, 2, 3, 4};

        {
            size_t keys_found;
            auto result = DB::detail::getDescendants(
                keys,
                *parent_to_child_index,
                DB::detail::GetAllDescendantsStrategy(),
                keys_found);

            const auto & actual_elements = result.elements;
            const auto & actual_offsets = result.offsets;

            PaddedPODArray<UInt64> expected_elements = {1, 2, 3, 4, 2, 3, 4, 4};
            PaddedPODArray<IColumn::Offset> expected_offsets = {4, 7, 8, 8, 8};

            ASSERT_EQ(actual_elements, expected_elements);
            ASSERT_EQ(actual_offsets, expected_offsets);
            ASSERT_EQ(keys_found, 3);
        }
        {
            size_t keys_found;
            auto result = DB::detail::getDescendants(
                keys,
                *parent_to_child_index,
                DB::detail::GetDescendantsAtSpecificLevelStrategy{1},
                keys_found);

            const auto & actual_elements = result.elements;
            const auto & actual_offsets = result.offsets;

            PaddedPODArray<UInt64> expected_elements = {1, 2, 3, 4};
            PaddedPODArray<IColumn::Offset> expected_offsets = {1, 3, 4, 4, 4};

            ASSERT_EQ(actual_elements, expected_elements);
            ASSERT_EQ(actual_offsets, expected_offsets);
            ASSERT_EQ(keys_found, 3);
        }
    }
    {
        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;
        parent_to_child[1].emplace_back(2);
        parent_to_child[2].emplace_back(1);

        auto parent_to_child_index = std::make_shared<DictionaryHierarchicalParentToChildIndex>(parent_to_child);

        PaddedPODArray<UInt64> keys = {1, 2, 3};

        {
            size_t keys_found;
            auto result = DB::detail::getDescendants(
                keys,
                *parent_to_child_index,
                DB::detail::GetAllDescendantsStrategy(),
                keys_found);

            const auto & actual_elements = result.elements;
            const auto & actual_offsets = result.offsets;

            PaddedPODArray<UInt64> expected_elements = {2, 1, 1};
            PaddedPODArray<IColumn::Offset> expected_offsets = {2, 3, 3};

            ASSERT_EQ(actual_elements, expected_elements);
            ASSERT_EQ(actual_offsets, expected_offsets);
            ASSERT_EQ(keys_found, 2);
        }
        {
            size_t keys_found;
            auto result = DB::detail::getDescendants(
                keys,
                *parent_to_child_index,
                DB::detail::GetDescendantsAtSpecificLevelStrategy{1},
                keys_found);

            const auto & actual_elements = result.elements;
            const auto & actual_offsets = result.offsets;

            PaddedPODArray<UInt64> expected_elements = {2, 1};
            PaddedPODArray<IColumn::Offset> expected_offsets = {1, 2, 2};

            ASSERT_EQ(actual_elements, expected_elements);
            ASSERT_EQ(actual_offsets, expected_offsets);
            ASSERT_EQ(keys_found, 2);
        }
    }
}
