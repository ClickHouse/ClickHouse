#pragma once

#include <common/types.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>

#include <Dictionaries/IDictionary.h>

namespace DB
{

namespace detail
{
    template <typename KeyType>
    struct ElementsAndOffsets
    {
        PaddedPODArray<KeyType> elements;
        PaddedPODArray<IColumn::Offset> offsets;
    };

    template <typename T>
    struct IsKeyValidFuncInterface
    {
        bool operator()(T key [[maybe_unused]]) { return false; }
    };

    template <typename T>
    struct GetParentKeyFuncInterface
    {
        std::optional<T> operator()(T key [[maybe_unused]]) { return {}; }
    };

    template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
    ElementsAndOffsets<KeyType> getKeysHierarchy(
        const PaddedPODArray<KeyType> & hierarchy_keys,
        const KeyType & hierarchy_null_value,
        IsKeyValidFunc && is_key_valid_func,
        GetParentKeyFunc && get_key_func)
    {
        size_t hierarchy_keys_size = hierarchy_keys.size();

        PaddedPODArray<KeyType> elements;
        elements.reserve(hierarchy_keys_size);

        PaddedPODArray<IColumn::Offset> offsets;
        offsets.reserve(hierarchy_keys_size);

        struct OffsetInArray
        {
            size_t offset_index;
            size_t array_element_offset;
        };

        HashMap<KeyType, OffsetInArray> already_processes_keys_to_offset;
        already_processes_keys_to_offset.reserve(hierarchy_keys_size);

        for (size_t i = 0; i < hierarchy_keys_size; ++i)
        {
            auto hierarchy_key = hierarchy_keys[i];
            size_t current_hierarchy_depth = 0;

            bool is_key_valid = std::forward<IsKeyValidFunc>(is_key_valid_func)(hierarchy_key);

            if (!is_key_valid)
            {
                offsets.emplace_back(elements.size());
                continue;
            }

            while (true)
            {
                const auto * it = already_processes_keys_to_offset.find(hierarchy_key);

                if (it)
                {
                    const auto & index = it->getMapped();

                    size_t offset = index.offset_index;

                    bool is_loop = (offset == offsets.size());

                    if (unlikely(is_loop))
                        break;

                    size_t array_element_offset = index.array_element_offset;

                    size_t previous_offset_size = offset > 0 ? offsets[offset - 1] : 0;
                    size_t start_index = previous_offset_size + array_element_offset;
                    size_t end_index = offsets[offset];

                    current_hierarchy_depth += end_index - start_index;

                    /// TODO: Insert part of pod array into itself
                    while (start_index < end_index)
                    {
                        elements.emplace_back(elements[start_index]);
                        ++start_index;
                    }

                    break;
                }

                if (hierarchy_key == hierarchy_null_value || current_hierarchy_depth >= DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH)
                    break;

                already_processes_keys_to_offset[hierarchy_key] = {offsets.size(), current_hierarchy_depth};
                elements.emplace_back(hierarchy_key);
                ++current_hierarchy_depth;

                std::optional<KeyType> parent_key = std::forward<GetParentKeyFunc>(get_key_func)(hierarchy_key);

                if (!parent_key.has_value())
                    break;

                hierarchy_key = *parent_key;
            }

            offsets.emplace_back(elements.size());
        }

        ElementsAndOffsets<KeyType> result = {std::move(elements), std::move(offsets)};

        return result;
    }

    struct GetAllDescendantsStrategy { size_t level = 0; };
    struct GetDescendantsAtSpecificLevelStrategy { size_t level = 0; };

    template <typename KeyType, typename Strategy>
    ElementsAndOffsets<KeyType> getDescendants(
        const PaddedPODArray<KeyType> & requested_keys,
        const HashMap<KeyType, PaddedPODArray<KeyType>> & parent_to_child,
        Strategy strategy)
    {
        PaddedPODArray<KeyType> descendants;
        descendants.reserve(requested_keys.size());

        PaddedPODArray<size_t> descendants_offsets;
        descendants_offsets.reserve(requested_keys.size());

        struct Range
        {
            size_t start_index;
            size_t end_index;
        };

        static constexpr Int64 key_range_requires_update = -1;
        HashMap<KeyType, Range> already_processed_keys_to_range [[maybe_unused]];

        if constexpr (std::is_same_v<Strategy, GetAllDescendantsStrategy>)
            already_processed_keys_to_range.reserve(requested_keys.size());

        struct KeyAndDepth
        {
            KeyType key;
            Int64 depth;
        };

        HashSet<KeyType> already_processed_keys_during_loop;
        already_processed_keys_during_loop.reserve(requested_keys.size());

        PaddedPODArray<KeyAndDepth> next_keys_to_process_stack;
        next_keys_to_process_stack.reserve(requested_keys.size());

        Int64 level = static_cast<Int64>(strategy.level);

        for (size_t i = 0; i < requested_keys.size(); ++i)
        {
            const KeyType & requested_key = requested_keys[i];

            if (parent_to_child.find(requested_key) == nullptr)
            {
                descendants_offsets.emplace_back(descendants.size());
                continue;
            }

            next_keys_to_process_stack.emplace_back(KeyAndDepth{requested_key, 0});

            while (!next_keys_to_process_stack.empty())
            {
                KeyAndDepth key_to_process = next_keys_to_process_stack.back();

                KeyType key = key_to_process.key;
                Int64 depth = key_to_process.depth;
                next_keys_to_process_stack.pop_back();

                if constexpr (std::is_same_v<Strategy, GetAllDescendantsStrategy>)
                {
                    if (depth == key_range_requires_update)
                    {
                        auto * it = already_processed_keys_to_range.find(key);
                        assert(it);

                        auto & range_to_update = it->getMapped();
                        range_to_update.end_index = descendants.size();
                        continue;
                    }
                }

                if (unlikely(already_processed_keys_during_loop.find(key) != nullptr))
                {
                    next_keys_to_process_stack.clear();
                    break;
                }

                if constexpr (std::is_same_v<Strategy, GetAllDescendantsStrategy>)
                {
                    const auto * already_processed_it = already_processed_keys_to_range.find(key);

                    if (already_processed_it)
                    {
                        Range range = already_processed_it->getMapped();

                        if (unlikely(range.start_index > range.end_index))
                        {
                            /// Broken range because there was loop
                            already_processed_keys_to_range.erase(key);
                        }
                        else
                        {
                            /// TODO: Insert part of pod array
                            while (range.start_index != range.end_index)
                            {
                                descendants.emplace_back(descendants[range.start_index]);
                                ++range.start_index;
                            }

                            continue;
                        }
                    }
                }

                const auto * it = parent_to_child.find(key);

                if (!it || depth >= DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH)
                    continue;

                if constexpr (std::is_same_v<Strategy, GetDescendantsAtSpecificLevelStrategy>)
                {
                    if (depth > level)
                        continue;
                }

                if constexpr (std::is_same_v<Strategy, GetAllDescendantsStrategy>)
                {
                    size_t range_start_index = descendants.size();
                    already_processed_keys_to_range[key].start_index = range_start_index;
                    next_keys_to_process_stack.emplace_back(KeyAndDepth{key, -1});
                }

                already_processed_keys_during_loop.insert(key);

                ++depth;

                const auto & childs = it->getMapped();

                for (auto child_key : childs)
                {
                    if (std::is_same_v<Strategy, GetAllDescendantsStrategy> || depth == level)
                        descendants.emplace_back(child_key);

                    next_keys_to_process_stack.emplace_back(KeyAndDepth{child_key, depth});
                }
            }

            already_processed_keys_during_loop.clear();

            descendants_offsets.emplace_back(descendants.size());
        }

        ElementsAndOffsets<KeyType> result = {std::move(descendants), std::move(descendants_offsets)};
        return result;
    }

    template<typename KeyType>
    ColumnPtr convertElementsAndOffsetsIntoArray(ElementsAndOffsets<KeyType> && elements_and_offsets)
    {
        auto elements_column = ColumnVector<KeyType>::create();
        elements_column->getData() = std::move(elements_and_offsets.elements);

        auto offsets_column = ColumnVector<IColumn::Offset>::create();
        offsets_column->getData() = std::move(elements_and_offsets.offsets);

        auto column_array = ColumnArray::create(std::move(elements_column), std::move(offsets_column));

        return column_array;
    }
}

template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
ColumnPtr getKeysHierarchyArray(
    const PaddedPODArray<KeyType> & hierarchy_keys,
    const KeyType & hierarchy_null_value,
    IsKeyValidFunc && is_key_valid_func,
    GetParentKeyFunc && get_parent_func)
{
    auto elements_and_offsets = detail::getKeysHierarchy(hierarchy_keys, hierarchy_null_value, std::forward<IsKeyValidFunc>(is_key_valid_func), std::forward<GetParentKeyFunc>(get_parent_func));
    return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
}

template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
PaddedPODArray<UInt8> isInKeysHierarchy(
    const PaddedPODArray<KeyType> & hierarchy_keys,
    const PaddedPODArray<KeyType> & hierarchy_in_keys,
    const KeyType & hierarchy_null_value,
    IsKeyValidFunc && is_key_valid_func,
    GetParentKeyFunc && get_parent_func)
{
    assert(hierarchy_keys.size() == hierarchy_in_keys.size());

    PaddedPODArray<UInt8> result;
    result.resize_fill(hierarchy_keys.size());

    detail::ElementsAndOffsets<KeyType> hierarchy = detail::getKeysHierarchy(
        hierarchy_keys,
        hierarchy_null_value,
        std::forward<IsKeyValidFunc>(is_key_valid_func),
        std::forward<GetParentKeyFunc>(get_parent_func));

    auto & offsets = hierarchy.offsets;
    auto & elements = hierarchy.elements;

    for (size_t i = 0; i < offsets.size(); ++i)
    {
        size_t i_elements_start = i > 0 ? offsets[i - 1] : 0;
        size_t i_elements_end = offsets[i];

        auto & key_to_find = hierarchy_in_keys[i];

        const auto * begin = elements.begin() + i_elements_start;
        const auto * end = elements.begin() + i_elements_end;

        const auto * it = std::find(begin, end, key_to_find);

        bool contains_key = (it != end);
        result[i] = contains_key;
    }

    return result;
}

template <typename KeyType>
ColumnPtr getDescendantsArray(
    const PaddedPODArray<KeyType> & requested_keys,
    const HashMap<KeyType, PaddedPODArray<KeyType>> & parent_to_child,
    size_t level)
{
    if (level == 0)
    {
        detail::GetAllDescendantsStrategy strategy { .level = level };
        auto elements_and_offsets = detail::getDescendants(requested_keys, parent_to_child, strategy);
        return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
    }
    else
    {
        detail::GetDescendantsAtSpecificLevelStrategy strategy { .level = level };
        auto elements_and_offsets = detail::getDescendants(requested_keys, parent_to_child, strategy);
        return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
    }
}

ColumnPtr getHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    const DataTypePtr & key_type);

ColumnUInt8::Ptr isInHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr & key_type);

}
