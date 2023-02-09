#pragma once

#include <base/types.h>
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

    /** Calculate hierarchy for keys iterating the hierarchy from child to parent using get_parent_key_func provided by client.
      * Hierarchy iteration is stopped if key equals null value, get_parent_key_func returns null optional, or hierarchy depth
      * greater or equal than DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH.
      * IsKeyValidFunc used for each input hierarchy key, if it returns false result hierarchy for that key will have size 0.
      * Hierarchy result is ElementsAndOffsets structure, for each element there is hierarchy array,
      * with size offset[element_index] - (element_index > 0 ? offset[element_index - 1] : 0).
      *
      * Example:
      * id  parent_id
      * 1   0
      * 2   1
      * 3   1
      * 4   2
      *
      * If hierarchy_null_value will be 0. Requested keys [1, 2, 3, 4, 5].
      * Result: [1], [2, 1], [3, 1], [4, 2, 1], []
      * Elements: [1, 2, 1, 3, 1, 4, 2, 1]
      * Offsets: [1, 3, 5, 8, 8]
      */
    template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
    ElementsAndOffsets<KeyType> getHierarchy(
        const PaddedPODArray<KeyType> & keys,
        const KeyType & hierarchy_null_value,
        IsKeyValidFunc && is_key_valid_func,
        GetParentKeyFunc && get_parent_key_func)
    {
        size_t hierarchy_keys_size = keys.size();

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
            auto hierarchy_key = keys[i];
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

                    elements.insertFromItself(elements.begin() + start_index, elements.begin() + end_index);
                    break;
                }

                if (hierarchy_key == hierarchy_null_value || current_hierarchy_depth >= DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH)
                    break;

                already_processes_keys_to_offset[hierarchy_key] = {offsets.size(), current_hierarchy_depth};
                elements.emplace_back(hierarchy_key);
                ++current_hierarchy_depth;

                std::optional<KeyType> parent_key = std::forward<GetParentKeyFunc>(get_parent_key_func)(hierarchy_key);

                if (!parent_key.has_value())
                    break;

                hierarchy_key = *parent_key;
            }

            offsets.emplace_back(elements.size());
        }

        ElementsAndOffsets<KeyType> result = {std::move(elements), std::move(offsets)};

        return result;
    }

    /** Returns array with UInt8 represent if key from in_keys array is in hierarchy of key from keys column.
      * If value in result array is 1 that means key from in_keys array is in hierarchy of key from
      * keys array with same index, 0 therwise.
      * For getting hierarchy implementation uses getKeysHierarchy function.
      *
      * Not: keys size must be equal to in_keys_size.
      */
    template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
    PaddedPODArray<UInt8> getIsInHierarchy(
        const PaddedPODArray<KeyType> & keys,
        const PaddedPODArray<KeyType> & in_keys,
        const KeyType & hierarchy_null_value,
        IsKeyValidFunc && is_key_valid_func,
        GetParentKeyFunc && get_parent_func)
    {
        assert(keys.size() == in_keys.size());

        PaddedPODArray<UInt8> result;
        result.resize_fill(keys.size());

        detail::ElementsAndOffsets<KeyType> hierarchy = detail::getHierarchy(
            keys,
            hierarchy_null_value,
            std::forward<IsKeyValidFunc>(is_key_valid_func),
            std::forward<GetParentKeyFunc>(get_parent_func));

        auto & offsets = hierarchy.offsets;
        auto & elements = hierarchy.elements;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t i_elements_start = i > 0 ? offsets[i - 1] : 0;
            size_t i_elements_end = offsets[i];

            auto & key_to_find = in_keys[i];

            const auto * begin = elements.begin() + i_elements_start;
            const auto * end = elements.begin() + i_elements_end;

            const auto * it = std::find(begin, end, key_to_find);

            bool contains_key = (it != end);
            result[i] = contains_key;
        }

        return result;
    }

    struct GetAllDescendantsStrategy { size_t level = 0; };
    struct GetDescendantsAtSpecificLevelStrategy { size_t level = 0; };

    /** Get descendants for keys iterating the hierarchy from parent to child using parent_to_child hash map provided by client.
      * GetAllDescendantsStrategy get all descendants for key
      * GetDescendantsAtSpecificLevelStrategy get descendants only for specific hierarchy level.
      * Hierarchy result is ElementsAndOffsets structure, for each element there is descendants array,
      * with size offset[element_index] - (element_index > 0 ? offset[element_index - 1] : 0).
      *
      * @param valid_keys - number of keys that are valid in parent_to_child map
      *
      * Example:
      * id  parent_id
      * 1   0
      * 2   1
      * 3   1
      * 4   2
      *
      * Example. Strategy GetAllDescendantsStrategy.
      * Requested keys [0, 1, 2, 3, 4].
      * Result: [1, 2, 3, 4], [2, 2, 4], [4], [], []
      * Elements: [1, 2, 3, 4, 2, 3, 4, 4]
      * Offsets: [4, 7, 8, 8, 8]
      *
      * Example. Strategy GetDescendantsAtSpecificLevelStrategy with level 1.
      * Requested keys [0, 1, 2, 3, 4].
      * Result: [1], [2, 3], [4], [], [];
      * Offsets: [1, 3, 4, 4, 4];
      */
    template <typename KeyType, typename Strategy>
    ElementsAndOffsets<KeyType> getDescendants(
        const PaddedPODArray<KeyType> & keys,
        const HashMap<KeyType, PaddedPODArray<KeyType>> & parent_to_child,
        Strategy strategy,
        size_t & valid_keys)
    {
        /// If strategy is GetAllDescendantsStrategy we try to cache and later reuse previously calculated descendants.
        /// If strategy is GetDescendantsAtSpecificLevelStrategy we does not use cache strategy.
        size_t keys_size = keys.size();
        valid_keys = 0;

        PaddedPODArray<KeyType> descendants;
        descendants.reserve(keys_size);

        PaddedPODArray<IColumn::Offset> descendants_offsets;
        descendants_offsets.reserve(keys_size);

        struct Range
        {
            size_t start_index;
            size_t end_index;
        };

        static constexpr Int64 key_range_requires_update = -1;
        HashMap<KeyType, Range> already_processed_keys_to_range [[maybe_unused]];

        if constexpr (std::is_same_v<Strategy, GetAllDescendantsStrategy>)
            already_processed_keys_to_range.reserve(keys_size);

        struct KeyAndDepth
        {
            KeyType key;
            Int64 depth;
        };

        HashSet<KeyType> already_processed_keys_during_loop;
        already_processed_keys_during_loop.reserve(keys_size);

        PaddedPODArray<KeyAndDepth> next_keys_to_process_stack;
        next_keys_to_process_stack.reserve(keys_size);

        Int64 level = static_cast<Int64>(strategy.level);

        for (size_t i = 0; i < keys_size; ++i)
        {
            const KeyType & requested_key = keys[i];

            if (parent_to_child.find(requested_key) == nullptr)
            {
                descendants_offsets.emplace_back(descendants.size());
                continue;
            }
            ++valid_keys;

            next_keys_to_process_stack.emplace_back(KeyAndDepth{requested_key, 0});

            /** To cache range for key without recursive function calls and custom stack we put special
              * signaling value on stack key_range_requires_update.
              * When we pop such value from stack that means processing descendants for key is finished
              * and we can update range with end_index.
              */
            while (!next_keys_to_process_stack.empty())
            {
                KeyAndDepth key_to_process = next_keys_to_process_stack.back();

                KeyType key = key_to_process.key;
                Int64 depth = key_to_process.depth;
                next_keys_to_process_stack.pop_back();

                if constexpr (std::is_same_v<Strategy, GetAllDescendantsStrategy>)
                {
                    /// Update end_index for key
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
                            auto insert_start_iterator = descendants.begin() + range.start_index;
                            auto insert_end_iterator = descendants.begin() + range.end_index;
                            descendants.insertFromItself(insert_start_iterator, insert_end_iterator);
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
                    /// Put special signaling value on stack and update cache with range start
                    size_t range_start_index = descendants.size();
                    already_processed_keys_to_range[key].start_index = range_start_index;
                    next_keys_to_process_stack.emplace_back(KeyAndDepth{key, key_range_requires_update});
                }

                already_processed_keys_during_loop.insert(key);

                ++depth;

                const auto & children = it->getMapped();

                for (auto child_key : children)
                {
                    /// In case of GetAllDescendantsStrategy we add any descendant to result array
                    /// If strategy is GetDescendantsAtSpecificLevelStrategy we require depth == level
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

    /// Converts ElementAndOffsets structure into ArrayColumn
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

/// Returns hierarchy array column for keys
template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
ColumnPtr getKeysHierarchyArray(
    const PaddedPODArray<KeyType> & keys,
    const KeyType & hierarchy_null_value,
    IsKeyValidFunc && is_key_valid_func,
    GetParentKeyFunc && get_parent_func)
{
    auto elements_and_offsets = detail::getHierarchy(
        keys,
        hierarchy_null_value,
        std::forward<IsKeyValidFunc>(is_key_valid_func),
        std::forward<GetParentKeyFunc>(get_parent_func));

    return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
}

/// Returns is in hierarchy column for keys
template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
ColumnUInt8::Ptr getKeysIsInHierarchyColumn(
    const PaddedPODArray<KeyType> & hierarchy_keys,
    const PaddedPODArray<KeyType> & hierarchy_in_keys,
    const KeyType & hierarchy_null_value,
    IsKeyValidFunc && is_key_valid_func,
    GetParentKeyFunc && get_parent_func)
{
    auto is_in_hierarchy_data = detail::getIsInHierarchy(
        hierarchy_keys,
        hierarchy_in_keys,
        hierarchy_null_value,
        std::forward<IsKeyValidFunc>(is_key_valid_func),
        std::forward<GetParentKeyFunc>(get_parent_func));

    auto result = ColumnUInt8::create();
    result->getData() = std::move(is_in_hierarchy_data);

    return result;
}

/// Returns descendants array column for keys
///
/// @param valid_keys - number of keys that are valid in parent_to_child map
template <typename KeyType>
ColumnPtr getKeysDescendantsArray(
    const PaddedPODArray<KeyType> & requested_keys,
    const HashMap<KeyType, PaddedPODArray<KeyType>> & parent_to_child,
    size_t level,
    size_t & valid_keys)
{
    if (level == 0)
    {
        detail::GetAllDescendantsStrategy strategy { .level = level };
        auto elements_and_offsets = detail::getDescendants(requested_keys, parent_to_child, strategy, valid_keys);
        return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
    }
    else
    {
        detail::GetDescendantsAtSpecificLevelStrategy strategy { .level = level };
        auto elements_and_offsets = detail::getDescendants(requested_keys, parent_to_child, strategy, valid_keys);
        return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
    }
}

/** Default getHierarchy implementation for dictionaries that does not have structure with child to parent representation.
  * Implementation will build such structure with getColumn calls, and then getHierarchy for such structure.
  *
  * @param valid_keys - number of keys (from @key_column) for which information about parent exists.
  * @return ColumnArray with hierarchy arrays for keys from key_column.
  */
ColumnPtr getKeysHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    const DataTypePtr & key_type,
    size_t & valid_keys);

/** Default isInHierarchy implementation for dictionaries that does not have structure with child to parent representation.
  * Implementation will build such structure with getColumn calls, and then getHierarchy for such structure.
  *
  * @param valid_keys - number of keys (from @key_column) for which information about parent exists.
  * @return UInt8 column if key from in_key_column is in key hierarchy from key_column.
  */
ColumnUInt8::Ptr getKeysIsInHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr & key_type,
    size_t & valid_keys);

}
