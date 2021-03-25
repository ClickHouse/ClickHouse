#pragma once

#include <common/types.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>

#include <Dictionaries/IDictionary.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

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
struct GetKeyFuncInterface
{
    std::optional<T> operator()(T key [[maybe_unused]]) { return {}; }
};

template <typename KeyType, typename IsKeyValidFunc, typename GetKeyFunc>
ElementsAndOffsets<KeyType> getKeysHierarchy(
    const PaddedPODArray<KeyType> & hierarchy_keys,
    const KeyType & hierarchy_null_value,
    IsKeyValidFunc && is_key_valid_func,
    GetKeyFunc && get_key_func)
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

            std::optional<KeyType> parent_key = std::forward<GetKeyFunc>(get_key_func)(hierarchy_key);

            if (!parent_key.has_value())
                break;

            hierarchy_key = *parent_key;
        }

        offsets.emplace_back(elements.size());
    }

    ElementsAndOffsets<KeyType> result = {std::move(elements), std::move(offsets)};

    return result;
}

template <typename KeyType, typename IsKeyValidFunc, typename GetParentKeyFunc>
ColumnPtr getKeysHierarchyArray(
    const PaddedPODArray<KeyType> & hierarchy_keys,
    const KeyType & hierarchy_null_value,
    IsKeyValidFunc && is_key_valid_func,
    GetParentKeyFunc && get_parent_func)
{
    auto elements_and_offsets = getKeysHierarchy(hierarchy_keys, hierarchy_null_value, std::forward<IsKeyValidFunc>(is_key_valid_func), std::forward<GetParentKeyFunc>(get_parent_func));

    auto elements_column = ColumnVector<KeyType>::create();
    elements_column->getData() = std::move(elements_and_offsets.elements);

    auto offsets_column = ColumnVector<IColumn::Offset>::create();
    offsets_column->getData() = std::move(elements_and_offsets.offsets);

    auto column_array = ColumnArray::create(std::move(elements_column), std::move(offsets_column));
    return column_array;
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

    ElementsAndOffsets<KeyType> hierarchy = getKeysHierarchy(
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

template <typename KeyType, typename IsKeyValidFunc, typename GetDescendantKeyFunc>
ColumnPtr getDescendantsArray(
    const PaddedPODArray<KeyType> & hierarchy_keys,
    const KeyType & hierarchy_null_value,
    size_t level,
    IsKeyValidFunc && is_key_valid_func,
    GetDescendantKeyFunc && get_descendant_func)
{
    auto elements_and_offsets = getKeysHierarchy(
        hierarchy_keys,
        hierarchy_null_value,
        std::forward<IsKeyValidFunc>(is_key_valid_func),
        std::forward<GetDescendantKeyFunc>(get_descendant_func));

    auto & elements = elements_and_offsets.elements;
    auto & offsets = elements_and_offsets.offsets;

    std::cerr << "getDescendantsArray" << std::endl;
    std::cerr << "Elements " << elements.size() << std::endl;
    for (auto element : elements)
        std::cerr << element << " ";
    std::cerr << std::endl;
    std::cerr << "Offsets " << offsets.size() << std::endl;
    for (auto offset : offsets)
        std::cerr << offset << " ";
    std::cerr << std::endl;

    PaddedPODArray<KeyType> descendants;
    descendants.reserve(elements.size());

    PaddedPODArray<size_t> descendants_offsets;
    descendants_offsets.reserve(elements.size());

    for (size_t i = 0; i < offsets.size(); ++i)
    {
        size_t offset_start_index = i > 0 ? offsets[i - 1] : 0;
        size_t offset_end_index = offsets[i];
        size_t size = offset_end_index - offset_start_index;

        if (level == 0)
            descendants.insert(elements.begin() + offset_start_index + 1, elements.begin() + offset_end_index);
        else if (level < size)
            descendants.emplace_back(elements[offset_start_index + level]);

        descendants_offsets.emplace_back(descendants.size());
    }

    auto elements_column = ColumnVector<KeyType>::create();
    elements_column->getData() = std::move(descendants);

    auto offsets_column = ColumnVector<IColumn::Offset>::create();
    offsets_column->getData() = std::move(descendants_offsets);

    auto column_array = ColumnArray::create(std::move(elements_column), std::move(offsets_column));
    return column_array;
}

ColumnPtr getHierarchyDefaultImplementation(const IDictionary * dictionary, ColumnPtr key_column, const DataTypePtr & key_type);

ColumnUInt8::Ptr isInHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr & key_type);

}
