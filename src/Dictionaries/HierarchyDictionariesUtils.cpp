#include "HierarchyDictionariesUtils.h"

#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace detail
{
    ColumnPtr convertElementsAndOffsetsIntoArray(ElementsAndOffsets && elements_and_offsets)
    {
        auto elements_column = ColumnVector<UInt64>::create();
        elements_column->getData() = std::move(elements_and_offsets.elements);

        auto offsets_column = ColumnVector<IColumn::Offset>::create();
        offsets_column->getData() = std::move(elements_and_offsets.offsets);

        auto column_array = ColumnArray::create(std::move(elements_column), std::move(offsets_column));

        return column_array;
    }
}

namespace
{
    struct ChildToParentHierarchicalContext
    {
        HashMap<UInt64, UInt64> child_key_to_parent_key;
        std::optional<HashSet<UInt64>> child_key_parent_key_is_null;
    };

    /** In case of cache or direct dictionary we does not have structure with child to parent representation.
      * This function build such structure calling getColumn for initial keys to request and for next keys in hierarchy,
      * until all keys are requested or result key is null value.
      * To distinguish null value key and key that is not present in dictionary, we use special default value column
      * with max UInt64 value, if result column key has such value we assume that current key is not presented in dictionary storage.
      */
    ChildToParentHierarchicalContext getChildToParentHierarchicalContext(
        const IDictionary * dictionary,
        const DictionaryAttribute & hierarchical_attribute,
        const PaddedPODArray<UInt64> & initial_keys_to_request,
        const DataTypePtr & key_type)
    {
        std::optional<UInt64> null_value;

        if (!hierarchical_attribute.null_value.isNull())
            null_value = hierarchical_attribute.null_value.safeGet<UInt64>();

        ColumnPtr key_to_request_column = ColumnVector<UInt64>::create();
        auto * key_to_request_column_typed = static_cast<ColumnVector<UInt64> *>(key_to_request_column->assumeMutable().get());

        UInt64 key_not_in_storage_value = std::numeric_limits<UInt64>::max();
        ColumnPtr key_not_in_storage_default_value_column = ColumnVector<UInt64>::create(initial_keys_to_request.size(), key_not_in_storage_value);
        if (hierarchical_attribute.is_nullable)
            key_not_in_storage_default_value_column = makeNullable(key_not_in_storage_default_value_column);

        PaddedPODArray<UInt64> & keys_to_request = key_to_request_column_typed->getData();
        keys_to_request.assign(initial_keys_to_request);

        PaddedPODArray<UInt64> next_keys_to_request;
        HashSet<UInt64> already_requested_keys;

        ChildToParentHierarchicalContext context;

        if (hierarchical_attribute.is_nullable)
            context.child_key_parent_key_is_null = HashSet<UInt64>();

        HashMap<UInt64, UInt64> & child_key_to_parent_key = context.child_key_to_parent_key;
        std::optional<HashSet<UInt64>> & child_key_parent_key_is_null = context.child_key_parent_key_is_null;

        while (!keys_to_request.empty())
        {
            child_key_to_parent_key.reserve(keys_to_request.size());

            auto hierarchical_attribute_parent_key_column = dictionary->getColumn(
                hierarchical_attribute.name,
                hierarchical_attribute.type,
                {key_to_request_column},
                {key_type},
                key_not_in_storage_default_value_column);

            const PaddedPODArray<UInt8> * in_key_column_nullable_mask = nullptr;

            ColumnPtr parent_key_column_non_null = hierarchical_attribute_parent_key_column;
            if (hierarchical_attribute_parent_key_column->isNullable())
            {
                const auto * parent_key_column_typed = assert_cast<const ColumnNullable *>(hierarchical_attribute_parent_key_column.get());
                in_key_column_nullable_mask = &parent_key_column_typed->getNullMapData();
                parent_key_column_non_null = parent_key_column_typed->getNestedColumnPtr();
            }

            const auto * parent_key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(&*parent_key_column_non_null);
            if (!parent_key_column_typed)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Parent key column should be UInt64. Actual {}",
                    hierarchical_attribute.type->getName());

            const auto & parent_keys = parent_key_column_typed->getData();
            next_keys_to_request.clear();

            size_t keys_to_request_size = keys_to_request.size();
            for (size_t i = 0; i < keys_to_request_size; ++i)
            {
                auto child_key = keys_to_request[i];
                auto parent_key = parent_keys[i];

                if (unlikely(in_key_column_nullable_mask) && (*in_key_column_nullable_mask)[i])
                {
                    child_key_parent_key_is_null->insert(child_key);
                    continue;
                }

                if (parent_key == key_not_in_storage_value)
                    continue;

                child_key_to_parent_key[child_key] = parent_key;

                if ((null_value && parent_key == *null_value) ||
                    already_requested_keys.find(parent_key) != nullptr)
                    continue;

                already_requested_keys.insert(parent_key);
                next_keys_to_request.emplace_back(parent_key);
            }

            keys_to_request.clear();
            keys_to_request.assign(next_keys_to_request);
        }

        return context;
    }
}

ColumnPtr getKeysDescendantsArray(
    const PaddedPODArray<UInt64> & requested_keys,
    const DictionaryHierarchicalParentToChildIndex & parent_to_child_index,
    size_t level,
    size_t & valid_keys)
{
    if (level == 0)
    {
        detail::GetAllDescendantsStrategy strategy { .level = level };
        auto elements_and_offsets = detail::getDescendants(requested_keys, parent_to_child_index, strategy, valid_keys);
        return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
    }

    detail::GetDescendantsAtSpecificLevelStrategy strategy{.level = level};
    auto elements_and_offsets = detail::getDescendants(requested_keys, parent_to_child_index, strategy, valid_keys);
    return detail::convertElementsAndOffsetsIntoArray(std::move(elements_and_offsets));
}

ColumnPtr getKeysHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    const DataTypePtr & key_type,
    size_t & valid_keys)
{
    valid_keys = 0;

    key_column = key_column->convertToFullColumnIfConst();
    const auto * key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(&*key_column);
    if (!key_column_typed)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Key column should be UInt64");

    const auto & dictionary_structure = dictionary->getStructure();
    size_t hierarchical_attribute_index = *dictionary_structure.hierarchical_attribute_index;
    const auto & hierarchical_attribute = dictionary_structure.attributes[hierarchical_attribute_index];

    const PaddedPODArray<UInt64> & requested_keys = key_column_typed->getData();
    ChildToParentHierarchicalContext child_to_parent_hierarchical_context
        = getChildToParentHierarchicalContext(dictionary, hierarchical_attribute, requested_keys, key_type);

    auto is_key_valid_func = [&](auto & key)
    {
        if (unlikely(child_to_parent_hierarchical_context.child_key_parent_key_is_null)
            && child_to_parent_hierarchical_context.child_key_parent_key_is_null->find(key))
            return true;

        return child_to_parent_hierarchical_context.child_key_to_parent_key.find(key) != nullptr;
    };

    std::optional<UInt64> null_value;

    if (!hierarchical_attribute.null_value.isNull())
        null_value = hierarchical_attribute.null_value.safeGet<UInt64>();

    auto get_parent_key_func = [&](auto & key)
    {
        std::optional<UInt64> result;

        auto it = child_to_parent_hierarchical_context.child_key_to_parent_key.find(key);
        if (it == nullptr)
            return result;

        UInt64 parent_key = it->getMapped();
        if (null_value && parent_key == *null_value)
            return result;

        result = parent_key;
        valid_keys += 1;
        return result;
    };

    return getKeysHierarchyArray(requested_keys, is_key_valid_func, get_parent_key_func);
}

ColumnUInt8::Ptr getKeysIsInHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr & key_type,
    size_t & valid_keys)
{
    valid_keys = 0;

    key_column = key_column->convertToFullColumnIfConst();
    in_key_column = in_key_column->convertToFullColumnIfConst();

    const auto * key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(&*key_column);
    if (!key_column_typed)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Key column should be UInt64");

    const auto * in_key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(&*in_key_column);
    if (!in_key_column_typed)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Key column should be UInt64");

    const auto & dictionary_structure = dictionary->getStructure();
    size_t hierarchical_attribute_index = *dictionary_structure.hierarchical_attribute_index;
    const auto & hierarchical_attribute = dictionary_structure.attributes[hierarchical_attribute_index];

    const PaddedPODArray<UInt64> & requested_keys = key_column_typed->getData();
    ChildToParentHierarchicalContext child_to_parent_hierarchical_context
        = getChildToParentHierarchicalContext(dictionary, hierarchical_attribute, requested_keys, key_type);

    auto is_key_valid_func = [&](auto & key)
    {
        if (unlikely(child_to_parent_hierarchical_context.child_key_parent_key_is_null)
            && child_to_parent_hierarchical_context.child_key_parent_key_is_null->find(key))
            return true;

        return child_to_parent_hierarchical_context.child_key_to_parent_key.find(key) != nullptr;
    };

    std::optional<UInt64> null_value;

    if (!hierarchical_attribute.null_value.isNull())
        null_value = hierarchical_attribute.null_value.safeGet<UInt64>();

    auto get_parent_key_func = [&](auto & key)
    {
        std::optional<UInt64> result;

        auto it = child_to_parent_hierarchical_context.child_key_to_parent_key.find(key);
        if (it == nullptr)
            return result;

        UInt64 parent_key = it->getMapped();
        if (null_value && parent_key == *null_value)
            return result;

        result = parent_key;
        valid_keys += 1;
        return result;
    };

    const auto & in_keys = in_key_column_typed->getData();
    return getKeysIsInHierarchyColumn(requested_keys, in_keys, is_key_valid_func, get_parent_key_func);
}

}
