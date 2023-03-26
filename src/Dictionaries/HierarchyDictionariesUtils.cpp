#include "HierarchyDictionariesUtils.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
    /** In case of cache or direct dictionary we does not have structure with child to parent representation.
      * This function build such structure calling getColumn for initial keys to request and for next keys in hierarchy,
      * until all keys are requested or result key is null value.
      * To distinguish null value key and key that is not present in dictionary, we use special default value column
      * with max UInt64 value, if result column key has such value we assume that current key is not presented in dictionary storage.
      */
    HashMap<UInt64, UInt64> getChildToParentHierarchyMapImpl(
        const IDictionary * dictionary,
        const DictionaryAttribute & hierarchical_attribute,
        const PaddedPODArray<UInt64> & initial_keys_to_request,
        const DataTypePtr & key_type)
    {
        UInt64 null_value = hierarchical_attribute.null_value.get<UInt64>();

        ColumnPtr key_to_request_column = ColumnVector<UInt64>::create();
        auto * key_to_request_column_typed = static_cast<ColumnVector<UInt64> *>(key_to_request_column->assumeMutable().get());

        UInt64 key_not_in_storage_value = std::numeric_limits<UInt64>::max();
        ColumnPtr key_not_in_storage_default_value_column = ColumnVector<UInt64>::create(initial_keys_to_request.size(), key_not_in_storage_value);

        PaddedPODArray<UInt64> & keys_to_request = key_to_request_column_typed->getData();
        keys_to_request.assign(initial_keys_to_request);

        PaddedPODArray<UInt64> next_keys_to_request;
        HashSet<UInt64> already_requested_keys;

        HashMap<UInt64, UInt64> child_to_parent_key;

        while (!keys_to_request.empty())
        {
            child_to_parent_key.reserve(child_to_parent_key.size() + keys_to_request.size());

            auto parent_key_column = dictionary->getColumn(
                hierarchical_attribute.name,
                hierarchical_attribute.type,
                {key_to_request_column},
                {key_type},
                key_not_in_storage_default_value_column);

            const auto * parent_key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(*parent_key_column);
            if (!parent_key_column_typed)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Parent key column should be UInt64. Actual {}",
                    hierarchical_attribute.type->getName());

            const auto & parent_keys = parent_key_column_typed->getData();
            next_keys_to_request.clear();

            for (size_t i = 0; i < keys_to_request.size(); ++i)
            {
                auto key = keys_to_request[i];
                auto parent_key = parent_keys[i];

                if (parent_key == key_not_in_storage_value)
                    continue;

                child_to_parent_key[key] = parent_key;

                if (parent_key == null_value ||
                    already_requested_keys.find(parent_key) != nullptr)
                    continue;

                already_requested_keys.insert(parent_key);
                next_keys_to_request.emplace_back(parent_key);
            }

            keys_to_request.clear();
            keys_to_request.assign(next_keys_to_request);
        }

        return child_to_parent_key;
    }
}

ColumnPtr getKeysHierarchyDefaultImplementation(
    const IDictionary * dictionary,
    ColumnPtr key_column,
    const DataTypePtr & key_type,
    size_t & valid_keys)
{
    valid_keys = 0;

    key_column = key_column->convertToFullColumnIfConst();
    const auto * key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(*key_column);
    if (!key_column_typed)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Key column should be UInt64");

    const auto & dictionary_structure = dictionary->getStructure();
    size_t hierarchical_attribute_index = *dictionary_structure.hierarchical_attribute_index;
    const auto & hierarchical_attribute = dictionary_structure.attributes[hierarchical_attribute_index];

    const PaddedPODArray<UInt64> & requested_keys = key_column_typed->getData();
    HashMap<UInt64, UInt64> key_to_parent_key = getChildToParentHierarchyMapImpl(dictionary, hierarchical_attribute, requested_keys, key_type);

    auto is_key_valid_func = [&](auto & key) { return key_to_parent_key.find(key) != nullptr; };

    auto get_parent_key_func = [&](auto & key)
    {
        auto it = key_to_parent_key.find(key);
        std::optional<UInt64> result = (it != nullptr ? std::make_optional(it->getMapped()) : std::nullopt);
        valid_keys += result.has_value();
        return result;
    };

    UInt64 null_value = hierarchical_attribute.null_value.get<UInt64>();

    auto dictionary_hierarchy_array = getKeysHierarchyArray(requested_keys, null_value, is_key_valid_func, get_parent_key_func);
    return dictionary_hierarchy_array;
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

    const auto * key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(*key_column);
    if (!key_column_typed)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Key column should be UInt64");

    const auto * in_key_column_typed = checkAndGetColumn<ColumnVector<UInt64>>(*in_key_column);
    if (!in_key_column_typed)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Key column should be UInt64");

    const auto & dictionary_structure = dictionary->getStructure();
    size_t hierarchical_attribute_index = *dictionary_structure.hierarchical_attribute_index;
    const auto & hierarchical_attribute = dictionary_structure.attributes[hierarchical_attribute_index];

    const PaddedPODArray<UInt64> & requested_keys = key_column_typed->getData();
    HashMap<UInt64, UInt64> key_to_parent_key = getChildToParentHierarchyMapImpl(dictionary, hierarchical_attribute, requested_keys, key_type);

    auto is_key_valid_func = [&](auto & key) { return key_to_parent_key.find(key) != nullptr; };

    auto get_parent_key_func = [&](auto & key)
    {
        auto it = key_to_parent_key.find(key);
        std::optional<UInt64> result = (it != nullptr ? std::make_optional(it->getMapped()) : std::nullopt);
        valid_keys += result.has_value();
        return result;
    };

    UInt64 null_value = hierarchical_attribute.null_value.get<UInt64>();
    const auto & in_keys = in_key_column_typed->getData();

    auto result = getKeysIsInHierarchyColumn(requested_keys, in_keys, null_value, is_key_valid_func, get_parent_key_func);
    return result;
}

}
