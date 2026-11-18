#include <Dictionaries/RangeHashedDictionary.h>

#define INSTANTIATE_GET_ITEMS_SHORT_CIRCUIT_IMPL(DictionaryKeyType, IsNullable, ValueType) \
    template void RangeHashedDictionary<DictionaryKeyType>::getItemsShortCircuitImpl<ValueType, IsNullable>( \
        const Attribute & attribute, \
        const Columns & key_columns, \
        typename RangeHashedDictionary<DictionaryKeyType>::ValueSetterFunc<ValueType> && set_value, \
        IColumn::Filter & default_mask) const;

#define INSTANTIATE_GET_ITEMS_SHORT_CIRCUIT_IMPL_FOR_ATTRIBUTE_TYPE(AttributeType) \
    INSTANTIATE_GET_ITEMS_SHORT_CIRCUIT_IMPL(DictionaryKeyType::Simple, true, DictionaryValueType<AttributeType>) \
    INSTANTIATE_GET_ITEMS_SHORT_CIRCUIT_IMPL(DictionaryKeyType::Simple, false, DictionaryValueType<AttributeType>) \
    INSTANTIATE_GET_ITEMS_SHORT_CIRCUIT_IMPL(DictionaryKeyType::Complex, true, DictionaryValueType<AttributeType>) \
    INSTANTIATE_GET_ITEMS_SHORT_CIRCUIT_IMPL(DictionaryKeyType::Complex, false, DictionaryValueType<AttributeType>)

namespace DB
{

template <DictionaryKeyType dictionary_key_type>
template <typename ValueType, bool is_nullable>
void RangeHashedDictionary<dictionary_key_type>::getItemsShortCircuitImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    typename RangeHashedDictionary<dictionary_key_type>::ValueSetterFunc<ValueType> && set_value,
    IColumn::Filter & default_mask) const
{
    const auto & attribute_container = std::get<AttributeContainerType<ValueType>>(attribute.container);

    PaddedPODArray<UInt64> value_indexes;
    const size_t keys_found = findValueIndexes(key_columns, value_indexes);
    const size_t keys_size = value_indexes.size();
    default_mask.resize(keys_size);

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        const UInt64 value_index = value_indexes[key_index];

        if (value_index != value_index_not_found)
        {
            default_mask[key_index] = 0;

            ValueType value = attribute_container[value_index];

            if constexpr (is_nullable)
            {
                bool is_null = (*attribute.is_value_nullable)[value_index];
                set_value(key_index, value, is_null);
            }
            else
            {
                set_value(key_index, value, false);
            }

            continue;
        }

        default_mask[key_index] = 1;
        set_value(key_index, ValueType{}, true);
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}
}
