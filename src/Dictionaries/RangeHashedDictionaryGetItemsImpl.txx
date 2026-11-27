#include <Dictionaries/RangeHashedDictionary.h>

#define INSTANTIATE_GET_ITEMS_IMPL(DictionaryKeyType, IsNullable, AttributeType, ValueType) \
template void RangeHashedDictionary<DictionaryKeyType>::getItemsImpl<ValueType, IsNullable, DictionaryDefaultValueExtractor<AttributeType>>( \
    const Attribute & attribute,\
    const Columns & key_columns,\
    typename RangeHashedDictionary<DictionaryKeyType>::ValueSetterFunc<ValueType> && set_value,\
    DictionaryDefaultValueExtractor<AttributeType> & default_value_extractor) const;

#define INSTANTIATE_GET_ITEMS_IMPL_FOR_ATTRIBUTE_TYPE(AttributeType) \
    INSTANTIATE_GET_ITEMS_IMPL(DictionaryKeyType::Simple, true, AttributeType, DictionaryValueType<AttributeType>) \
    INSTANTIATE_GET_ITEMS_IMPL(DictionaryKeyType::Simple, false, AttributeType, DictionaryValueType<AttributeType>) \
    INSTANTIATE_GET_ITEMS_IMPL(DictionaryKeyType::Complex, true, AttributeType, DictionaryValueType<AttributeType>) \
    INSTANTIATE_GET_ITEMS_IMPL(DictionaryKeyType::Complex, false, AttributeType, DictionaryValueType<AttributeType>)

namespace DB
{

template <DictionaryKeyType dictionary_key_type>
template <typename ValueType, bool is_nullable, typename DefaultValueExtractor>
void RangeHashedDictionary<dictionary_key_type>::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    typename RangeHashedDictionary<dictionary_key_type>::ValueSetterFunc<ValueType> && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_container = std::get<AttributeContainerType<ValueType>>(attribute.container);

    PaddedPODArray<UInt64> value_indexes;
    const size_t keys_found = findValueIndexes(key_columns, value_indexes);
    const size_t keys_size = value_indexes.size();

    for (size_t key_index = 0; key_index < keys_size; ++key_index)
    {
        const UInt64 value_index = value_indexes[key_index];

        if (value_index != value_index_not_found)
        {
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

        if constexpr (is_nullable)
            set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
        else
            set_value(key_index, default_value_extractor[key_index], false);
    }

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}
}
