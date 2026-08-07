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

    size_t keys_found = 0;

    const ColumnPtr & range_column = key_columns.back();
    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();
    default_mask.resize(keys_size);

    impl::callOnRangeType(
        dict_struct.range_min->type,
        [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using RangeColumnType = typename Types::LeftType;
            using RangeStorageType = typename RangeColumnType::ValueType;
            using RangeInterval = Interval<RangeStorageType>;

            const auto * range_column_typed = typeid_cast<const RangeColumnType *>(range_column.get());
            if (!range_column_typed)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Dictionary {} range column type should be equal to {}",
                    getFullName(),
                    dict_struct.range_min->type->getName());

            const auto & range_column_data = range_column_typed->getData();

            const auto & key_attribute_container = std::get<KeyAttributeContainerType<RangeStorageType>>(key_attribute.container);

            for (size_t key_index = 0; key_index < keys_size; ++key_index)
            {
                auto key = keys_extractor.extractCurrentKey();
                const auto it = key_attribute_container.find(key);

                if (it)
                {
                    const auto date = range_column_data[key_index];
                    const auto & interval_tree = it->getMapped();

                    size_t value_index = 0;
                    std::optional<RangeInterval> range;

                    interval_tree.find(
                        date,
                        [&](auto & interval, auto & interval_value_index)
                        {
                            if (range)
                            {
                                if (likely(configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::min) && interval < *range)
                                {
                                    range = interval;
                                    value_index = interval_value_index;
                                }
                                else if (configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::max && interval > *range)
                                {
                                    range = interval;
                                    value_index = interval_value_index;
                                }
                            }
                            else
                            {
                                range = interval;
                                value_index = interval_value_index;
                            }

                            return true;
                        });

                    if (range.has_value())
                    {
                        default_mask[key_index] = 0;
                        ++keys_found;

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

                        keys_extractor.rollbackCurrentKey();
                        continue;
                    }
                }

                default_mask[key_index] = 1;
                set_value(key_index, ValueType{}, true);

                keys_extractor.rollbackCurrentKey();
            }
        });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}
}
