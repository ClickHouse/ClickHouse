#include <Dictionaries/RangeHashedDictionary.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

template <DictionaryKeyType dictionary_key_type>
ColumnPtr RangeHashedDictionary<dictionary_key_type>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultOrFilter default_or_filter) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(default_or_filter);
    assert(is_short_circuit || std::holds_alternative<RefDefault>(default_or_filter));

    if (dictionary_key_type == DictionaryKeyType::Complex)
    {
        auto key_types_copy = key_types;
        key_types_copy.pop_back();
        dict_struct.validateKeyTypes(key_types_copy);
    }

    ColumnPtr result;

    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, attribute_type);
    const size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute = attributes[attribute_index];

    /// Cast range column to storage type
    Columns modified_key_columns = key_columns;
    const ColumnPtr & range_storage_column = key_columns.back();
    ColumnWithTypeAndName column_to_cast = {range_storage_column->convertToFullColumnIfConst(), key_types.back(), ""};
    modified_key_columns.back() = castColumnAccurate(column_to_cast, dict_struct.range_min->type);

    size_t keys_size = key_columns.front()->size();
    bool is_attribute_nullable = attribute.is_value_nullable.has_value();

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (is_attribute_nullable)
    {
        col_null_map_to = ColumnUInt8::create(keys_size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        auto column = ColumnProvider::getColumn(dictionary_attribute, keys_size);

        if (is_short_circuit)
        {
            IColumn::Filter & default_mask = std::get<RefFilter>(default_or_filter).get();
            size_t keys_found = 0;

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                auto * out = column.get();

                keys_found = getItemsShortCircuitImpl<ValueType, false>(
                    attribute,
                    modified_key_columns,
                    [&](size_t, const Array & value, bool)
                    {
                        out->insert(value);
                    },
                    default_mask);
            }
            else if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                auto * out = column.get();

                if (is_attribute_nullable)
                    keys_found = getItemsShortCircuitImpl<ValueType, true>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, StringRef value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out->insertData(value.data, value.size);
                        },
                        default_mask);
                else
                    keys_found = getItemsShortCircuitImpl<ValueType, false>(
                        attribute,
                        modified_key_columns,
                        [&](size_t, StringRef value, bool)
                        {
                            out->insertData(value.data, value.size);
                        },
                        default_mask);
            }
            else
            {
                auto & out = column->getData();

                if (is_attribute_nullable)
                    keys_found = getItemsShortCircuitImpl<ValueType, true>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, const auto value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out[row] = value;
                        },
                        default_mask);
                else
                    keys_found = getItemsShortCircuitImpl<ValueType, false>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, const auto value, bool)
                        {
                            out[row] = value;
                        },
                        default_mask);

                out.resize(keys_found);
            }

            if (is_attribute_nullable)
                vec_null_map_to->resize(keys_found);
        }
        else
        {
            const ColumnPtr & default_values_column = std::get<RefDefault>(default_or_filter).get();

            DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(
            dictionary_attribute.null_value, default_values_column);

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                auto * out = column.get();

                getItemsImpl<ValueType, false>(
                    attribute,
                    modified_key_columns,
                    [&](size_t, const Array & value, bool)
                    {
                        out->insert(value);
                    },
                    default_value_extractor);
            }
            else if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                auto * out = column.get();

                if (is_attribute_nullable)
                    getItemsImpl<ValueType, true>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, StringRef value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out->insertData(value.data, value.size);
                        },
                        default_value_extractor);
                else
                    getItemsImpl<ValueType, false>(
                        attribute,
                        modified_key_columns,
                        [&](size_t, StringRef value, bool)
                        {
                            out->insertData(value.data, value.size);
                        },
                        default_value_extractor);
            }
            else
            {
                auto & out = column->getData();

                if (is_attribute_nullable)
                    getItemsImpl<ValueType, true>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, const auto value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out[row] = value;
                        },
                        default_value_extractor);
                else
                    getItemsImpl<ValueType, false>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, const auto value, bool)
                        {
                            out[row] = value;
                        },
                        default_value_extractor);
            }
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (is_attribute_nullable)
        result = ColumnNullable::create(result, std::move(col_null_map_to));

    return result;
}

template <DictionaryKeyType dictionary_key_type>
template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
void RangeHashedDictionary<dictionary_key_type>::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto & attribute_container = std::get<AttributeContainerType<AttributeType>>(attribute.container);

    size_t keys_found = 0;

    const ColumnPtr & range_column = key_columns.back();
    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;
        using RangeInterval = Interval<RangeStorageType>;

        const auto * range_column_typed = typeid_cast<const RangeColumnType *>(range_column.get());
        if (!range_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
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

                interval_tree.find(date, [&](auto & interval, auto & interval_value_index)
                {
                    if (range)
                    {
                        if (likely(configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::min) && interval < *range)
                        {
                            range = interval;
                            value_index = interval_value_index;
                        }
                        else if (configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::max && interval > * range)
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
                    ++keys_found;

                    AttributeType value = attribute_container[value_index];

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

            if constexpr (is_nullable)
                set_value(key_index, default_value_extractor[key_index], default_value_extractor.isNullAt(key_index));
            else
                set_value(key_index, default_value_extractor[key_index], false);

            keys_extractor.rollbackCurrentKey();
        }
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <DictionaryKeyType dictionary_key_type>
template <typename AttributeType, bool is_nullable, typename ValueSetter>
size_t RangeHashedDictionary<dictionary_key_type>::getItemsShortCircuitImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    IColumn::Filter & default_mask) const
{
    const auto & attribute_container = std::get<AttributeContainerType<AttributeType>>(attribute.container);

    size_t keys_found = 0;

    const ColumnPtr & range_column = key_columns.back();
    auto key_columns_copy = key_columns;
    key_columns_copy.pop_back();

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns_copy, arena_holder.getComplexKeyArena());
    const size_t keys_size = keys_extractor.getKeysSize();
    default_mask.resize(keys_size);

    callOnRangeType(dict_struct.range_min->type, [&](const auto & types)
    {
        using Types = std::decay_t<decltype(types)>;
        using RangeColumnType = typename Types::LeftType;
        using RangeStorageType = typename RangeColumnType::ValueType;
        using RangeInterval = Interval<RangeStorageType>;

        const auto * range_column_typed = typeid_cast<const RangeColumnType *>(range_column.get());
        if (!range_column_typed)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
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

                interval_tree.find(date, [&](auto & interval, auto & interval_value_index)
                {
                    if (range)
                    {
                        if (likely(configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::min) && interval < *range)
                        {
                            range = interval;
                            value_index = interval_value_index;
                        }
                        else if (configuration.lookup_strategy == RangeHashedDictionaryLookupStrategy::max && interval > * range)
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

                    AttributeType value = attribute_container[value_index];

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

            keys_extractor.rollbackCurrentKey();
        }
    });

    query_count.fetch_add(keys_size, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
    return keys_found;
}

template
ColumnPtr RangeHashedDictionary<DictionaryKeyType::Simple>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultOrFilter default_or_filter) const;

template
ColumnPtr RangeHashedDictionary<DictionaryKeyType::Complex>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    DefaultOrFilter default_or_filter) const;

}
