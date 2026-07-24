#include <Dictionaries/RangeHashedDictionary.h>

namespace DB
{

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

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                auto * out = column.get();

                getItemsShortCircuitImpl<ValueType, false>(
                    attribute, modified_key_columns, [&](size_t, const Array & value, bool) { out->insert(value); }, default_mask);
            }
            else if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                auto * out = column.get();

                if (is_attribute_nullable)
                    getItemsShortCircuitImpl<ValueType, true>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, StringRef value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out->insertData(value.data, value.size);
                        },
                        default_mask);
                else
                    getItemsShortCircuitImpl<ValueType, false>(
                        attribute,
                        modified_key_columns,
                        [&](size_t, StringRef value, bool) { out->insertData(value.data, value.size); },
                        default_mask);
            }
            else
            {
                auto & out = column->getData();

                if (is_attribute_nullable)
                    getItemsShortCircuitImpl<ValueType, true>(
                        attribute,
                        modified_key_columns,
                        [&](size_t row, const auto value, bool is_null)
                        {
                            (*vec_null_map_to)[row] = is_null;
                            out[row] = value;
                        },
                        default_mask);
                else
                    getItemsShortCircuitImpl<ValueType, false>(
                        attribute, modified_key_columns, [&](size_t row, const auto value, bool) { out[row] = value; }, default_mask);
            }
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
