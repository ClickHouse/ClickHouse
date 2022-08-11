#include <Dictionaries/IDictionary.h>

namespace DB
{

void IDictionary::convertKeyColumns(Columns & key_columns, DataTypes & key_types) const
{
    const auto & dictionary_structure = getStructure();
    auto key_attributes_types = dictionary_structure.getKeyTypes();
    size_t key_attributes_types_size = key_attributes_types.size();
    size_t key_types_size = key_types.size();

    if (key_types_size != key_attributes_types_size)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Dictionary {} key lookup structure does not match, expected {}",
            getFullName(),
            dictionary_structure.getKeyDescription());

    for (size_t key_attribute_type_index = 0; key_attribute_type_index < key_attributes_types_size; ++key_attribute_type_index)
    {
        const auto & key_attribute_type = key_attributes_types[key_attribute_type_index];
        auto & key_type = key_types[key_attribute_type_index];

        if (key_attribute_type->equals(*key_type))
            continue;

        auto & key_column_to_cast = key_columns[key_attribute_type_index];
        ColumnWithTypeAndName column_to_cast = {key_column_to_cast, key_type, ""};
        auto casted_column = castColumnAccurate(column_to_cast, key_attribute_type);
        key_column_to_cast = std::move(casted_column);
        key_type = key_attribute_type;
    }
}

Columns IDictionary::getColumns(
    const Strings & attribute_names,
    const DataTypes & result_types,
    const Columns & key_columns,
    const DataTypes & key_types,
    const Columns & default_values_columns) const
{
    size_t attribute_names_size = attribute_names.size();

    Columns result;
    result.reserve(attribute_names_size);

    for (size_t i = 0; i < attribute_names_size; ++i)
    {
        const auto & attribute_name = attribute_names[i];
        const auto & result_type = result_types[i];
        const auto & default_values_column = default_values_columns[i];

        result.emplace_back(getColumn(attribute_name, result_type, key_columns, key_types, default_values_column));
    }

    return result;
}

Names IDictionary::getPrimaryKey() const
{
    return getStructure().getKeysNames();
}

Block IDictionary::getColumns(const Names & attribute_names, const ColumnsWithTypeAndName & key_columns, PaddedPODArray<UInt8> & found_keys_map) const
{
    size_t attribute_names_size = attribute_names.size();

    Strings required_attributes_names;
    DataTypes attributes_types;
    Columns attributes_default_columns;
    Block attributes_header;

    const auto & dictionary_structure = getStructure();
    size_t key_columns_size = key_columns.size();

    if (attribute_names.empty())
    {
        size_t dictionary_attributes_size = dictionary_structure.attributes.size();
        attributes_types.reserve(dictionary_attributes_size);
        attributes_default_columns.reserve(dictionary_attributes_size);
        required_attributes_names.reserve(dictionary_attributes_size);

        /// If attribute names is empty, then use all attributes from dictionary structure
        for (const auto & attribute : dictionary_structure.attributes)
        {
            attributes_types.emplace_back(attribute.type);
            required_attributes_names.emplace_back(attribute.name);
            attributes_default_columns.emplace_back(attribute.type->createColumnConst(key_columns_size, attribute.null_value));
            attributes_header.insert(ColumnWithTypeAndName(nullptr, attribute.type, attribute.name));
        }
    }
    else
    {
        attributes_types.reserve(attribute_names_size);
        attributes_default_columns.reserve(attribute_names_size);
        required_attributes_names.reserve(attribute_names_size);

        for (const auto & attribute_name : attribute_names)
        {
            if (!dictionary_structure.hasAttribute(attribute_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Dictionary {} does not have attribute with name {}",
                    getFullName(),
                    attribute_name);

            const auto & attribute = dictionary_structure.getAttribute(attribute_name);
            attributes_types.emplace_back(attribute.type);
            required_attributes_names.emplace_back(attribute.name);
            attributes_default_columns.emplace_back(attribute.type->createColumnConst(key_columns_size, attribute.null_value));
            attributes_header.insert(ColumnWithTypeAndName(nullptr, attribute.type, attribute.name));
        }
    }

    if (key_columns.empty())
        return attributes_header;

    Columns key_columns_values;
    key_columns_values.reserve(key_columns_size);

    DataTypes key_types_values;
    key_columns_values.reserve(key_columns_size);

    for (const auto & key_column : key_columns)
    {
        key_columns_values.push_back(key_column.column);
        key_types_values.push_back(key_column.type);
    }

    /// Fill found keys map
    auto mask = hasKeys(key_columns_values, key_types_values);
    const auto & mask_data = mask->getData();
    found_keys_map.assign(mask_data);

    size_t attributes_types_size = attributes_types.size();
    Columns default_columns(attributes_types_size);

    for (size_t i = 0; i < attribute_names_size; ++i)
        default_columns[i] = attributes_types[i]->createColumnConstWithDefaultValue(key_columns.size());

    Columns result_columns = getColumns(attribute_names, attributes_types, key_columns_values, key_types_values, default_columns);

    /// Result block should consist of key columns and then attributes
    for (const auto & key_column_with_type : key_columns)
    {
        /// Insert default values for keys that were not found
        ColumnPtr filtered_key_col = JoinCommon::filterWithBlanks(key_column_with_type.column, found_keys_map);
        result_columns.insert(result_columns.begin(), filtered_key_col);
    }

    return attributes_header.cloneWithColumns(result_columns);
}


}
