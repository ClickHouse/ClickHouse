#include <Dictionaries/IDictionary.h>
#include <Interpreters/JoinUtils.h>


namespace DB
{

static void splitNamesAndTypesFromStructure(const DictionaryStructure & structure, const Names & result_names, Names & attribute_names, DataTypes & result_types)
{
    if (!result_names.empty())
    {
        for (const auto & attr_name : result_names)
        {
            if (!structure.hasAttribute(attr_name))
                continue; /// skip keys
            const auto & attr = structure.getAttribute(attr_name);
            attribute_names.emplace_back(attr.name);
            result_types.emplace_back(attr.type);
        }
    }
    else
    {
        /// If result_names is empty, then use all attributes from structure
        for (const auto & attr : structure.attributes)
        {
            attribute_names.emplace_back(attr.name);
            result_types.emplace_back(attr.type);
        }
    }
}

Names IDictionary::getPrimaryKey() const
{
    return getStructure().getKeysNames();
}

Chunk IDictionary::getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & out_null_map, const Names & result_names) const
{
    if (keys.empty())
        return Chunk(getSampleBlock(result_names).cloneEmpty().getColumns(), 0);

    const auto & dictionary_structure = getStructure();

    /// Split column keys and types into separate vectors, to use in `IDictionary::getColumns`
    Columns key_columns;
    DataTypes key_types;
    for (const auto & key : keys)
    {
        key_columns.emplace_back(key.column);
        key_types.emplace_back(key.type);
    }

    /// Fill null map
    {
        out_null_map.clear();

        auto mask = hasKeys(key_columns, key_types);
        const auto & mask_data = mask->getData();

        out_null_map.resize(mask_data.size(), 0);
        std::copy(mask_data.begin(), mask_data.end(), out_null_map.begin());
    }

    Names attribute_names;
    DataTypes result_types;
    splitNamesAndTypesFromStructure(dictionary_structure, result_names, attribute_names, result_types);

    Columns default_cols(result_types.size());
    for (size_t i = 0; i < result_types.size(); ++i)
        /// Dictinonary may have non-standart default values specified
        default_cols[i] = result_types[i]->createColumnConstWithDefaultValue(out_null_map.size());

    Columns result_columns = getColumns(attribute_names, result_types, key_columns, key_types, default_cols);

    /// Result block should consist of key columns and then attributes
    for (const auto & key_col : key_columns)
    {
        /// Insert default values for keys that were not found
        ColumnPtr filtered_key_col = JoinCommon::filterWithBlanks(key_col, out_null_map);
        result_columns.insert(result_columns.begin(), filtered_key_col);
    }

    size_t num_rows = result_columns[0]->size();
    return Chunk(std::move(result_columns), num_rows);
}

Block IDictionary::getSampleBlock(const Names & result_names) const
{
    const auto & dictionary_structure = getStructure();
    const auto & key_types = dictionary_structure.getKeyTypes();
    const auto & key_names = dictionary_structure.getKeysNames();

    Block sample_block;

    for (size_t i = 0; i < key_types.size(); ++i)
        sample_block.insert(ColumnWithTypeAndName(nullptr, key_types.at(i), key_names.at(i)));

    if (result_names.empty())
    {
        for (const auto & attr : dictionary_structure.attributes)
            sample_block.insert(ColumnWithTypeAndName(nullptr, attr.type, attr.name));
    }
    else
    {
        for (const auto & attr_name : result_names)
        {
            if (!dictionary_structure.hasAttribute(attr_name))
                continue; /// skip keys
            const auto & attr = dictionary_structure.getAttribute(attr_name);
            sample_block.insert(ColumnWithTypeAndName(nullptr, attr.type, attr_name));
        }
    }
    return sample_block;
}

}
