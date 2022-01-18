#include "DictionarySource.h"
#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DictionarySourceData::DictionarySourceData(
    std::shared_ptr<const IDictionary> dictionary_, PaddedPODArray<UInt64> && ids_, const Names & column_names_)
    : num_rows(ids_.size())
    , dictionary(dictionary_)
    , column_names(column_names_.begin(), column_names_.end())
    , ids(std::move(ids_))
    , key_type(DictionaryInputStreamKeyType::Id)
{
}

DictionarySourceData::DictionarySourceData(
    std::shared_ptr<const IDictionary> dictionary_,
    const PaddedPODArray<StringRef> & keys,
    const Names & column_names_)
    : num_rows(keys.size())
    , dictionary(dictionary_)
    , column_names(column_names_.begin(), column_names_.end())
    , key_type(DictionaryInputStreamKeyType::ComplexKey)
{
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    key_columns = deserializeColumnsWithTypeAndNameFromKeys(dictionary_structure, keys, 0, keys.size());
}

DictionarySourceData::DictionarySourceData(
    std::shared_ptr<const IDictionary> dictionary_,
    const Columns & data_columns_,
    const Names & column_names_,
    GetColumnsFunction && get_key_columns_function_,
    GetColumnsFunction && get_view_columns_function_)
    : num_rows(data_columns_.front()->size())
    , dictionary(dictionary_)
    , column_names(column_names_.begin(), column_names_.end())
    , data_columns(data_columns_)
    , get_key_columns_function(std::move(get_key_columns_function_))
    , get_view_columns_function(std::move(get_view_columns_function_))
    , key_type(DictionaryInputStreamKeyType::Callback)
{
}

Block DictionarySourceData::getBlock(size_t start, size_t length) const
{
    /// TODO: Rewrite
    switch (key_type)
    {
        case DictionaryInputStreamKeyType::ComplexKey:
        {
            Columns columns;
            ColumnsWithTypeAndName view_columns;
            columns.reserve(key_columns.size());
            for (const auto & key_column : key_columns)
            {
                ColumnPtr column = key_column.column->cut(start, length);
                columns.emplace_back(column);
                view_columns.emplace_back(column, key_column.type, key_column.name);
            }
            return fillBlock({}, columns, {}, std::move(view_columns));
        }

        case DictionaryInputStreamKeyType::Id:
        {
            PaddedPODArray<UInt64> ids_to_fill(ids.begin() + start, ids.begin() + start + length);
            return fillBlock(ids_to_fill, {}, {}, {});
        }

        case DictionaryInputStreamKeyType::Callback:
        {
            Columns columns;
            columns.reserve(data_columns.size());
            for (const auto & data_column : data_columns)
                columns.push_back(data_column->cut(start, length));
            const DictionaryStructure & dictionaty_structure = dictionary->getStructure();
            const auto & attributes = *dictionaty_structure.key;
            ColumnsWithTypeAndName keys_with_type_and_name = get_key_columns_function(columns, attributes);
            ColumnsWithTypeAndName view_with_type_and_name = get_view_columns_function(columns, attributes);
            DataTypes types;
            columns.clear();
            for (const auto & key_column : keys_with_type_and_name)
            {
                columns.push_back(key_column.column);
                types.push_back(key_column.type);
            }
            return fillBlock({}, columns, types, std::move(view_with_type_and_name));
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected DictionaryInputStreamKeyType.");
}

Block DictionarySourceData::fillBlock(
    const PaddedPODArray<UInt64> & ids_to_fill,
    const Columns & keys,
    const DataTypes & types,
    ColumnsWithTypeAndName && view) const
{
    DataTypes data_types = types;
    ColumnsWithTypeAndName block_columns;

    data_types.reserve(keys.size());
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    if (data_types.empty() && dictionary_structure.key)
        for (const auto & key : *dictionary_structure.key)
            data_types.push_back(key.type);

    for (const auto & column : view)
        if (column_names.find(column.name) != column_names.end())
            block_columns.push_back(column);

    const DictionaryStructure & structure = dictionary->getStructure();
    ColumnPtr ids_column = getColumnFromPODArray(ids_to_fill);

    if (structure.id && column_names.find(structure.id->name) != column_names.end())
    {
        block_columns.emplace_back(ids_column, std::make_shared<DataTypeUInt64>(), structure.id->name);
    }

    auto dictionary_key_type = dictionary->getKeyType();

    for (const auto & attribute : structure.attributes)
    {
        if (column_names.find(attribute.name) != column_names.end())
        {
            ColumnPtr column;

            if (dictionary_key_type == DictionaryKeyType::Simple)
            {
                column = dictionary->getColumn(
                    attribute.name,
                    attribute.type,
                    {ids_column},
                    {std::make_shared<DataTypeUInt64>()},
                    nullptr /* default_values_column */);
            }
            else
            {
                column = dictionary->getColumn(
                    attribute.name,
                    attribute.type,
                    keys,
                    data_types,
                    nullptr /* default_values_column*/);
            }

            block_columns.emplace_back(column, attribute.type, attribute.name);
        }
    }

    return Block(block_columns);
}

}
