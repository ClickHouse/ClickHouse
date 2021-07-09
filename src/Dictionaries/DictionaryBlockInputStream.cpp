#include "DictionaryBlockInputStream.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

DictionaryBlockInputStream::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionary> dictionary_, UInt64 max_block_size_, PaddedPODArray<UInt64> && ids_, const Names & column_names_)
    : DictionaryBlockInputStreamBase(ids_.size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_)
    , ids(std::move(ids_))
    , key_type(DictionaryInputStreamKeyType::Id)
{
}

DictionaryBlockInputStream::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionary> dictionary_,
    UInt64 max_block_size_,
    const PaddedPODArray<StringRef> & keys,
    const Names & column_names_)
    : DictionaryBlockInputStreamBase(keys.size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_)
    , key_type(DictionaryInputStreamKeyType::ComplexKey)
{
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    fillKeyColumns(keys, 0, keys.size(), dictionary_structure, key_columns);
}

DictionaryBlockInputStream::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionary> dictionary_,
    UInt64 max_block_size_,
    const Columns & data_columns_,
    const Names & column_names_,
    GetColumnsFunction && get_key_columns_function_,
    GetColumnsFunction && get_view_columns_function_)
    : DictionaryBlockInputStreamBase(data_columns_.front()->size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_)
    , data_columns(data_columns_)
    , get_key_columns_function(std::move(get_key_columns_function_))
    , get_view_columns_function(std::move(get_view_columns_function_))
    , key_type(DictionaryInputStreamKeyType::Callback)
{
}

Block DictionaryBlockInputStream::getBlock(size_t start, size_t length) const
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

Block DictionaryBlockInputStream::fillBlock(
    const PaddedPODArray<UInt64> & ids_to_fill,
    const Columns & keys,
    const DataTypes & types,
    ColumnsWithTypeAndName && view) const
{
    std::unordered_set<std::string> names(column_names.begin(), column_names.end());

    DataTypes data_types = types;
    ColumnsWithTypeAndName block_columns;

    data_types.reserve(keys.size());
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    if (data_types.empty() && dictionary_structure.key)
        for (const auto & key : *dictionary_structure.key)
            data_types.push_back(key.type);

    for (const auto & column : view)
        if (names.find(column.name) != names.end())
            block_columns.push_back(column);

    const DictionaryStructure & structure = dictionary->getStructure();
    ColumnPtr ids_column = getColumnFromIds(ids_to_fill);

    if (structure.id && names.find(structure.id->name) != names.end())
    {
        block_columns.emplace_back(ids_column, std::make_shared<DataTypeUInt64>(), structure.id->name);
    }

    auto dictionary_key_type = dictionary->getKeyType();

    for (const auto & attribute : structure.attributes)
    {
        if (names.find(attribute.name) != names.end())
        {
            ColumnPtr column;

            if (dictionary_key_type == DictionaryKeyType::simple)
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

ColumnPtr DictionaryBlockInputStream::getColumnFromIds(const PaddedPODArray<UInt64> & ids_to_fill)
{
    auto column_vector = ColumnVector<UInt64>::create();
    column_vector->getData().assign(ids_to_fill);
    return column_vector;
}

void DictionaryBlockInputStream::fillKeyColumns(
    const PaddedPODArray<StringRef> & keys,
    size_t start,
    size_t size,
    const DictionaryStructure & dictionary_structure,
    ColumnsWithTypeAndName & result)
{
    MutableColumns columns;
    columns.reserve(dictionary_structure.key->size());

    for (const DictionaryAttribute & attribute : *dictionary_structure.key)
        columns.emplace_back(attribute.type->createColumn());

    for (size_t index = start; index < size; ++index)
    {
        const auto & key = keys[index];
        const auto *ptr = key.data;
        for (auto & column : columns)
            ptr = column->deserializeAndInsertFromArena(ptr);
    }

    for (size_t i = 0, num_columns = columns.size(); i < num_columns; ++i)
    {
        const auto & dictionary_attribute = (*dictionary_structure.key)[i];
        result.emplace_back(ColumnWithTypeAndName{std::move(columns[i]), dictionary_attribute.type, dictionary_attribute.name});
    }
}

}
