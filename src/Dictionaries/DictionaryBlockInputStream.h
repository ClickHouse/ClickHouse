#pragma once

#include <memory>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <common/logger_useful.h>
#include <ext/range.h>
#include "DictionaryBlockInputStreamBase.h"
#include "DictionaryStructure.h"
#include "IDictionary.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/* BlockInputStream implementation for external dictionaries
 * read() returns blocks consisting of the in-memory contents of the dictionaries
 */
template <typename Key>
class DictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    DictionaryBlockInputStream(
        std::shared_ptr<const IDictionaryBase> dictionary, UInt64 max_block_size, PaddedPODArray<Key> && ids, const Names & column_names);

    DictionaryBlockInputStream(
        std::shared_ptr<const IDictionaryBase> dictionary,
        UInt64 max_block_size,
        const std::vector<StringRef> & keys,
        const Names & column_names);

    using GetColumnsFunction = std::function<ColumnsWithTypeAndName(const Columns &, const std::vector<DictionaryAttribute> & attributes)>;

    // Used to separate key columns format for storage and view.
    // Calls get_key_columns_function to get key column for dictionary get function call
    // and get_view_columns_function to get key representation.
    // Now used in trie dictionary, where columns are stored as ip and mask, and are showed as string
    DictionaryBlockInputStream(
        std::shared_ptr<const IDictionaryBase> dictionary,
        UInt64 max_block_size,
        const Columns & data_columns,
        const Names & column_names,
        GetColumnsFunction && get_key_columns_function,
        GetColumnsFunction && get_view_columns_function);

    String getName() const override { return "Dictionary"; }

protected:
    Block getBlock(size_t start, size_t size) const override;

private:
    Block
    fillBlock(const PaddedPODArray<Key> & ids_to_fill, const Columns & keys, const DataTypes & types, ColumnsWithTypeAndName && view) const;

    ColumnPtr getColumnFromIds(const PaddedPODArray<Key> & ids_to_fill) const;

    void fillKeyColumns(
        const std::vector<StringRef> & keys,
        size_t start,
        size_t size,
        const DictionaryStructure & dictionary_structure,
        ColumnsWithTypeAndName & columns) const;

    std::shared_ptr<const IDictionaryBase> dictionary;
    Names column_names;
    PaddedPODArray<Key> ids;
    ColumnsWithTypeAndName key_columns;

    Columns data_columns;
    GetColumnsFunction get_key_columns_function;
    GetColumnsFunction get_view_columns_function;

    enum class DictionaryInputStreamKeyType
    {
        Id,
        ComplexKey,
        Callback
    };

    DictionaryInputStreamKeyType key_type;
};


template <typename Key>
DictionaryBlockInputStream<Key>::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionaryBase> dictionary_, UInt64 max_block_size_, PaddedPODArray<Key> && ids_, const Names & column_names_)
    : DictionaryBlockInputStreamBase(ids_.size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_)
    , ids(std::move(ids_))
    , key_type(DictionaryInputStreamKeyType::Id)
{
}

template <typename Key>
DictionaryBlockInputStream<Key>::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionaryBase> dictionary_,
    UInt64 max_block_size_,
    const std::vector<StringRef> & keys,
    const Names & column_names_)
    : DictionaryBlockInputStreamBase(keys.size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_)
    , key_type(DictionaryInputStreamKeyType::ComplexKey)
{
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    fillKeyColumns(keys, 0, keys.size(), dictionary_structure, key_columns);
}

template <typename Key>
DictionaryBlockInputStream<Key>::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionaryBase> dictionary_,
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


template <typename Key>
Block DictionaryBlockInputStream<Key>::getBlock(size_t start, size_t length) const
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
            PaddedPODArray<Key> ids_to_fill(ids.begin() + start, ids.begin() + start + length);
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

    throw Exception("Unexpected DictionaryInputStreamKeyType.", ErrorCodes::LOGICAL_ERROR);
}

template <typename Key>
Block DictionaryBlockInputStream<Key>::fillBlock(
    const PaddedPODArray<Key> & ids_to_fill, const Columns & keys, const DataTypes & types, ColumnsWithTypeAndName && view) const
{
    std::unordered_set<std::string> names(column_names.begin(), column_names.end());

    DataTypes data_types = types;
    ColumnsWithTypeAndName block_columns;

    data_types.reserve(keys.size());
    const DictionaryStructure & dictionaty_structure = dictionary->getStructure();
    if (data_types.empty() && dictionaty_structure.key)
        for (const auto & key : *dictionaty_structure.key)
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

    for (const auto idx : ext::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute & attribute = structure.attributes[idx];
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

template <typename Key>
ColumnPtr DictionaryBlockInputStream<Key>::getColumnFromIds(const PaddedPODArray<Key> & ids_to_fill) const
{
    auto column_vector = ColumnVector<UInt64>::create();
    column_vector->getData().reserve(ids_to_fill.size());
    for (UInt64 id : ids_to_fill)
        column_vector->insertValue(id);
    return column_vector;
}


template <typename Key>
void DictionaryBlockInputStream<Key>::fillKeyColumns(
    const std::vector<StringRef> & keys,
    size_t start,
    size_t size,
    const DictionaryStructure & dictionary_structure,
    ColumnsWithTypeAndName & res) const
{
    MutableColumns columns;
    columns.reserve(dictionary_structure.key->size());

    for (const DictionaryAttribute & attribute : *dictionary_structure.key)
        columns.emplace_back(attribute.type->createColumn());

    for (auto idx : ext::range(start, size))
    {
        const auto & key = keys[idx];
        auto ptr = key.data;
        for (auto & column : columns)
            ptr = column->deserializeAndInsertFromArena(ptr);
    }

    for (size_t i = 0, num_columns = columns.size(); i < num_columns; ++i)
        res.emplace_back(
            ColumnWithTypeAndName{std::move(columns[i]), (*dictionary_structure.key)[i].type, (*dictionary_structure.key)[i].name});
}

}
