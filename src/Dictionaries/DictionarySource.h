#pragma once

#include <memory>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/DictionarySourceBase.h>


namespace DB
{

class DictionarySourceData
{
public:
    DictionarySourceData(
        std::shared_ptr<const IDictionary> dictionary,
        PaddedPODArray<UInt64> && ids,
        const Names & column_names);

    DictionarySourceData(
        std::shared_ptr<const IDictionary> dictionary,
        const PaddedPODArray<StringRef> & keys,
        const Names & column_names);

    using GetColumnsFunction = std::function<ColumnsWithTypeAndName(const Columns &, const std::vector<DictionaryAttribute> & attributes)>;

    // Used to separate key columns format for storage and view.
    // Calls get_key_columns_function to get key column for dictionary get function call
    // and get_view_columns_function to get key representation.
    // Now used in trie dictionary, where columns are stored as ip and mask, and are showed as string
    DictionarySourceData(
        std::shared_ptr<const IDictionary> dictionary,
        const Columns & data_columns,
        const Names & column_names,
        GetColumnsFunction && get_key_columns_function,
        GetColumnsFunction && get_view_columns_function);

    Block getBlock(size_t start, size_t length) const;
    size_t getNumRows() const { return num_rows; }

private:
    Block fillBlock(
        const PaddedPODArray<UInt64> & ids_to_fill,
        const Columns & keys,
        const DataTypes & types,
        ColumnsWithTypeAndName && view) const;

    const size_t num_rows;
    std::shared_ptr<const IDictionary> dictionary;
    std::unordered_set<std::string> column_names;
    PaddedPODArray<UInt64> ids;
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

class DictionarySource final : public DictionarySourceBase
{
public:
    DictionarySource(DictionarySourceData data_, UInt64 max_block_size)
        : DictionarySourceBase(data_.getBlock(0, 0), data_.getNumRows(), max_block_size)
        , data(std::move(data_))
    {}

    String getName() const override { return "DictionarySource"; }
    Block getBlock(size_t start, size_t length) const override { return data.getBlock(start, length); }

    DictionarySourceData data;
};

}
