#pragma once
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/DictionarySourceBase.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/RangeHashedDictionary.h>


namespace DB
{

template <DictionaryKeyType dictionary_key_type, typename RangeType>
class RangeDictionarySourceData
{
public:

    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    RangeDictionarySourceData(
        std::shared_ptr<const IDictionary> dictionary,
        const Names & column_names,
        PaddedPODArray<KeyType> && keys,
        PaddedPODArray<RangeType> && start_dates,
        PaddedPODArray<RangeType> && end_dates);

    Block getBlock(size_t start, size_t length) const;
    size_t getNumRows() const { return keys.size(); }

private:

    Block fillBlock(
        const PaddedPODArray<KeyType> & keys_to_fill,
        const PaddedPODArray<RangeType> & block_start_dates,
        const PaddedPODArray<RangeType> & block_end_dates,
        size_t start,
        size_t end) const;

    PaddedPODArray<Int64> makeDateKeys(
        const PaddedPODArray<RangeType> & block_start_dates,
        const PaddedPODArray<RangeType> & block_end_dates) const;

    std::shared_ptr<const IDictionary> dictionary;
    NameSet column_names;
    PaddedPODArray<KeyType> keys;
    PaddedPODArray<RangeType> start_dates;
    PaddedPODArray<RangeType> end_dates;
};


template <DictionaryKeyType dictionary_key_type, typename RangeType>
RangeDictionarySourceData<dictionary_key_type, RangeType>::RangeDictionarySourceData(
    std::shared_ptr<const IDictionary> dictionary_,
    const Names & column_names_,
    PaddedPODArray<KeyType> && keys,
    PaddedPODArray<RangeType> && block_start_dates,
    PaddedPODArray<RangeType> && block_end_dates)
    : dictionary(dictionary_)
    , column_names(column_names_.begin(), column_names_.end())
    , keys(std::move(keys))
    , start_dates(std::move(block_start_dates))
    , end_dates(std::move(block_end_dates))
{
}

template <DictionaryKeyType dictionary_key_type, typename RangeType>
Block RangeDictionarySourceData<dictionary_key_type, RangeType>::getBlock(size_t start, size_t length) const
{
    PaddedPODArray<KeyType> block_keys;
    PaddedPODArray<RangeType> block_start_dates;
    PaddedPODArray<RangeType> block_end_dates;
    block_keys.reserve(length);
    block_start_dates.reserve(length);
    block_end_dates.reserve(length);

    for (size_t index = start; index < start + length; ++index)
    {
        block_keys.push_back(keys[index]);
        block_start_dates.push_back(start_dates[index]);
        block_end_dates.push_back(end_dates[index]);
    }

    return fillBlock(block_keys, block_start_dates, block_end_dates, start, start + length);
}

template <DictionaryKeyType dictionary_key_type, typename RangeType>
PaddedPODArray<Int64> RangeDictionarySourceData<dictionary_key_type, RangeType>::makeDateKeys(
    const PaddedPODArray<RangeType> & block_start_dates,
    const PaddedPODArray<RangeType> & block_end_dates) const
{
    PaddedPODArray<Int64> keys(block_start_dates.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (Range::isCorrectDate(block_start_dates[i]))
            keys[i] = block_start_dates[i];
        else
            keys[i] = block_end_dates[i];
    }

    return keys;
}


template <DictionaryKeyType dictionary_key_type, typename RangeType>
Block RangeDictionarySourceData<dictionary_key_type, RangeType>::fillBlock(
    const PaddedPODArray<KeyType> & keys_to_fill,
    const PaddedPODArray<RangeType> & block_start_dates,
    const PaddedPODArray<RangeType> & block_end_dates,
    size_t start,
    size_t end) const
{
    ColumnsWithTypeAndName columns;
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();

    DataTypes keys_types;
    Columns keys_columns;
    Strings keys_names = dictionary_structure.getKeysNames();

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        keys_columns = {getColumnFromPODArray(keys_to_fill)};
        keys_types = {std::make_shared<DataTypeUInt64>()};
    }
    else
    {
        for (const auto & attribute : *dictionary_structure.key)
            keys_types.emplace_back(attribute.type);

        auto deserialized_columns = deserializeColumnsFromKeys(dictionary_structure, keys, start, end);
        for (auto & deserialized_column : deserialized_columns)
            keys_columns.emplace_back(std::move(deserialized_column));
    }

    size_t keys_size = keys_names.size();

    assert(keys_columns.size() == keys_size);
    assert(keys_types.size() == keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        auto & key_name = keys_names[i];

        if (column_names.find(key_name) != column_names.end())
           columns.emplace_back(keys_columns[i], keys_types[i], key_name);
    }

    auto date_key = makeDateKeys(block_start_dates, block_end_dates);
    auto date_column = getColumnFromPODArray(date_key);
    keys_columns.emplace_back(std::move(date_column));
    keys_types.emplace_back(std::make_shared<DataTypeInt64>());

    const auto & range_min_column_name = dictionary_structure.range_min->name;
    if (column_names.find(range_min_column_name) != column_names.end())
    {
        auto range_min_column = getColumnFromPODArray(block_start_dates);
        columns.emplace_back(range_min_column, dictionary_structure.range_max->type, range_min_column_name);
    }

    const auto & range_max_column_name = dictionary_structure.range_max->name;
    if (column_names.find(range_max_column_name) != column_names.end())
    {
        auto range_max_column = getColumnFromPODArray(block_end_dates);
        columns.emplace_back(range_max_column, dictionary_structure.range_max->type, range_max_column_name);
    }

    size_t attributes_size = dictionary_structure.attributes.size();
    for (size_t attribute_index = 0; attribute_index < attributes_size; ++attribute_index)
    {
        const auto & attribute = dictionary_structure.attributes[attribute_index];
        if (column_names.find(attribute.name) == column_names.end())
            continue;

        auto column = dictionary->getColumn(
            attribute.name,
            attribute.type,
            keys_columns,
            keys_types,
            nullptr /* default_values_column*/);

        columns.emplace_back(std::move(column), attribute.type, attribute.name);
    }

    return Block(columns);
}

template <DictionaryKeyType dictionary_key_type, typename RangeType>
class RangeDictionarySource : public DictionarySourceBase
{
public:

    RangeDictionarySource(RangeDictionarySourceData<dictionary_key_type, RangeType> data_, size_t max_block_size);

    String getName() const override { return "RangeDictionarySource"; }

protected:
    Block getBlock(size_t start, size_t length) const override;

    RangeDictionarySourceData<dictionary_key_type, RangeType> data;
};

template <DictionaryKeyType dictionary_key_type, typename RangeType>
RangeDictionarySource<dictionary_key_type, RangeType>::RangeDictionarySource(RangeDictionarySourceData<dictionary_key_type, RangeType> data_, size_t max_block_size)
    : DictionarySourceBase(data_.getBlock(0, 0), data_.getNumRows(), max_block_size)
    , data(std::move(data_))
{
}

template <DictionaryKeyType dictionary_key_type, typename RangeType>
Block RangeDictionarySource<dictionary_key_type, RangeType>::getBlock(size_t start, size_t length) const
{
    return data.getBlock(start, length);
}

}
