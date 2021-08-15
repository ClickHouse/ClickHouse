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

template <typename RangeType>
class RangeDictionarySourceData
{
public:
    using Key = UInt64;

    RangeDictionarySourceData(
        std::shared_ptr<const IDictionary> dictionary,
        const Names & column_names,
        PaddedPODArray<Key> && ids_to_fill,
        PaddedPODArray<RangeType> && start_dates,
        PaddedPODArray<RangeType> && end_dates);

    Block getBlock(size_t start, size_t length) const;
    size_t getNumRows() const { return ids.size(); }

private:

    Block fillBlock(
        const PaddedPODArray<Key> & ids_to_fill,
        const PaddedPODArray<RangeType> & block_start_dates,
        const PaddedPODArray<RangeType> & block_end_dates) const;

    PaddedPODArray<Int64> makeDateKey(
        const PaddedPODArray<RangeType> & block_start_dates,
        const PaddedPODArray<RangeType> & block_end_dates) const;

    std::shared_ptr<const IDictionary> dictionary;
    NameSet column_names;
    PaddedPODArray<Key> ids;
    PaddedPODArray<RangeType> start_dates;
    PaddedPODArray<RangeType> end_dates;
};


template <typename RangeType>
RangeDictionarySourceData<RangeType>::RangeDictionarySourceData(
    std::shared_ptr<const IDictionary> dictionary_,
    const Names & column_names_,
    PaddedPODArray<Key> && ids_,
    PaddedPODArray<RangeType> && block_start_dates,
    PaddedPODArray<RangeType> && block_end_dates)
    : dictionary(dictionary_)
    , column_names(column_names_.begin(), column_names_.end())
    , ids(std::move(ids_))
    , start_dates(std::move(block_start_dates))
    , end_dates(std::move(block_end_dates))
{
}

template <typename RangeType>
Block RangeDictionarySourceData<RangeType>::getBlock(size_t start, size_t length) const
{
    PaddedPODArray<Key> block_ids;
    PaddedPODArray<RangeType> block_start_dates;
    PaddedPODArray<RangeType> block_end_dates;
    block_ids.reserve(length);
    block_start_dates.reserve(length);
    block_end_dates.reserve(length);

    for (auto idx : collections::range(start, start + length))
    {
        block_ids.push_back(ids[idx]);
        block_start_dates.push_back(start_dates[idx]);
        block_end_dates.push_back(end_dates[idx]);
    }

    return fillBlock(block_ids, block_start_dates, block_end_dates);
}

template <typename RangeType>
PaddedPODArray<Int64> RangeDictionarySourceData<RangeType>::makeDateKey(
    const PaddedPODArray<RangeType> & block_start_dates, const PaddedPODArray<RangeType> & block_end_dates) const
{
    PaddedPODArray<Int64> key(block_start_dates.size());
    for (size_t i = 0; i < key.size(); ++i)
    {
        if (RangeHashedDictionary::Range::isCorrectDate(block_start_dates[i]))
            key[i] = block_start_dates[i];
        else
            key[i] = block_end_dates[i];
    }

    return key;
}


template <typename RangeType>
Block RangeDictionarySourceData<RangeType>::fillBlock(
    const PaddedPODArray<Key> & ids_to_fill,
    const PaddedPODArray<RangeType> & block_start_dates,
    const PaddedPODArray<RangeType> & block_end_dates) const
{
    ColumnsWithTypeAndName columns;
    const DictionaryStructure & structure = dictionary->getStructure();

    auto ids_column = getColumnFromPODArray(ids_to_fill);
    const std::string & id_column_name = structure.id->name;
    if (column_names.find(id_column_name) != column_names.end())
        columns.emplace_back(ids_column, std::make_shared<DataTypeUInt64>(), id_column_name);

    auto date_key = makeDateKey(block_start_dates, block_end_dates);
    auto date_column = getColumnFromPODArray(date_key);

    const std::string & range_min_column_name = structure.range_min->name;
    if (column_names.find(range_min_column_name) != column_names.end())
    {
        auto range_min_column = getColumnFromPODArray(block_start_dates);
        columns.emplace_back(range_min_column, structure.range_max->type, range_min_column_name);
    }

    const std::string & range_max_column_name = structure.range_max->name;
    if (column_names.find(range_max_column_name) != column_names.end())
    {
        auto range_max_column = getColumnFromPODArray(block_end_dates);
        columns.emplace_back(range_max_column, structure.range_max->type, range_max_column_name);
    }

    for (const auto idx : collections::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute & attribute = structure.attributes[idx];
        if (column_names.find(attribute.name) != column_names.end())
        {
            ColumnPtr column = dictionary->getColumn(
                attribute.name,
                attribute.type,
                {ids_column, date_column},
                {std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeInt64>()},
                nullptr);
            columns.emplace_back(column, attribute.type, attribute.name);
        }
    }
    return Block(columns);
}

/*
 * BlockInputStream implementation for external dictionaries
 * read() returns single block consisting of the in-memory contents of the dictionaries
 */
template <typename RangeType>
class RangeDictionarySource : public DictionarySourceBase
{
public:
    using Key = UInt64;

    RangeDictionarySource(RangeDictionarySourceData<RangeType> data_, size_t max_block_size);

    String getName() const override { return "RangeDictionarySource"; }

protected:
    Block getBlock(size_t start, size_t length) const override;

    RangeDictionarySourceData<RangeType> data;
};

template <typename RangeType>
RangeDictionarySource<RangeType>::RangeDictionarySource(RangeDictionarySourceData<RangeType> data_, size_t max_block_size)
    : DictionarySourceBase(data_.getBlock(0, 0), data_.getNumRows(), max_block_size)
    , data(std::move(data_))
{
}

template <typename RangeType>
Block RangeDictionarySource<RangeType>::getBlock(size_t start, size_t length) const
{
    return data.getBlock(start, length);
}

}
