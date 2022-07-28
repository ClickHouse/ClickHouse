#pragma once
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include <common/range.h>
#include "DictionaryBlockInputStreamBase.h"
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "RangeHashedDictionary.h"

namespace DB
{
/*
 * BlockInputStream implementation for external dictionaries
 * read() returns single block consisting of the in-memory contents of the dictionaries
 */
template <typename RangeType>
class RangeDictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    using Key = UInt64;

    RangeDictionaryBlockInputStream(
        std::shared_ptr<const IDictionary> dictionary,
        size_t max_block_size,
        const Names & column_names,
        PaddedPODArray<Key> && ids_to_fill,
        PaddedPODArray<RangeType> && start_dates,
        PaddedPODArray<RangeType> && end_dates);

    String getName() const override { return "RangeDictionary"; }

protected:
    Block getBlock(size_t start, size_t length) const override;

private:
    template <typename T>
    ColumnPtr getColumnFromPODArray(const PaddedPODArray<T> & array) const;

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
RangeDictionaryBlockInputStream<RangeType>::RangeDictionaryBlockInputStream(
    std::shared_ptr<const IDictionary> dictionary_,
    size_t max_block_size_,
    const Names & column_names_,
    PaddedPODArray<Key> && ids_,
    PaddedPODArray<RangeType> && block_start_dates,
    PaddedPODArray<RangeType> && block_end_dates)
    : DictionaryBlockInputStreamBase(ids_.size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_.begin(), column_names_.end())
    , ids(std::move(ids_))
    , start_dates(std::move(block_start_dates))
    , end_dates(std::move(block_end_dates))
{
}

template <typename RangeType>
Block RangeDictionaryBlockInputStream<RangeType>::getBlock(size_t start, size_t length) const
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
template <typename T>
ColumnPtr RangeDictionaryBlockInputStream<RangeType>::getColumnFromPODArray(const PaddedPODArray<T> & array) const
{
    auto column_vector = ColumnVector<T>::create();
    column_vector->getData().reserve(array.size());
    column_vector->getData().insert(array.begin(), array.end());

    return column_vector;
}

template <typename RangeType>
PaddedPODArray<Int64> RangeDictionaryBlockInputStream<RangeType>::makeDateKey(
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
Block RangeDictionaryBlockInputStream<RangeType>::fillBlock(
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

}
