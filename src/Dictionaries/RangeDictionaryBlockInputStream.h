#pragma once
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include <ext/range.h>
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
template <typename DictionaryType, typename RangeType, typename Key>
class RangeDictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    using DictionaryPtr = std::shared_ptr<DictionaryType const>;

    RangeDictionaryBlockInputStream(
        DictionaryPtr dictionary,
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

    template <typename DictionarySpecialAttributeType, typename T>
    void addSpecialColumn(
        const std::optional<DictionarySpecialAttributeType> & attribute,
        DataTypePtr type,
        const std::string & default_name,
        const std::unordered_set<std::string> & column_names_set,
        const PaddedPODArray<T> & values,
        ColumnsWithTypeAndName & columns,
        bool force = false) const;

    Block fillBlock(
        const PaddedPODArray<Key> & ids_to_fill,
        const PaddedPODArray<RangeType> & block_start_dates,
        const PaddedPODArray<RangeType> & block_end_dates) const;

    PaddedPODArray<Int64>
    makeDateKey(const PaddedPODArray<RangeType> & block_start_dates, const PaddedPODArray<RangeType> & block_end_dates) const;

    DictionaryPtr dictionary;
    Names column_names;
    PaddedPODArray<Key> ids;
    PaddedPODArray<RangeType> start_dates;
    PaddedPODArray<RangeType> end_dates;
};


template <typename DictionaryType, typename RangeType, typename Key>
RangeDictionaryBlockInputStream<DictionaryType, RangeType, Key>::RangeDictionaryBlockInputStream(
    DictionaryPtr dictionary_,
    size_t max_block_size_,
    const Names & column_names_,
    PaddedPODArray<Key> && ids_,
    PaddedPODArray<RangeType> && block_start_dates,
    PaddedPODArray<RangeType> && block_end_dates)
    : DictionaryBlockInputStreamBase(ids_.size(), max_block_size_)
    , dictionary(dictionary_)
    , column_names(column_names_)
    , ids(std::move(ids_))
    , start_dates(std::move(block_start_dates))
    , end_dates(std::move(block_end_dates))
{
}

template <typename DictionaryType, typename RangeType, typename Key>
Block RangeDictionaryBlockInputStream<DictionaryType, RangeType, Key>::getBlock(size_t start, size_t length) const
{
    PaddedPODArray<Key> block_ids;
    PaddedPODArray<RangeType> block_start_dates;
    PaddedPODArray<RangeType> block_end_dates;
    block_ids.reserve(length);
    block_start_dates.reserve(length);
    block_end_dates.reserve(length);

    for (auto idx : ext::range(start, start + length))
    {
        block_ids.push_back(ids[idx]);
        block_start_dates.push_back(start_dates[idx]);
        block_end_dates.push_back(end_dates[idx]);
    }

    return fillBlock(block_ids, block_start_dates, block_end_dates);
}

template <typename DictionaryType, typename RangeType, typename Key>
template <typename T>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, RangeType, Key>::getColumnFromPODArray(const PaddedPODArray<T> & array) const
{
    auto column_vector = ColumnVector<T>::create();
    column_vector->getData().reserve(array.size());
    for (T value : array)
        column_vector->insertValue(value);
    return column_vector;
}

template <typename DictionaryType, typename RangeType, typename Key>
template <typename DictionarySpecialAttributeType, typename T>
void RangeDictionaryBlockInputStream<DictionaryType, RangeType, Key>::addSpecialColumn(
    const std::optional<DictionarySpecialAttributeType> & attribute,
    DataTypePtr type,
    const std::string & default_name,
    const std::unordered_set<std::string> & column_names_set,
    const PaddedPODArray<T> & values,
    ColumnsWithTypeAndName & columns,
    bool force) const
{
    std::string name = default_name;
    if (attribute)
        name = attribute->name;

    if (force || column_names_set.find(name) != column_names_set.end())
        columns.emplace_back(getColumnFromPODArray(values), type, name);
}

template <typename DictionaryType, typename RangeType, typename Key>
PaddedPODArray<Int64> RangeDictionaryBlockInputStream<DictionaryType, RangeType, Key>::makeDateKey(
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


template <typename DictionaryType, typename RangeType, typename Key>
Block RangeDictionaryBlockInputStream<DictionaryType, RangeType, Key>::fillBlock(
    const PaddedPODArray<Key> & ids_to_fill,
    const PaddedPODArray<RangeType> & block_start_dates,
    const PaddedPODArray<RangeType> & block_end_dates) const
{
    ColumnsWithTypeAndName columns;
    const DictionaryStructure & structure = dictionary->getStructure();

    std::unordered_set<std::string> names(column_names.begin(), column_names.end());

    addSpecialColumn(structure.id, std::make_shared<DataTypeUInt64>(), "ID", names, ids_to_fill, columns, true);
    auto ids_column = columns.back().column;
    addSpecialColumn(structure.range_min, structure.range_max->type, "Range Start", names, block_start_dates, columns);
    addSpecialColumn(structure.range_max, structure.range_max->type, "Range End", names, block_end_dates, columns);

    auto date_key = makeDateKey(block_start_dates, block_end_dates);
    auto date_column = getColumnFromPODArray(date_key);

    for (const auto idx : ext::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute & attribute = structure.attributes[idx];
        if (names.find(attribute.name) != names.end())
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
