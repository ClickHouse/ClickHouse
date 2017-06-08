#pragma once
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <Dictionaries/DictionaryBlockInputStreamBase.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <ext/range.h>

namespace DB
{

/*
 * BlockInputStream implementation for external dictionaries
 * read() returns single block consisting of the in-memory contents of the dictionaries
 */
template <class DictionaryType, class Key>
class RangeDictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    using DictionatyPtr = std::shared_ptr<DictionaryType const>;

    RangeDictionaryBlockInputStream(
        DictionatyPtr dictionary, size_t max_block_size, const Names & column_names, PaddedPODArray<Key> && ids,
        PaddedPODArray<UInt16> && start_dates, PaddedPODArray<UInt16> && end_dates);

    String getName() const override {
        return "RangeDictionaryBlockInputStream";
    }

protected:
    Block getBlock(size_t start, size_t length) const override;

private:
    template <class Type>
    using DictionaryGetter = void (DictionaryType::*)(const std::string &, const PaddedPODArray<Key> &,
                             const PaddedPODArray<UInt16> &, PaddedPODArray<Type> &) const;

    template <class AttributeType>
    ColumnPtr getColumnFromAttribute(DictionaryGetter<AttributeType> getter,
                                     const PaddedPODArray<Key>& ids, const PaddedPODArray<UInt16> & dates,
                                     const DictionaryAttribute& attribute, const DictionaryType& dictionary) const;
    ColumnPtr getColumnFromAttributeString(const PaddedPODArray<Key>& ids, const PaddedPODArray<UInt16> & dates,
                                           const DictionaryAttribute& attribute, const DictionaryType& dictionary) const;
    template <class T>
    ColumnPtr getColumnFromPODArray(const PaddedPODArray<T>& array) const;

    template <class T>
    void addSpecialColumn(
        const std::experimental::optional<DictionarySpecialAttribute>& attribute, DataTypePtr type,
        const std::string & default_name, const std::unordered_set<std::string> & column_names,
        const PaddedPODArray<T> & values, ColumnsWithTypeAndName& columns) const;

    Block fillBlock(const PaddedPODArray<Key> & ids,
                    const PaddedPODArray<UInt16> & start_dates, const PaddedPODArray<UInt16> & end_dates) const;

    DictionatyPtr dictionary;
    Names column_names;
    PaddedPODArray<Key> ids;
    PaddedPODArray<UInt16> start_dates;
    PaddedPODArray<UInt16> end_dates;
};


template <class DictionaryType, class Key>
RangeDictionaryBlockInputStream<DictionaryType, Key>::RangeDictionaryBlockInputStream(
    DictionatyPtr dictionary, size_t max_column_size, const Names & column_names, PaddedPODArray<Key> && ids,
    PaddedPODArray<UInt16> && start_dates, PaddedPODArray<UInt16> && end_dates)
    : DictionaryBlockInputStreamBase(ids.size(), max_column_size),
      dictionary(dictionary), column_names(column_names),
      ids(std::move(ids)), start_dates(std::move(start_dates)), end_dates(std::move(end_dates))
{
}

template <class DictionaryType, class Key>
Block RangeDictionaryBlockInputStream<DictionaryType, Key>::getBlock(size_t start, size_t length) const
{
    PaddedPODArray<Key> block_ids;
    PaddedPODArray<UInt16> block_start_dates;
    PaddedPODArray<UInt16> block_end_dates;
    block_ids.reserve(length);
    block_start_dates.reserve(length);
    block_end_dates.reserve(length);

    for (auto idx : ext::range(start, start + length))
    {
        block_ids.push_back(ids[idx]);
        block_start_dates.push_back(block_start_dates[idx]);
        block_end_dates.push_back(block_end_dates[idx]);
    }

    return fillBlock(block_ids, block_start_dates, block_end_dates);
}

template <class DictionaryType, class Key>
template <class AttributeType>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttribute(
    DictionaryGetter<AttributeType> getter, const PaddedPODArray<Key>& ids,
    const PaddedPODArray<UInt16> & dates, const DictionaryAttribute& attribute, const DictionaryType& dictionary) const
{
    auto column_vector = std::make_unique<ColumnVector<AttributeType>>(ids.size());
    (dictionary.*getter)(attribute.name, ids, dates, column_vector->getData());
    return ColumnPtr(std::move(column_vector));
}

template <class DictionaryType, class Key>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttributeString(
    const PaddedPODArray<Key>& ids, const PaddedPODArray<UInt16> & dates,
    const DictionaryAttribute& attribute, const DictionaryType& dictionary) const
{
    auto column_string = std::make_unique<ColumnString>();
    dictionary.getString(attribute.name, ids, dates, column_string.get());
    return ColumnPtr(std::move(column_string));
}

template <class DictionaryType, class Key>
template <class T>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, Key>::getColumnFromPODArray(const PaddedPODArray<T>& array) const
{
    auto column_vector = std::make_unique<ColumnVector<T>>();
    column_vector->getData().reserve(array.size());
    for (T value : array)
    {
        column_vector->insert(value);
    }
    return ColumnPtr(std::move(column_vector));
}


template <class DictionaryType, class Key>
template <class T>
void RangeDictionaryBlockInputStream<DictionaryType, Key>::addSpecialColumn(
    const std::experimental::optional<DictionarySpecialAttribute> & attribute, DataTypePtr type,
    const std::string& default_name, const std::unordered_set<std::string> & column_names,
    const PaddedPODArray<T> & values, ColumnsWithTypeAndName & columns) const
{
    std::string name = default_name;
    if (attribute) {
        name = attribute->name;
    }
    if (column_names.find(name) != column_names.end()) {
        columns.emplace_back(getColumnFromPODArray(values), type, name);
    }
}

template <class DictionaryType, class Key>
Block RangeDictionaryBlockInputStream<DictionaryType, Key>::fillBlock(
    const PaddedPODArray<Key>& ids,
    const PaddedPODArray<UInt16> & start_dates, const PaddedPODArray<UInt16> & end_dates) const
{
    ColumnsWithTypeAndName columns;
    const DictionaryStructure& structure = dictionary->getStructure();

    std::unordered_set<std::string> names(column_names.begin(), column_names.end());

    addSpecialColumn(structure.id, std::make_shared<DataTypeUInt64>(), "ID", names, ids, columns);
    addSpecialColumn(structure.range_min, std::make_shared<DataTypeDate>(), "Range Start", names, start_dates, columns);
    addSpecialColumn(structure.range_max, std::make_shared<DataTypeDate>(), "Range End", names, end_dates, columns);

    for (const auto idx : ext::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute& attribute = structure.attributes[idx];
        if (names.find(attribute.name) != names.end())
        {
            ColumnPtr column;
#define GET_COLUMN_FORM_ATTRIBUTE(TYPE)\
            column = getColumnFromAttribute<TYPE>(&DictionaryType::get##TYPE, ids, start_dates, attribute, *dictionary)
            switch (attribute.underlying_type)
            {
            case AttributeUnderlyingType::UInt8:
                GET_COLUMN_FORM_ATTRIBUTE(UInt8);
                break;
            case AttributeUnderlyingType::UInt16:
                GET_COLUMN_FORM_ATTRIBUTE(UInt16);
                break;
            case AttributeUnderlyingType::UInt32:
                GET_COLUMN_FORM_ATTRIBUTE(UInt32);
                break;
            case AttributeUnderlyingType::UInt64:
                GET_COLUMN_FORM_ATTRIBUTE(UInt64);
                break;
            case AttributeUnderlyingType::Int8:
                GET_COLUMN_FORM_ATTRIBUTE(Int8);
                break;
            case AttributeUnderlyingType::Int16:
                GET_COLUMN_FORM_ATTRIBUTE(Int16);
                break;
            case AttributeUnderlyingType::Int32:
                GET_COLUMN_FORM_ATTRIBUTE(Int32);
                break;
            case AttributeUnderlyingType::Int64:
                GET_COLUMN_FORM_ATTRIBUTE(Int64);
                break;
            case AttributeUnderlyingType::Float32:
                GET_COLUMN_FORM_ATTRIBUTE(Float32);
                break;
            case AttributeUnderlyingType::Float64:
                GET_COLUMN_FORM_ATTRIBUTE(Float64);
                break;
            case AttributeUnderlyingType::String:
                column = getColumnFromAttributeString(ids, start_dates, attribute, *dictionary);
                break;
            }

            columns.emplace_back(column, attribute.type, attribute.name);
        }
    }
    return Block(columns);
}

}
