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
#include <ext/range.hpp>

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
    RangeDictionaryBlockInputStream(
        const DictionaryType& dictionary, const Names & column_names, const PaddedPODArray<Key> & ids,
        const PaddedPODArray<UInt16> & start_dates, const PaddedPODArray<UInt16> & end_dates);

    String getName() const override { return "RangeDictionaryBlockInputStream"; }

private:
    template <class Type>
    using DictionaryGetter = void (DictionaryType::*)(const std::string &, const PaddedPODArray<Key> &,
                                                      const PaddedPODArray<UInt16> &, PaddedPODArray<Type> &) const;

    template <class AttributeType>
    ColumnPtr getColumnFromAttribute(DictionaryGetter<AttributeType> getter,
                                     const PaddedPODArray<Key>& ids, const PaddedPODArray<UInt16> & dates,
                                     const DictionaryAttribute& attribute, const DictionaryType& dictionary);
    ColumnPtr getColumnFromAttributeString(const PaddedPODArray<Key>& ids, const PaddedPODArray<UInt16> & dates,
                                           const DictionaryAttribute& attribute, const DictionaryType& dictionary);
    template <class T>
    ColumnPtr getColumnFromPODArray(const PaddedPODArray<T>& array);

    template <class T>
    void addSpecialColumn(
        const std::experimental::optional<DictionarySpecialAttribute>& attribute, DataTypePtr type,
        const std::string & default_name, const std::unordered_set<std::string> & column_names,
        const PaddedPODArray<T> & values, ColumnsWithTypeAndName& columns);
};

template <class DictionaryType, class Key>
template <class T>
void RangeDictionaryBlockInputStream<DictionaryType, Key>::addSpecialColumn(
    const std::experimental::optional<DictionarySpecialAttribute> & attribute, DataTypePtr type,
    const std::string& default_name, const std::unordered_set<std::string> & column_names, 
    const PaddedPODArray<T> & values, ColumnsWithTypeAndName & columns)
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
RangeDictionaryBlockInputStream<DictionaryType, Key>::RangeDictionaryBlockInputStream(
    const DictionaryType& dictionary, const Names & column_names, const PaddedPODArray<Key>& ids,
    const PaddedPODArray<UInt16> & start_dates, const PaddedPODArray<UInt16> & end_dates)
{
    ColumnsWithTypeAndName columns;
    const DictionaryStructure& structure = dictionary.getStructure();

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
                column = getColumnFromAttribute<TYPE>(&DictionaryType::get##TYPE, ids, start_dates, attribute, dictionary)
            switch (attribute.underlying_type)
            {
                case AttributeUnderlyingType::UInt8: GET_COLUMN_FORM_ATTRIBUTE(UInt8); break;
                case AttributeUnderlyingType::UInt16: GET_COLUMN_FORM_ATTRIBUTE(UInt16); break;
                case AttributeUnderlyingType::UInt32: GET_COLUMN_FORM_ATTRIBUTE(UInt32); break;
                case AttributeUnderlyingType::UInt64: GET_COLUMN_FORM_ATTRIBUTE(UInt64); break;
                case AttributeUnderlyingType::Int8: GET_COLUMN_FORM_ATTRIBUTE(Int8); break;
                case AttributeUnderlyingType::Int16: GET_COLUMN_FORM_ATTRIBUTE(Int16); break;
                case AttributeUnderlyingType::Int32: GET_COLUMN_FORM_ATTRIBUTE(Int32); break;
                case AttributeUnderlyingType::Int64: GET_COLUMN_FORM_ATTRIBUTE(Int64); break;
                case AttributeUnderlyingType::Float32: GET_COLUMN_FORM_ATTRIBUTE(Float32); break;
                case AttributeUnderlyingType::Float64: GET_COLUMN_FORM_ATTRIBUTE(Float64); break;
                case AttributeUnderlyingType::String:
                    column = getColumnFromAttributeString(ids, start_dates, attribute, dictionary); break;
            }

            columns.emplace_back(column, attribute.type, attribute.name);
        }
    }
    block = Block(columns);
}

template <class DictionaryType, class Key>
template <class AttributeType>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttribute(
    DictionaryGetter<AttributeType> getter, const PaddedPODArray<Key>& ids,
    const PaddedPODArray<UInt16> & dates, const DictionaryAttribute& attribute, const DictionaryType& dictionary)
{
    auto column_vector = std::make_unique<ColumnVector<AttributeType>>(ids.size());
    (dictionary.*getter)(attribute.name, ids, dates, column_vector->getData());
    return ColumnPtr(std::move(column_vector));
}

template <class DictionaryType, class Key>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttributeString(
    const PaddedPODArray<Key>& ids, const PaddedPODArray<UInt16> & dates,
    const DictionaryAttribute& attribute, const DictionaryType& dictionary)
{
    auto column_string = std::make_unique<ColumnString>();
    dictionary.getString(attribute.name, ids, dates, column_string.get());
    return ColumnPtr(std::move(column_string));
}

template <class DictionaryType, class Key>
template <class T>
ColumnPtr RangeDictionaryBlockInputStream<DictionaryType, Key>::getColumnFromPODArray(const PaddedPODArray<T>& array)
{
    auto column_vector = std::make_unique<ColumnVector<T>>();
    column_vector->getData().reserve(array.size());
    for (T value : array) 
    {
        column_vector->insert(value);
    }
    return ColumnPtr(std::move(column_vector));
}

}