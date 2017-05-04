#pragma once
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryBlockInputStreamBase.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <ext/range.hpp>
#include <common/logger_useful.h>
#include <Core/Names.h>

namespace DB 
{

/* 
 * BlockInputStream implementation for external dictionaries 
 * read() returns single block consisting of the in-memory contents of the dictionaries
 */
template <class DictionaryType, class Key>
class DictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    DictionaryBlockInputStream(const DictionaryType& dictionary,
                               const PaddedPODArray<Key> & ids, const Names & column_names);
    DictionaryBlockInputStream(const DictionaryType& dictionary,
                               const std::vector<StringRef> & keys, const Names & column_names);

    String getName() const override { return "DictionaryBlockInputStream"; }

private:

    // pointer types to getXXX functions
    // for single key dictionaries
    template <class Type>
    using DictionaryGetter = void (DictionaryType::*)(
        const std::string &, const PaddedPODArray<Key> &, PaddedPODArray<Type> &) const;
    using DictionaryStringGetter = void (DictionaryType::*)(
        const std::string &, const PaddedPODArray<Key> &, ColumnString *) const;
    // for complex complex key dictionaries
    template <class Type>
    using GetterByKey = void (DictionaryType::*)(
        const std::string &, const ConstColumnPlainPtrs &, const DataTypes &, PaddedPODArray<Type> & out) const;
    using StringGetterByKey = void (DictionaryType::*)(
        const std::string &, const ConstColumnPlainPtrs &, const DataTypes &, ColumnString * out) const;

    // call getXXX
    // for single key dictionaries
    template <class Type, class Container>
    void callGetter(DictionaryGetter<Type> getter, const PaddedPODArray<Key> & ids,
                    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary);
    template <class Container>
    void callGetter(DictionaryStringGetter getter, const PaddedPODArray<Key> & ids,
                    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary);
    // for complex complex key dictionaries
    template <class Type, class Container>
    void callGetter(GetterByKey<Type> getter, const PaddedPODArray<Key> & ids,
                    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary);
    template <class Container>
    void callGetter(StringGetterByKey getter, const PaddedPODArray<Key> & ids,
                    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary);

    template <template <class> class Getter, class StringGetter>
    void fillBlock(const DictionaryType& dictionary, const Names& column_names,
                   const PaddedPODArray<Key>& ids, const ColumnsWithTypeAndName& keys);


    template <class AttributeType, class Getter>
    ColumnPtr getColumnFromAttribute(Getter getter, const PaddedPODArray<Key> & ids, 
                                     const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
                                     const DictionaryAttribute & attribute, const DictionaryType & dictionary);
    template <class Getter>
    ColumnPtr getColumnFromStringAttribute(Getter getter, const PaddedPODArray<Key> & ids,
                                           const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
                                           const DictionaryAttribute& attribute, const DictionaryType& dictionary);
    ColumnPtr getColumnFromIds(const PaddedPODArray<Key>& ids);

    void fillKeyColumns(const std::vector<StringRef> & keys, const DictionaryStructure& dictionary_structure,
                        ColumnsWithTypeAndName & columns);

    Poco::Logger * logger;
};

template <class DictionaryType, class Key>
DictionaryBlockInputStream<DictionaryType, Key>::DictionaryBlockInputStream(
    const DictionaryType& dictionary, const PaddedPODArray<Key> & ids, const Names& column_names)
{
    logger = &Poco::Logger::get("DictionaryBlockInputStream");
    fillBlock<DictionaryGetter, DictionaryStringGetter>(dictionary, column_names, ids, {});
}

template <class DictionaryType, class Key>
DictionaryBlockInputStream<DictionaryType, Key>::DictionaryBlockInputStream(
    const DictionaryType& dictionary, const std::vector<StringRef> & keys, const Names& column_names)
{
    logger = &Poco::Logger::get("DictionaryBlockInputStream");
    const DictionaryStructure& dictionaty_structure = dictionary.getStructure();
    ColumnsWithTypeAndName columns;
    fillKeyColumns(keys, dictionaty_structure, columns);
    fillBlock<GetterByKey, StringGetterByKey>(dictionary, column_names, {}, columns);
}

template <class DictionaryType, class Key>
template <class Type, class Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    DictionaryGetter<Type> getter, const PaddedPODArray<Key> & ids,
    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary)
{
    (dictionary.*getter)(attribute.name, ids, container);
}

template <class DictionaryType, class Key>
template <class Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    DictionaryStringGetter getter, const PaddedPODArray<Key> & ids,
    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary)
{
    (dictionary.*getter)(attribute.name, ids, container);
}

template <class DictionaryType, class Key>
template <class Type, class Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    GetterByKey<Type> getter, const PaddedPODArray<Key> & ids,
    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary)
{
    (dictionary.*getter)(attribute.name, keys, data_types, container);
}

template <class DictionaryType, class Key>
template <class Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    StringGetterByKey getter, const PaddedPODArray<Key> & ids,
    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary)
{
    (dictionary.*getter)(attribute.name, keys, data_types, container);
}

template <class DictionaryType, class Key>
template <template <class> class Getter, class StringGetter>
void DictionaryBlockInputStream<DictionaryType, Key>::fillBlock(
    const DictionaryType& dictionary, const Names& column_names,
    const PaddedPODArray<Key>& ids, const ColumnsWithTypeAndName& keys)
{
    std::unordered_set<std::string> names(column_names.begin(), column_names.end());

    ConstColumnPlainPtrs key_columns_palin_ptrs;
    DataTypes data_types;
    key_columns_palin_ptrs.reserve(keys.size());
    for (const auto& key : keys)
    {
        key_columns_palin_ptrs.push_back(key.column.get());
        data_types.push_back(key.type);
    }

    ColumnsWithTypeAndName columns;
    const DictionaryStructure& structure = dictionary.getStructure();

    if (structure.id && names.find(structure.id->name) != names.end())
        columns.emplace_back(getColumnFromIds(ids), std::make_shared<DataTypeUInt64>(), structure.id->name);

    for (const auto & key : keys)
        if (names.find(key.name) != names.end())
            columns.push_back(key);

    for (const auto idx : ext::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute& attribute = structure.attributes[idx];
        if (names.find(attribute.name) != names.end()) 
        {
            ColumnPtr column;
            #define GET_COLUMN_FORM_ATTRIBUTE(TYPE) \
                column = getColumnFromAttribute<TYPE, Getter<TYPE>>( \
                &DictionaryType::get##TYPE, ids, key_columns_palin_ptrs, data_types, attribute, dictionary)
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
                {
                    column = getColumnFromStringAttribute<StringGetter>(
                        &DictionaryType::getString, ids, key_columns_palin_ptrs, data_types, attribute, dictionary);
                    break;
                }
            }

            columns.emplace_back(column, attribute.type, attribute.name);
        }
    }
    block = Block(columns);
}

template <class DictionaryType, class Key>
template <class AttributeType, class Getter>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttribute(
    Getter getter, const PaddedPODArray<Key> & ids,
    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
    const DictionaryAttribute & attribute, const DictionaryType & dictionary)
{
    auto size = ids.size();
    if (!keys.empty())
        size = keys.front()->size();
    auto column_vector = std::make_unique<ColumnVector<AttributeType>>(size);
    callGetter(getter, ids, keys, data_types, column_vector->getData(), attribute, dictionary);
    return ColumnPtr(std::move(column_vector));
}

template <class DictionaryType, class Key>
template <class Getter>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromStringAttribute(
    Getter getter, const PaddedPODArray<Key> & ids,
    const ConstColumnPlainPtrs & keys, const DataTypes & data_types,
    const DictionaryAttribute& attribute, const DictionaryType& dictionary)
{
    auto column_string = std::make_unique<ColumnString>();
    auto ptr = column_string.get();
    callGetter(getter, ids, keys, data_types, ptr, attribute, dictionary);
    return ColumnPtr(std::move(column_string));
}

template <class DictionaryType, class Key>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromIds(const PaddedPODArray<Key>& ids)
{
    auto column_vector = std::make_unique<ColumnVector<UInt64>>();
    column_vector->getData().reserve(ids.size());
    for (UInt64 id : ids) 
    {
        column_vector->insert(id);
    }
    return ColumnPtr(std::move(column_vector));
}

template <class DictionaryType, class Key>
void DictionaryBlockInputStream<DictionaryType, Key>::fillKeyColumns(
    const std::vector<StringRef> & keys, const DictionaryStructure& dictionary_structure,
    ColumnsWithTypeAndName & columns)
{
    for (const DictionaryAttribute & attribute: *dictionary_structure.key)
    {
        #define ADD_COLUMN(TYPE) columns.push_back( \
            ColumnWithTypeAndName(std::move(std::make_unique<ColumnVector<TYPE>>()), attribute.type, attribute.name))
        switch (attribute.underlying_type)
        {
            case AttributeUnderlyingType::UInt8: ADD_COLUMN(UInt8); break;
            case AttributeUnderlyingType::UInt16:ADD_COLUMN(UInt16); break;
            case AttributeUnderlyingType::UInt32: ADD_COLUMN(UInt32); break;
            case AttributeUnderlyingType::UInt64: ADD_COLUMN(UInt64); break;
            case AttributeUnderlyingType::Int8: ADD_COLUMN(Int8); break;
            case AttributeUnderlyingType::Int16: ADD_COLUMN(Int16); break;
            case AttributeUnderlyingType::Int32: ADD_COLUMN(Int32); break;
            case AttributeUnderlyingType::Int64: ADD_COLUMN(Int64); break;
            case AttributeUnderlyingType::Float32: ADD_COLUMN(Float32); break;
            case AttributeUnderlyingType::Float64: ADD_COLUMN(Float64); break;
            case AttributeUnderlyingType::String: 
            {
                columns.push_back(ColumnWithTypeAndName(
                    std::move(std::make_unique<ColumnString>()), attribute.type, attribute.name));
                break;
            }
        }
    }

    for (const auto & key : keys)
    {
        auto ptr = key.data;
        for (const auto & column : columns)
            ptr = column.column->deserializeAndInsertFromArena(ptr);
        if (ptr != key.data + key.size)
            throw Exception("DictionaryBlockInputStream: unable to deserialize data");
    }
}

}