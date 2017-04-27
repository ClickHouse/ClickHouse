#pragma once
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/DictionaryStructure.h>
#include <ext/range.hpp>

namespace DB {


/* 
 * BlockInputStream implementation for external dictionaries 
 * read() returns single block consisting of the in-memory contents of the dictionaries
 */
template <class DictionaryType, class Key>
class DictionaryBlockInputStream : public IProfilingBlockInputStream
{
public:
    DictionaryBlockInputStream(const DictionaryType& dictionary, const PaddedPODArray<Key> & ids);

    String getID() const override;
    String getName() const override { return "DictionaryBlockInputStream"; }
protected:
    Block readImpl() override;
    void readPrefixImpl() override { was_read = false; }
    void readSuffixImpl() override { was_read = false; }

private:
    Block block;
    bool was_read;

    template<class Type>
    using DictionaryGetter = void (DictionaryType::*)(const std::string &, 
                                                      const PaddedPODArray<Key> &, PaddedPODArray<Type> &) const;

    template <class AttributeType>
    ColumnPtr getColumnFromAttribute(DictionaryGetter<AttributeType> getter, const PaddedPODArray<Key>& ids, 
                                     const DictionaryAttribute& attribute, const DictionaryType& dictionary);
    ColumnPtr getColumnFromAttributeString(const PaddedPODArray<Key>& ids,
                                           const DictionaryAttribute& attribute, const DictionaryType& dictionary);
    ColumnPtr getColumnFromIds(const PaddedPODArray<Key>& ids);
};

template <class DictionaryType, class Key>
DictionaryBlockInputStream<DictionaryType, Key>::DictionaryBlockInputStream(const DictionaryType& dictionary, 
                                                                            const PaddedPODArray<Key>& ids)
    : was_read(false)
{
    ColumnsWithTypeAndName columns;
    const DictionaryStructure& structure = dictionary.getStructure();

    std::string id_column_name = "id";
    if (structure.id)
        id_column_name = structure.id->name;

    columns.emplace_back(getColumnFromIds(ids), std::make_shared<DataTypeUInt64>(), id_column_name);

    for (const auto idx : ext::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute& attribute = structure.attributes[idx];
        ColumnPtr column;
        #define GET_COLUMN_FORM_ATTRIBUTE(TYPE)\
            column = getColumnFromAttribute<TYPE>(&DictionaryType::get##TYPE, ids, attribute, dictionary)
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
            case AttributeUnderlyingType::String: column = getColumnFromAttributeString(ids, attribute, dictionary); break;
        }

        columns.emplace_back(column, attribute.type, attribute.name);
    }
    block = Block(columns);
}

template <class DictionaryType, class Key>
String DictionaryBlockInputStream<DictionaryType, Key>::getID() const
{
    std::stringstream ss;
    ss << static_cast<const void*> (this);
    return ss.str();
}

template <class DictionaryType, class Key>
Block DictionaryBlockInputStream<DictionaryType, Key>::readImpl()
{
    if (was_read)
        return Block();

    was_read = true;
    return block;
}


template <class DictionaryType, class Key>
template <class AttributeType>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttribute(
    DictionaryBlockInputStream::DictionaryGetter<AttributeType> getter,
    const PaddedPODArray<Key>& ids, const DictionaryAttribute& attribute, const DictionaryType& dictionary)
{
    auto column_vector = std::make_unique<ColumnVector<AttributeType>>(ids.size());
    (dictionary.*getter)(attribute.name, ids, column_vector->getData());
    return ColumnPtr(std::move(column_vector));
}

template <class DictionaryType, class Key>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttributeString(
    const PaddedPODArray<Key>& ids, const DictionaryAttribute& attribute, const DictionaryType& dictionary)
{
    auto column_string = std::make_unique<ColumnString>();
    dictionary.getString(attribute.name, ids, column_string.get());
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

}