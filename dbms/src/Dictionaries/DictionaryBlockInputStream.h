#pragma once
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryBlockInputStreamBase.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <ext/range.h>
#include <common/logger_useful.h>
#include <Core/Names.h>
#include <memory>

namespace DB
{

/*
 * BlockInputStream implementation for external dictionaries
 * read() returns single block consisting of the in-memory contents of the dictionaries
 */
template <typename DictionaryType, typename Key>
class DictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    using DictionatyPtr = std::shared_ptr<DictionaryType const>;

    DictionaryBlockInputStream(std::shared_ptr<const IDictionaryBase> dictionary, size_t max_block_size,
                               PaddedPODArray<Key> && ids, const Names & column_names);
    DictionaryBlockInputStream(std::shared_ptr<const IDictionaryBase> dictionary, size_t max_block_size,
                               const std::vector<StringRef> & keys, const Names & column_names);

    using GetColumnsFunction =
        std::function<ColumnsWithTypeAndName(const Columns &, const std::vector<DictionaryAttribute> & attributes)>;
    // Used to separate key columns format for storage and view.
    // Calls get_key_columns_function to get key column for dictionary get fuction call
    // and get_view_columns_function to get key representation.
    // Now used in trie dictionary, where columns are stored as ip and mask, and are showed as string
    DictionaryBlockInputStream(std::shared_ptr<const IDictionaryBase> dictionary, size_t max_block_size,
                               const Columns & data_columns, const Names & column_names,
                               GetColumnsFunction && get_key_columns_function,
                               GetColumnsFunction && get_view_columns_function);

    String getName() const override
    {
        return "Dictionary";
    }

protected:
    Block getBlock(size_t start, size_t size) const override;

private:
    // pointer types to getXXX functions
    // for single key dictionaries
    template <typename Type>
    using DictionaryGetter = void (DictionaryType::*)(
                                 const std::string &, const PaddedPODArray<Key> &, PaddedPODArray<Type> &) const;
    using DictionaryStringGetter = void (DictionaryType::*)(
                                       const std::string &, const PaddedPODArray<Key> &, ColumnString *) const;
    // for complex complex key dictionaries
    template <typename Type>
    using GetterByKey = void (DictionaryType::*)(
                            const std::string &, const Columns &, const DataTypes &, PaddedPODArray<Type> & out) const;
    using StringGetterByKey = void (DictionaryType::*)(
                                  const std::string &, const Columns &, const DataTypes &, ColumnString * out) const;

    // call getXXX
    // for single key dictionaries
    template <typename Type, typename Container>
    void callGetter(DictionaryGetter<Type> getter, const PaddedPODArray<Key> & ids,
                    const Columns & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const;
    template <typename Container>
    void callGetter(DictionaryStringGetter getter, const PaddedPODArray<Key> & ids,
                    const Columns & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const;
    // for complex complex key dictionaries
    template <typename Type, typename Container>
    void callGetter(GetterByKey<Type> getter, const PaddedPODArray<Key> & ids,
                    const Columns & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const;
    template <typename Container>
    void callGetter(StringGetterByKey getter, const PaddedPODArray<Key> & ids,
                    const Columns & keys, const DataTypes & data_types,
                    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const;

    template <template <typename> class Getter, typename StringGetter>
    Block fillBlock(const PaddedPODArray<Key> & ids, const Columns & keys,
                    const DataTypes & types, ColumnsWithTypeAndName && view) const;


    template <typename AttributeType, typename Getter>
    ColumnPtr getColumnFromAttribute(Getter getter, const PaddedPODArray<Key> & ids,
                                     const Columns & keys, const DataTypes & data_types,
                                     const DictionaryAttribute & attribute, const DictionaryType & dictionary) const;
    template <typename Getter>
    ColumnPtr getColumnFromStringAttribute(Getter getter, const PaddedPODArray<Key> & ids,
                                           const Columns & keys, const DataTypes & data_types,
                                           const DictionaryAttribute& attribute, const DictionaryType& dictionary) const;
    ColumnPtr getColumnFromIds(const PaddedPODArray<Key> & ids) const;

    void fillKeyColumns(const std::vector<StringRef> & keys, size_t start, size_t size,
                        const DictionaryStructure & dictionary_structure, ColumnsWithTypeAndName & columns) const;

    DictionatyPtr dictionary;
    Names column_names;
    PaddedPODArray<Key> ids;
    ColumnsWithTypeAndName key_columns;
    Poco::Logger * logger;
    Block (DictionaryBlockInputStream<DictionaryType, Key>::*fillBlockFunction)(
        const PaddedPODArray<Key> & ids, const Columns& keys,
        const DataTypes & types, ColumnsWithTypeAndName && view) const;

    Columns data_columns;
    GetColumnsFunction get_key_columns_function;
    GetColumnsFunction get_view_columns_function;

    enum class DictionaryKeyType
    {
        Id,
        ComplexKey,
        Callback
    };

    DictionaryKeyType key_type;
};

template <typename DictionaryType, typename Key>
DictionaryBlockInputStream<DictionaryType, Key>::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionaryBase> dictionary, size_t max_block_size,
    PaddedPODArray<Key> && ids, const Names& column_names)
    : DictionaryBlockInputStreamBase(ids.size(), max_block_size),
      dictionary(std::static_pointer_cast<const DictionaryType>(dictionary)),
      column_names(column_names), ids(std::move(ids)),
      logger(&Poco::Logger::get("DictionaryBlockInputStream")),
      fillBlockFunction(&DictionaryBlockInputStream<DictionaryType, Key>::fillBlock<DictionaryGetter, DictionaryStringGetter>),
      key_type(DictionaryKeyType::Id)
{
}

template <typename DictionaryType, typename Key>
DictionaryBlockInputStream<DictionaryType, Key>::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionaryBase> dictionary, size_t max_block_size,
    const std::vector<StringRef> & keys, const Names& column_names)
    : DictionaryBlockInputStreamBase(keys.size(), max_block_size),
      dictionary(std::static_pointer_cast<const DictionaryType>(dictionary)), column_names(column_names),
      logger(&Poco::Logger::get("DictionaryBlockInputStream")),
      fillBlockFunction(&DictionaryBlockInputStream<DictionaryType, Key>::fillBlock<GetterByKey, StringGetterByKey>),
      key_type(DictionaryKeyType::ComplexKey)
{
    const DictionaryStructure & dictionaty_structure = dictionary->getStructure();
    fillKeyColumns(keys, 0, keys.size(), dictionaty_structure, key_columns);
}

template <typename DictionaryType, typename Key>
DictionaryBlockInputStream<DictionaryType, Key>::DictionaryBlockInputStream(
    std::shared_ptr<const IDictionaryBase> dictionary, size_t max_block_size,
    const Columns & data_columns, const Names & column_names,
    GetColumnsFunction && get_key_columns_function,
    GetColumnsFunction && get_view_columns_function)
    : DictionaryBlockInputStreamBase(data_columns.front()->size(), max_block_size),
      dictionary(std::static_pointer_cast<const DictionaryType>(dictionary)), column_names(column_names),
      logger(&Poco::Logger::get("DictionaryBlockInputStream")),
      fillBlockFunction(&DictionaryBlockInputStream<DictionaryType, Key>::fillBlock<GetterByKey, StringGetterByKey>),
      data_columns(data_columns),
      get_key_columns_function(get_key_columns_function), get_view_columns_function(get_view_columns_function),
      key_type(DictionaryKeyType::Callback)
{
}

template <typename DictionaryType, typename Key>
Block DictionaryBlockInputStream<DictionaryType, Key>::getBlock(size_t start, size_t length) const
{
    switch (key_type)
    {
        case DictionaryKeyType::ComplexKey:
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
            return (this->*fillBlockFunction)({}, columns, {}, std::move(view_columns));
        }
        case DictionaryKeyType::Id:
        {
            PaddedPODArray<Key> block_ids(ids.begin() + start, ids.begin() + start + length);
            return (this->*fillBlockFunction)(block_ids, {}, {}, {});
        }
        case DictionaryKeyType::Callback:
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
            return (this->*fillBlockFunction)({}, columns, types, std::move(view_with_type_and_name));
        }
    }
    throw Exception("Unexpected DictionaryKeyType.", ErrorCodes::LOGICAL_ERROR);
}

template <typename DictionaryType, typename Key>
template <typename Type, typename Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    DictionaryGetter<Type> getter, const PaddedPODArray<Key> & ids,
    const Columns & /*keys*/, const DataTypes & /*data_types*/,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const
{
    (dictionary.*getter)(attribute.name, ids, container);
}

template <typename DictionaryType, typename Key>
template <typename Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    DictionaryStringGetter getter, const PaddedPODArray<Key> & ids,
    const Columns & /*keys*/, const DataTypes & /*data_types*/,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const
{
    (dictionary.*getter)(attribute.name, ids, container);
}

template <typename DictionaryType, typename Key>
template <typename Type, typename Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    GetterByKey<Type> getter, const PaddedPODArray<Key> & /*ids*/,
    const Columns & keys, const DataTypes & data_types,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const
{
    (dictionary.*getter)(attribute.name, keys, data_types, container);
}

template <typename DictionaryType, typename Key>
template <typename Container>
void DictionaryBlockInputStream<DictionaryType, Key>::callGetter(
    StringGetterByKey getter, const PaddedPODArray<Key> & /*ids*/,
    const Columns & keys, const DataTypes & data_types,
    Container & container, const DictionaryAttribute & attribute, const DictionaryType & dictionary) const
{
    (dictionary.*getter)(attribute.name, keys, data_types, container);
}

template <typename DictionaryType, typename Key>
template <template <typename> class Getter, typename StringGetter>
Block DictionaryBlockInputStream<DictionaryType, Key>::fillBlock(
    const PaddedPODArray<Key> & ids, const Columns & keys, const DataTypes & types, ColumnsWithTypeAndName && view) const
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

    if (structure.id && names.find(structure.id->name) != names.end())
        block_columns.emplace_back(getColumnFromIds(ids), std::make_shared<DataTypeUInt64>(), structure.id->name);

    for (const auto idx : ext::range(0, structure.attributes.size()))
    {
        const DictionaryAttribute& attribute = structure.attributes[idx];
        if (names.find(attribute.name) != names.end())
        {
            ColumnPtr column;
#define GET_COLUMN_FORM_ATTRIBUTE(TYPE) \
                column = getColumnFromAttribute<TYPE, Getter<TYPE>>( \
                &DictionaryType::get##TYPE, ids, keys, data_types, attribute, *dictionary)
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
            case AttributeUnderlyingType::UInt128:
                GET_COLUMN_FORM_ATTRIBUTE(UInt128);
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
            {
                column = getColumnFromStringAttribute<StringGetter>(
                    &DictionaryType::getString, ids, keys, data_types, attribute, *dictionary);
                break;
            }
            }

            block_columns.emplace_back(column, attribute.type, attribute.name);
        }
    }
    return Block(block_columns);
}

template <typename DictionaryType, typename Key>
template <typename AttributeType, typename Getter>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromAttribute(
    Getter getter, const PaddedPODArray<Key> & ids,
    const Columns & keys, const DataTypes & data_types,
    const DictionaryAttribute & attribute, const DictionaryType & dictionary) const
{
    auto size = ids.size();
    if (!keys.empty())
        size = keys.front()->size();
    auto column_vector = ColumnVector<AttributeType>::create(size);
    callGetter(getter, ids, keys, data_types, column_vector->getData(), attribute, dictionary);
    return std::move(column_vector);
}

template <typename DictionaryType, typename Key>
template <typename Getter>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromStringAttribute(
    Getter getter, const PaddedPODArray<Key> & ids,
    const Columns & keys, const DataTypes & data_types,
    const DictionaryAttribute& attribute, const DictionaryType& dictionary) const
{
    auto column_string = ColumnString::create();
    auto ptr = column_string.get();
    callGetter(getter, ids, keys, data_types, ptr, attribute, dictionary);
    return std::move(column_string);
}

template <typename DictionaryType, typename Key>
ColumnPtr DictionaryBlockInputStream<DictionaryType, Key>::getColumnFromIds(const PaddedPODArray<Key> & ids) const
{
    auto column_vector = ColumnVector<UInt64>::create();
    column_vector->getData().reserve(ids.size());
    for (UInt64 id : ids)
        column_vector->insert(id);
    return std::move(column_vector);
}

template <typename DictionaryType, typename Key>
void DictionaryBlockInputStream<DictionaryType, Key>::fillKeyColumns(
    const std::vector<StringRef> & keys, size_t start, size_t size,
    const DictionaryStructure & dictionary_structure, ColumnsWithTypeAndName & res) const
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

    for (size_t i = 0, size = columns.size(); i < size; ++i)
        res.emplace_back(ColumnWithTypeAndName{ std::move(columns[i]), (*dictionary_structure.key)[i].type, (*dictionary_structure.key)[i].name });
}

}
