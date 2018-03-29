#pragma once
#include <Columns/ColumnWithDictionary.h>
#include <Columns/ColumnUnique.h>
#include <memory>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/IDataType.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnFixedString.h>
#include <Core/TypeListNumber.h>
#include "DataTypeDate.h"
#include "DataTypeDateTime.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class DataTypeWithDictionary : public IDataType
{
private:
    DataTypePtr dictionary_type;
    DataTypePtr indexes_type;

public:

    DataTypeWithDictionary(DataTypePtr dictionary_type_, DataTypePtr indexes_type_)
            : dictionary_type(std::move(dictionary_type_)), indexes_type(std::move(indexes_type_))
    {
        if (!indexes_type->isUnsignedInteger())
            throw Exception("Index type of DataTypeWithDictionary must be unsigned integer, but got "
                            + indexes_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto inner_type = dictionary_type;
        if (dictionary_type->isNullable())
            inner_type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

        if (!inner_type->isStringOrFixedString()
            && !inner_type->isDateOrDateTime()
            && !inner_type->isNumber())
            throw Exception("DataTypeWithDictionary is supported only for numbers, strings, Date or DateTime, but got "
                            + dictionary_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    const DataTypePtr & getDictionaryType() const { return dictionary_type; }
    const DataTypePtr & getIndexesType() const { return indexes_type; }

    String getName() const override
    {
        return "WithDictionary(" + dictionary_type->getName() + ", " + indexes_type->getName() + ")";
    }
    const char * getFamilyName() const override { return "WithDictionary"; }

    void enumerateStreams(StreamCallback callback, SubstreamPath path) const override
    {
        path.push_back(Substream::DictionaryElements);
        dictionary_type->enumerateStreams(callback, path);
        path.back() = Substream::DictionaryIndexes;
        indexes_type->enumerateStreams(callback, path);
    }

    void serializeBinaryBulkWithMultipleStreams(
            const IColumn & column,
            OutputStreamGetter getter,
            size_t offset,
            size_t limit,
            bool /*position_independent_encoding*/,
            SubstreamPath path) const override
    {
        const ColumnWithDictionary & column_with_dictionary = typeid_cast<const ColumnWithDictionary &>(column);

        path.push_back(Substream::DictionaryElements);
        if (auto stream = getter(path))
            dictionary_type->serializeBinaryBulk(*column_with_dictionary.getUnique(), *stream, offset, limit);

        path.back() = Substream::DictionaryIndexes;
        if (auto stream = getter(path))
            indexes_type->serializeBinaryBulk(*column_with_dictionary.getIndexes(), *stream, offset, limit);
    }

    void deserializeBinaryBulkWithMultipleStreams(
            IColumn & column,
            InputStreamGetter getter,
            size_t limit,
            double /*avg_value_size_hint*/,
            bool /*position_independent_encoding*/,
            SubstreamPath path) const override
    {
        ColumnWithDictionary & column_with_dictionary = typeid_cast<ColumnWithDictionary &>(column);

        path.push_back(Substream::DictionaryElements);
        if (ReadBuffer * stream = getter(path))
            dictionary_type->deserializeBinaryBulk(*column_with_dictionary.getUnique(), *stream, limit, 0);

        path.back() = Substream::DictionaryIndexes;
        if (auto stream = getter(path))
            indexes_type->deserializeBinaryBulk(*column_with_dictionary.getIndexes(), *stream, limit, 0);
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override { dictionary_type->serializeBinary(field, ostr); }
    void deserializeBinary(Field & field, ReadBuffer & istr) const override { dictionary_type->deserializeBinary(field, istr); }

    const ColumnWithDictionary & getColumnWithDictionary(const IColumn & column) const
    {
        return typeid_cast<const ColumnWithDictionary &>(column);;
    }

    ColumnWithDictionary & getColumnWithDictionary(IColumn & column) const
    {
        return typeid_cast<ColumnWithDictionary &>(column);;
    }

    IColumn & getNestedUniqueColumn(ColumnWithDictionary & column_with_dictionary) const
    {
        return *column_with_dictionary.getUnique()->getNestedColumn()->assumeMutable();
    }

    template <typename ... Args>
    using SerealizeFunctionPtr = void (IDataType::*)(const IColumn &, size_t, WriteBuffer &, Args & ...) const;

    template <typename ... Args>
    void serializeImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, SerealizeFunctionPtr<Args ...> func, Args & ... args) const
    {
        auto & column_with_dictionary = getColumnWithDictionary(column);
        size_t unique_row_number = column_with_dictionary.getIndexes()->getUInt(row_num);
        (dictionary_type.get()->*func)(*column_with_dictionary.getUnique(), unique_row_number, ostr, std::forward<Args>(args)...);
    }

    template <typename ... Args>
    using DeserealizeFunctionPtr = void (IDataType::*)(IColumn &, ReadBuffer &, Args ...) const;

    template <typename ... Args>
    void deserializeImpl(IColumn & column, ReadBuffer & istr, DeserealizeFunctionPtr<Args ...> func, Args ... args) const
    {
        auto & column_with_dictionary = getColumnWithDictionary(column);
        auto nested_unique = getNestedUniqueColumn(column_with_dictionary).assumeMutable();

        auto size = column_with_dictionary.size();
        auto unique_size = nested_unique->size();

        (dictionary_type.get()->*func)(*nested_unique, istr, std::forward<Args>(args)...);

        /// Note: Insertion into ColumnWithDictionary from it's nested column may cause insertion from column to itself.
        /// Generally it's wrong because column may reallocate memory before insertion.
        column_with_dictionary.insertFrom(*nested_unique, unique_size);
        if (column_with_dictionary.getIndexes()->getUInt(size) != unique_size)
            nested_unique->popBack(1);
    }

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeBinary);
    }
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override
    {
        deserializeImpl(column, istr, &IDataType::deserializeBinary);
    }

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeTextEscaped);
    }

    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override
    {
        deserializeImpl(column, istr, &IDataType::deserializeTextEscaped);
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeTextQuoted);
    }

    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override
    {
        deserializeImpl(column, istr, &IDataType::deserializeTextQuoted);
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeTextCSV);
    }

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override
    {
        deserializeImpl(column, istr, &IDataType::deserializeTextCSV, delimiter);
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeText);
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON & settings) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeTextJSON, settings);
    }
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override
    {
        deserializeImpl(column, istr, &IDataType::deserializeTextJSON);
    }

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        serializeImpl(column, row_num, ostr, &IDataType::serializeTextXML);
    }

    template <typename ColumnType, typename IndexType>
    MutableColumnPtr createColumnImpl() const
    {
        return ColumnWithDictionary::create(ColumnUnique<ColumnType, IndexType>::create(dictionary_type),
                                            indexes_type->createColumn());
    }

    template <typename ColumnType>
    MutableColumnPtr createColumnImpl() const
    {
        if (typeid_cast<const DataTypeUInt8 *>(indexes_type.get()))
            return createColumnImpl<ColumnType, UInt8>();
        if (typeid_cast<const DataTypeUInt16 *>(indexes_type.get()))
            return createColumnImpl<ColumnType, UInt16>();
        if (typeid_cast<const DataTypeUInt32 *>(indexes_type.get()))
            return createColumnImpl<ColumnType, UInt32>();
        if (typeid_cast<const DataTypeUInt64 *>(indexes_type.get()))
            return createColumnImpl<ColumnType, UInt64>();

        throw Exception("The type of indexes must be unsigned integer, but got " + dictionary_type->getName(),
                        ErrorCodes::LOGICAL_ERROR);
    }

private:
    struct CreateColumnVector
    {
        MutableColumnPtr & column;
        const DataTypeWithDictionary * data_type_with_dictionary;
        const IDataType * type;

        CreateColumnVector(MutableColumnPtr & column, const DataTypeWithDictionary * data_type_with_dictionary,
                           const IDataType * type)
                : column(column), data_type_with_dictionary(data_type_with_dictionary), type(type) {}

        template <typename T, size_t>
        void operator()()
        {
            if (typeid_cast<const DataTypeNumber<T> *>(type))
                column = data_type_with_dictionary->createColumnImpl<ColumnVector<T>>();
        }
    };

public:
    MutableColumnPtr createColumn() const override
    {
        auto type = dictionary_type;
        if (type->isNullable())
            type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

        if (type->isString())
            return createColumnImpl<ColumnString>();
        if (type->isFixedString())
            return createColumnImpl<ColumnFixedString>();
        if (typeid_cast<const DataTypeDate *>(type.get()))
            return createColumnImpl<ColumnVector<UInt16>>();
        if (typeid_cast<const DataTypeDateTime *>(type.get()))
            return createColumnImpl<ColumnVector<UInt32>>();
        if (type->isNumber())
        {
            MutableColumnPtr column;
            TypeListNumbers::forEach(CreateColumnVector(column, this, dictionary_type.get()));

            if (!column)
                throw Exception("Unexpected numeric type: " + type->getName(), ErrorCodes::LOGICAL_ERROR);

            return std::move(column);
        }

        throw Exception("Unexpected dictionary type for DataTypeWithDictionary: " + type->getName(), ErrorCodes::LOGICAL_ERROR);
    }

    Field getDefault() const override { return dictionary_type->getDefault(); }

    bool equals(const IDataType & rhs) const override
    {
        if (typeid(rhs) != typeid(*this))
            return false;

        auto & rhs_with_dictionary = static_cast<const DataTypeWithDictionary &>(rhs);
        return dictionary_type->equals(*rhs_with_dictionary.dictionary_type)
               && indexes_type->equals(*rhs_with_dictionary.indexes_type);
    }

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return dictionary_type->cannotBeStoredInTables(); }
    bool shouldAlignRightInPrettyFormats() const override { return dictionary_type->shouldAlignRightInPrettyFormats(); }
    bool textCanContainOnlyValidUTF8() const override { return dictionary_type->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return dictionary_type->isComparable(); }
    bool canBeComparedWithCollation() const override { return dictionary_type->canBeComparedWithCollation(); }
    bool canBeUsedAsVersion() const override { return dictionary_type->canBeUsedAsVersion(); }
    bool isSummable() const override { return dictionary_type->isSummable(); };
    bool canBeUsedInBitOperations() const override { return dictionary_type->canBeUsedInBitOperations(); };
    bool canBeUsedInBooleanContext() const override { return dictionary_type->canBeUsedInBooleanContext(); };
    bool isNumber() const override { return false; }
    bool isInteger() const override { return false; }
    bool isUnsignedInteger() const override { return false; }
    bool isDateOrDateTime() const override { return false; }
    bool isValueRepresentedByNumber() const override { return dictionary_type->isValueRepresentedByNumber(); }
    bool isValueRepresentedByInteger() const override { return dictionary_type->isValueRepresentedByInteger(); }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isString() const override { return false; }
    bool isFixedString() const override { return false; }
    bool haveMaximumSizeOfValue() const override { return dictionary_type->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return dictionary_type->getMaximumSizeOfValueInMemory(); }
    size_t getSizeOfValueInMemory() const override { return dictionary_type->getSizeOfValueInMemory(); }
    bool isCategorial() const override { return false; }
    bool isEnum() const override { return false; }
    bool isNullable() const override { return false; }
    bool onlyNull() const override { return false; }
};

}
