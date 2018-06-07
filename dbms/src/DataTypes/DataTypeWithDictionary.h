#pragma once
#include <DataTypes/IDataType.h>
#include <Columns/IColumnUnique.h>

namespace DB
{

class DataTypeWithDictionary : public IDataType
{
private:
    DataTypePtr dictionary_type;
    DataTypePtr indexes_type;

public:
    DataTypeWithDictionary(DataTypePtr dictionary_type_, DataTypePtr indexes_type_);

    const DataTypePtr & getDictionaryType() const { return dictionary_type; }
    const DataTypePtr & getIndexesType() const { return indexes_type; }

    String getName() const override
    {
        return "WithDictionary(" + dictionary_type->getName() + ", " + indexes_type->getName() + ")";
    }
    const char * getFamilyName() const override { return "WithDictionary"; }

    void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const override;

    void serializeBinaryBulkStatePrefix(
            SerializeBinaryBulkSettings & settings,
            SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
            SerializeBinaryBulkSettings & settings,
            SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
            DeserializeBinaryBulkSettings & settings,
            DeserializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
            const IColumn & column,
            size_t offset,
            size_t limit,
            SerializeBinaryBulkSettings & settings,
            SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
            IColumn & column,
            size_t limit,
            DeserializeBinaryBulkSettings & settings,
            DeserializeBinaryBulkStatePtr & state) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;

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

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return dictionary_type->getDefault(); }

    bool equals(const IDataType & rhs) const override;

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
    bool withDictionary() const override { return true; }

    static MutableColumnUniquePtr createColumnUnique(const IDataType & keys_type, const IDataType & indexes_type);

private:

    template <typename ... Args>
    using SerealizeFunctionPtr = void (IDataType::*)(const IColumn &, size_t, WriteBuffer &, Args & ...) const;

    template <typename ... Args>
    void serializeImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
                       SerealizeFunctionPtr<Args ...> func, Args & ... args) const;

    template <typename ... Args>
    using DeserealizeFunctionPtr = void (IDataType::*)(IColumn &, ReadBuffer &, Args ...) const;

    template <typename ... Args>
    void deserializeImpl(IColumn & column, ReadBuffer & istr,
                         DeserealizeFunctionPtr<Args ...> func, Args ... args) const;

    template <typename ColumnType, typename IndexType>
    static MutableColumnUniquePtr createColumnUniqueImpl(const IDataType & keys_type);

    template <typename ColumnType>
    static MutableColumnUniquePtr createColumnUniqueImpl(const IDataType & keys_type, const IDataType & indexes_type);


    friend struct CreateColumnVector;
};

}
