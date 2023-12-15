#pragma once

#include <memory>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/EnumValues.h>
#include <DataTypes/DataTypeEnum.h>

namespace DB
{

template <typename Type>
class SerializationEnum : public SerializationNumber<Type>
{
public:
    using typename SerializationNumber<Type>::FieldType;
    using typename SerializationNumber<Type>::ColumnType;
    using Values = EnumValues<Type>::Values;

    SerializationEnum() = delete;
    /// To explicitly create a SerializationEnum from Values
    explicit SerializationEnum(const Values & values_) : own_enum_values(values_), ref_enum_values(&own_enum_values.value()) { }
    /// To create a SerializationEnum from an IDataType instance, will reuse EnumValues from the type
    /// Motivation: some Enum type has many elements, and building EnumValues is not trivial
    /// This constructor allow to create many SerializationEnum from same IDataType without rebuilding
    /// EnumValues for every call, so it's useful to get default serialization.
    explicit SerializationEnum(const std::shared_ptr<const DataTypeEnum<Type>> & enum_type)
        : own_enum_type(enum_type), ref_enum_values(static_cast<const EnumValues<Type> *>(enum_type.get()))
    {
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    FieldType readValue(ReadBuffer & istr) const
    {
        FieldType x;
        readText(x, istr);
        return ref_enum_values->findByValue(x)->first;
    }

    std::optional<EnumValues<Type>> own_enum_values;
    std::shared_ptr<const DataTypeEnum<Type>> own_enum_type;
    const EnumValues<Type> * ref_enum_values;
};

}
