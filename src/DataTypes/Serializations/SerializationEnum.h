#pragma once

#include <memory>
#include <typeinfo>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <DataTypes/EnumValues.h>
#include <DataTypes/DataTypeEnum.h>

namespace DB
{

template <typename Type>
class SerializationEnum : public SerializationNumber<Type>
{
private:
    using typename SerializationNumber<Type>::FieldType;
    using typename SerializationNumber<Type>::ColumnType;
    using Values = typename EnumValues<Type>::Values;

    // SerializationEnum can be constructed in two ways:
    /// - Make a copy of the Enum name-to-type mapping.
    /// - Only store a reference to an existing mapping. This is faster if the Enum has a lot of different values or if SerializationEnum is
    ///   constructed very frequently. Make sure that the pointed-to mapping has a longer lifespan than SerializationEnum!

    explicit SerializationEnum(const Values & values_)
        : own_enum_values(values_), ref_enum_values(own_enum_values.value())
    {
    }

    explicit SerializationEnum(const std::shared_ptr<const DataTypeEnum<Type>> & enum_type)
        : own_enum_type(enum_type), ref_enum_values(*enum_type)
    {
    }

public:
    static SerializationPtr create(const std::shared_ptr<const DataTypeEnum<Type>> & enum_type)
    {
        auto ptr = SerializationPtr(new SerializationEnum(enum_type));
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    static SerializationPtr create(const Values & values_)
    {
        auto ptr = SerializationPtr(new SerializationEnum(values_));
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    ~SerializationEnum() override;

    String getName() const override { return String(typeid(Type).name()) + "_Enum"; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    FieldType readValue(ReadBuffer & istr) const
    {
        FieldType x;
        readText(x, istr);
        return ref_enum_values.findByValue(x)->first;
    }

    bool tryReadValue(ReadBuffer & istr, FieldType & x) const
    {
       return tryReadText(x, istr) && ref_enum_values.hasValue(x);
    }

    std::optional<EnumValues<Type>> own_enum_values;
    std::shared_ptr<const DataTypeEnum<Type>> own_enum_type;
    const EnumValues<Type> & ref_enum_values;
};

}
