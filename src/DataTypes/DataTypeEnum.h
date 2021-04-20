#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Common/HashTable/HashMap.h>
#include <vector>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


class IDataTypeEnum : public IDataType
{
public:
    virtual Field castToName(const Field & value_or_name) const = 0;
    virtual Field castToValue(const Field & value_or_name) const = 0;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool isComparable() const override { return true; }
};


template <typename Type>
class DataTypeEnum final : public IDataTypeEnum
{
public:
    using FieldType = Type;
    using ColumnType = ColumnVector<FieldType>;
    using Value = std::pair<std::string, FieldType>;
    using Values = std::vector<Value>;
    using NameToValueMap = HashMap<StringRef, FieldType, StringRefHash>;
    using ValueToNameMap = std::unordered_map<FieldType, StringRef>;

    static constexpr bool is_parametric = true;

private:
    Values values;
    NameToValueMap name_to_value_map;
    ValueToNameMap value_to_name_map;
    std::string type_name;

    static std::string generateName(const Values & values);
    void fillMaps();

public:
    explicit DataTypeEnum(const Values & values_);

    const Values & getValues() const { return values; }
    std::string doGetName() const override { return type_name; }
    const char * getFamilyName() const override;

    TypeIndex getTypeId() const override { return sizeof(FieldType) == 1 ? TypeIndex::Enum8 : TypeIndex::Enum16; }

    auto findByValue(const FieldType & value) const
    {
        const auto it = value_to_name_map.find(value);
        if (it == std::end(value_to_name_map))
            throw Exception{"Unexpected value " + toString(value) + " for type " + getName(), ErrorCodes::BAD_ARGUMENTS};

        return it;
    }

    const StringRef & getNameForValue(const FieldType & value) const
    {
        return findByValue(value)->second;
    }

    FieldType getValue(StringRef field_name, bool try_treat_as_id = false) const
    {
        const auto it = name_to_value_map.find(field_name);
        if (!it)
        {
            /// It is used in CSV and TSV input formats. If we fail to find given string in
            /// enum names, we will try to treat it as enum id.
            if (try_treat_as_id)
            {
                FieldType x;
                ReadBufferFromMemory tmp_buf(field_name.data, field_name.size);
                readText(x, tmp_buf);
                /// Check if we reached end of the tmp_buf (otherwise field_name is not a number)
                /// and try to find it in enum ids
                if (tmp_buf.eof() && value_to_name_map.find(x) != value_to_name_map.end())
                    return x;
            }
            throw Exception{"Unknown element '" + field_name.toString() + "' for type " + getName(), ErrorCodes::BAD_ARGUMENTS};
        }
        return it->getMapped();
    }

    FieldType readValue(ReadBuffer & istr) const
    {
        FieldType x;
        readText(x, istr);
        return findByValue(x)->first;
    }

    Field castToName(const Field & value_or_name) const override;
    Field castToValue(const Field & value_or_name) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
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

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, const size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, const size_t limit, const double avg_value_size_hint) const override;

    MutableColumnPtr createColumn() const override { return ColumnType::create(); }

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;

    bool equals(const IDataType & rhs) const override;

    bool textCanContainOnlyValidUTF8() const override;
    size_t getSizeOfValueInMemory() const override { return sizeof(FieldType); }

    /// Check current Enum type extends another Enum type (contains all the same values and doesn't override name's with other values)
    /// Example:
    /// Enum('a' = 1, 'b' = 2) -> Enum('c' = 1, 'b' = 2, 'd' = 3) OK
    /// Enum('a' = 1, 'b' = 2) -> Enum('a' = 2, 'b' = 1) NOT OK
    bool contains(const IDataType & rhs) const;
};


using DataTypeEnum8 = DataTypeEnum<Int8>;
using DataTypeEnum16 = DataTypeEnum<Int16>;

}
