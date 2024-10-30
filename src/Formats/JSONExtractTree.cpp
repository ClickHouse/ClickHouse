#include <Formats/JSONExtractTree.h>
#include <Formats/SchemaInferenceUtils.h>

#include <Core/AccurateComparison.h>
#if USE_SIMDJSON
#include <Common/JSONParsers/SimdJSONParser.h>
#endif
#if USE_RAPIDJSON
#include <Common/JSONParsers/RapidJSONParser.h>
#endif

#include <Common/JSONParsers/DummyJSONParser.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <DataTypes/Serializations/SerializationVariant.h>


#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename JSONParser>
void jsonElementToString(const typename JSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings)
{
    if (element.isInt64())
    {
        writeIntText(element.getInt64(), buf);
        return;
    }
    if (element.isUInt64())
    {
        writeIntText(element.getUInt64(), buf);
        return;
    }
    if (element.isDouble())
    {
        writeFloatText(element.getDouble(), buf);
        return;
    }
    if (element.isBool())
    {
        if (element.getBool())
            writeCString("true", buf);
        else
            writeCString("false", buf);
        return;
    }
    if (element.isString())
    {
        writeJSONString(element.getString(), buf, format_settings);
        return;
    }
    if (element.isArray())
    {
        writeChar('[', buf);
        bool need_comma = false;
        for (auto value : element.getArray())
        {
            if (std::exchange(need_comma, true))
                writeChar(',', buf);
            jsonElementToString<JSONParser>(value, buf, format_settings);
        }
        writeChar(']', buf);
        return;
    }
    if (element.isObject())
    {
        writeChar('{', buf);
        bool need_comma = false;
        for (auto [key, value] : element.getObject())
        {
            if (std::exchange(need_comma, true))
                writeChar(',', buf);
            writeJSONString(key, buf, format_settings);
            writeChar(':', buf);
            jsonElementToString<JSONParser>(value, buf, format_settings);
        }
        writeChar('}', buf);
        return;
    }
    if (element.isNull())
    {
        writeCString("null", buf);
        return;
    }
}

template <typename JSONParser, typename NumberType>
bool tryGetNumericValueFromJSONElement(
    NumberType & value, const typename JSONParser::Element & element, bool convert_bool_to_integer, String & error)
{
    switch (element.type())
    {
        case ElementType::DOUBLE:
            if constexpr (std::is_floating_point_v<NumberType>)
            {
                /// We permit inaccurate conversion of double to float.
                /// Example: double 0.1 from JSON is not representable in float.
                /// But it will be more convenient for user to perform conversion.
                value = static_cast<NumberType>(element.getDouble());
            }
            else if (!accurate::convertNumeric<Float64, NumberType, false>(element.getDouble(), value))
            {
                error = fmt::format("cannot convert double value {} to {}", element.getDouble(), TypeName<NumberType>);
                return false;
            }
            break;
        case ElementType::UINT64:
            if (!accurate::convertNumeric<UInt64, NumberType, false>(element.getUInt64(), value))
            {
                error = fmt::format("cannot convert UInt64 value {} to {}", element.getUInt64(), TypeName<NumberType>);
                return false;
            }
            break;
        case ElementType::INT64:
            if (!accurate::convertNumeric<Int64, NumberType, false>(element.getInt64(), value))
            {
                error = fmt::format("cannot convert Int64 value {} to {}", element.getInt64(), TypeName<NumberType>);
                return false;
            }
            break;
        case ElementType::BOOL:
            if constexpr (is_integer<NumberType>)
            {
                if (convert_bool_to_integer)
                {
                    value = static_cast<NumberType>(element.getBool());
                    break;
                }
            }
            error = fmt::format("cannot convert bool value to {}", TypeName<NumberType>);
            return false;
        case ElementType::STRING: {
            auto rb = ReadBufferFromMemory{element.getString()};
            if constexpr (std::is_floating_point_v<NumberType>)
            {
                if (!tryReadFloatText(value, rb) || !rb.eof())
                {
                    error = fmt::format("cannot parse {} value here: {}", TypeName<NumberType>, element.getString());
                    return false;
                }
            }
            else
            {
                if (tryReadIntText(value, rb) && rb.eof())
                    break;

                /// Try to parse float and convert it to integer.
                Float64 tmp_float;
                rb.position() = rb.buffer().begin();
                if (!tryReadFloatText(tmp_float, rb) || !rb.eof())
                {
                    error = fmt::format("cannot parse {} value here: {}", TypeName<NumberType>, element.getString());
                    return false;
                }

                if (!accurate::convertNumeric<Float64, NumberType, false>(tmp_float, value))
                {
                    error = fmt::format("cannot parse {} value here: {}", TypeName<NumberType>, element.getString());
                    return false;
                }
            }
            break;
        }
        default:
            return false;
    }

    return true;
}

namespace
{

template <typename JSONParser>
String jsonElementToString(const typename JSONParser::Element & element, const FormatSettings & format_settings)
{
    WriteBufferFromOwnString buf;
    jsonElementToString<JSONParser>(element, buf, format_settings);
    return buf.str();
}

template <typename JSONParser, typename NumberType>
class NumericNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit NumericNode(bool is_bool_type_ = false) : is_bool_type(is_bool_type_) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }

            error = fmt::format("cannot parse {} value from null", TypeName<NumberType>);
            return false;
        }

        NumberType value;
        if (!tryGetNumericValueFromJSONElement<JSONParser, NumberType>(value, element, insert_settings.convert_bool_to_integer || is_bool_type, error))
        {
            if (error.empty())
                error = fmt::format("cannot read {} value from JSON element: {}", TypeName<NumberType>, jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        if (is_bool_type)
            value = static_cast<bool>(value);

        auto & col_vec = assert_cast<ColumnVector<NumberType> &>(column);
        col_vec.insertValue(value);
        return true;
    }

protected:
    bool is_bool_type;
};

template <typename JSONParser, typename NumberType>
class LowCardinalityNumericNode : public NumericNode<JSONParser, NumberType>
{
public:
    explicit LowCardinalityNumericNode(bool is_nullable_, bool is_bool_type_ = false)
        : NumericNode<JSONParser, NumberType>(is_bool_type_), is_nullable(is_nullable_)
    {
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (is_nullable || format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }

            error = fmt::format("cannot parse {} value from null", TypeName<NumberType>);
            return false;
        }

        NumberType value;
        if (!tryGetNumericValueFromJSONElement<JSONParser, NumberType>(value, element, insert_settings.convert_bool_to_integer || this->is_bool_type, error))
        {
            if (error.empty())
                error = fmt::format("cannot read {} value from JSON element: {}", TypeName<NumberType>, jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        if (this->is_bool_type)
            value = static_cast<bool>(value);

        auto & col_lc = assert_cast<ColumnLowCardinality &>(column);
        col_lc.insertData(reinterpret_cast<const char *>(&value), sizeof(value));
        return true;
    }

private:
    bool is_nullable;
};

template <typename JSONParser>
class StringNode : public JSONExtractTreeNode<JSONParser>
{
public:
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }
            error = "cannot parse String value from null";
            return false;
        }

        if (!element.isString())
        {
            auto & col_str = assert_cast<ColumnString &>(column);
            auto & chars = col_str.getChars();
            WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
            jsonElementToString<JSONParser>(element, buf, format_settings);
            buf.finalize();
            chars.push_back(0);
            col_str.getOffsets().push_back(chars.size());
        }
        else
        {
            auto value = element.getString();
            auto & col_str = assert_cast<ColumnString &>(column);
            col_str.insertData(value.data(), value.size());
        }
        return true;
    }
};

template <typename JSONParser>
class LowCardinalityStringNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit LowCardinalityStringNode(bool is_nullable_) : is_nullable(is_nullable_) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (is_nullable || format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }

            error = "cannot parse String value from null";
            return false;
        }

        if (!element.isString())
        {
            auto value = jsonElementToString<JSONParser>(element, format_settings);
            assert_cast<ColumnLowCardinality &>(column).insertData(value.data(), value.size());
        }
        else
        {
            auto value = element.getString();
            assert_cast<ColumnLowCardinality &>(column).insertData(value.data(), value.size());
        }

        return true;
    }

private:
    bool is_nullable;
};

template <typename JSONParser>
class FixedStringNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit FixedStringNode(size_t fixed_length_) : fixed_length(fixed_length_) { }
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }

            error = "cannot parse FixedString value from null";
            return false;
        }

        if (!element.isString())
            return checkValueSizeAndInsert(column, jsonElementToString<JSONParser>(element, format_settings), error);
        return checkValueSizeAndInsert(column, element.getString(), error);
    }

private:
    template <typename T>
    bool checkValueSizeAndInsert(IColumn & column, const T & value, String & error) const
    {
        if (value.size() > fixed_length)
        {
            error = fmt::format("too large string for FixedString({}): {}", fixed_length, value);
            return false;
        }
        assert_cast<ColumnFixedString &>(column).insertData(value.data(), value.size());
        return true;
    }

    size_t fixed_length;
};

template <typename JSONParser>
class LowCardinalityFixedStringNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit LowCardinalityFixedStringNode(bool is_nullable_, size_t fixed_length_) : is_nullable(is_nullable_), fixed_length(fixed_length_)
    {
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (is_nullable || format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }
            error = "cannot parse FixedString value from null";
            return false;
        }

        if (!element.isString())
            return checkValueSizeAndInsert(column, jsonElementToString<JSONParser>(element, format_settings), error);
        return checkValueSizeAndInsert(column, element.getString(), error);
    }

private:
    template <typename T>
    bool checkValueSizeAndInsert(IColumn & column, const T & value, String & error) const
    {
        if (value.size() > fixed_length)
        {
            error = fmt::format("too large string for FixedString({}): {}", fixed_length, value);
            return false;
        }

        // For the non low cardinality case of FixedString, the padding is done in the FixedString Column implementation.
        // In order to avoid having to pass the data to a FixedString Column and read it back (which would slow down the execution)
        // the data is padded here and written directly to the Low Cardinality Column
        if (value.size() == fixed_length)
        {
            assert_cast<ColumnLowCardinality &>(column).insertData(value.data(), value.size());
        }
        else
        {
            String padded_value(value);
            padded_value.resize(fixed_length, '\0');
            assert_cast<ColumnLowCardinality &>(column).insertData(padded_value.data(), padded_value.size());
        }
        return true;
    }

    bool is_nullable;
    size_t fixed_length;
};

template <typename JSONParser>
class UUIDNode : public JSONExtractTreeNode<JSONParser>
{
public:
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        if (!element.isString())
        {
            error = fmt::format("cannot read UUID value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto data = element.getString();
        UUID uuid;
        if (!tryParse(uuid, data))
        {
            error = fmt::format("cannot parse UUID value here: {}", data);
            return false;
        }

        assert_cast<ColumnUUID &>(column).insert(uuid);
        return true;
    }


    static bool tryParse(UUID & uuid, std::string_view data)
    {
        ReadBufferFromMemory buf(data.data(), data.size());
        return tryReadUUIDText(uuid, buf) && buf.eof();
    }
};

template <typename JSONParser>
class LowCardinalityUUIDNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit LowCardinalityUUIDNode(bool is_nullable_) : is_nullable(is_nullable_) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && (is_nullable || format_settings.null_as_default))
        {
            column.insertDefault();
            return true;
        }

        if (!element.isString())
        {
            error = fmt::format("cannot read UUID value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto data = element.getString();
        ReadBufferFromMemory buf(data.data(), data.size());
        UUID uuid;
        if (!tryReadUUIDText(uuid, buf) || !buf.eof())
        {
            error = fmt::format("cannot parse UUID value here: {}", data);
            return false;
        }
        assert_cast<ColumnLowCardinality &>(column).insertData(reinterpret_cast<const char *>(&uuid), sizeof(uuid));
        return true;
    }

private:
    bool is_nullable;
};

template <typename JSONParser, typename DateType, typename ColumnNumericType>
class DateNode : public JSONExtractTreeNode<JSONParser>
{
public:
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        if (!element.isString())
        {
            error = fmt::format("cannot read Date value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto data = element.getString();
        ReadBufferFromMemory buf(data.data(), data.size());
        DateType date;
        if (!tryReadDateText(date, buf) || !buf.eof())
        {
            error = fmt::format("cannot parse Date value here: {}", data);
            return false;
        }

        assert_cast<ColumnVector<ColumnNumericType> &>(column).insertValue(date);
        return true;
    }
};

template <typename JSONParser>
class DateTimeNode : public JSONExtractTreeNode<JSONParser>, public TimezoneMixin
{
public:
    explicit DateTimeNode(const DataTypeDateTime & datetime_type) : TimezoneMixin(datetime_type) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        time_t value;
        if (element.isString())
        {
            if (!tryParse(value, element.getString(), format_settings.date_time_input_format))
            {
                error = fmt::format("cannot parse DateTime value here: {}", element.getString());
                return false;
            }
        }
        else if (element.isUInt64())
        {
            value = element.getUInt64();
        }
        else
        {
            error = fmt::format("cannot read DateTime value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        assert_cast<ColumnDateTime &>(column).insert(value);
        return true;
    }

    bool tryParse(time_t & value, std::string_view data, FormatSettings::DateTimeInputFormat date_time_input_format) const
    {
        ReadBufferFromMemory buf(data.data(), data.size());
        switch (date_time_input_format)
        {
            case FormatSettings::DateTimeInputFormat::Basic:
                if (tryReadDateTimeText(value, buf, time_zone) && buf.eof())
                    return true;
                break;
            case FormatSettings::DateTimeInputFormat::BestEffort:
                if (tryParseDateTimeBestEffort(value, buf, time_zone, utc_time_zone) && buf.eof())
                    return true;
                break;
            case FormatSettings::DateTimeInputFormat::BestEffortUS:
                if (tryParseDateTimeBestEffortUS(value, buf, time_zone, utc_time_zone) && buf.eof())
                    return true;
                break;
        }

        return false;
    }
};

template <typename JSONParser, typename DecimalType>
class DecimalNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit DecimalNode(const DataTypePtr & type) : scale(assert_cast<const DataTypeDecimal<DecimalType> &>(*type).getScale()) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        DecimalType value{};

        switch (element.type())
        {
            case ElementType::DOUBLE:
                value = convertToDecimal<DataTypeNumber<Float64>, DataTypeDecimal<DecimalType>>(element.getDouble(), scale);
                break;
            case ElementType::UINT64:
                value = convertToDecimal<DataTypeNumber<UInt64>, DataTypeDecimal<DecimalType>>(element.getUInt64(), scale);
                break;
            case ElementType::INT64:
                value = convertToDecimal<DataTypeNumber<Int64>, DataTypeDecimal<DecimalType>>(element.getInt64(), scale);
                break;
            case ElementType::STRING: {
                auto rb = ReadBufferFromMemory{element.getString()};
                if (!SerializationDecimal<DecimalType>::tryReadText(value, rb, DecimalUtils::max_precision<DecimalType>, scale))
                {
                    error = fmt::format("cannot parse Decimal value here: {}", element.getString());
                    return false;
                }
                break;
            }
            case ElementType::NULL_VALUE: {
                if (!format_settings.null_as_default)
                {
                    error = "cannot convert null to Decimal value";
                    return false;
                }
                break;
            }
            default:
            {
                error = fmt::format("cannot read Decimal value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
                return false;
            }
        }

        assert_cast<ColumnDecimal<DecimalType> &>(column).insertValue(value);
        return true;
    }

private:
    UInt32 scale;
};


template <typename JSONParser>
class DateTime64Node : public JSONExtractTreeNode<JSONParser>, public TimezoneMixin
{
public:
    explicit DateTime64Node(const DataTypeDateTime64 & datetime64_type) : TimezoneMixin(datetime64_type), scale(datetime64_type.getScale())
    {
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        DateTime64 value;
        if (element.isString())
        {
            if (!tryParse(value, element.getString(), format_settings.date_time_input_format))
            {
                error = fmt::format("cannot parse DateTime64 value here: {}", element.getString());
                return false;
            }
        }
        else
        {
            switch (element.type())
            {
                case ElementType::DOUBLE:
                    value = convertToDecimal<DataTypeNumber<Float64>, DataTypeDecimal<DateTime64>>(element.getDouble(), scale);
                    break;
                case ElementType::UINT64:
                    value = convertToDecimal<DataTypeNumber<UInt64>, DataTypeDecimal<DateTime64>>(element.getUInt64(), scale);
                    break;
                case ElementType::INT64:
                    value = convertToDecimal<DataTypeNumber<Int64>, DataTypeDecimal<DateTime64>>(element.getInt64(), scale);
                    break;
                default:
                    error = fmt::format("cannot read DateTime64 value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
                    return false;
            }
        }

        assert_cast<ColumnDateTime64 &>(column).insert(value);
        return true;
    }

    bool tryParse(DateTime64 & value, std::string_view data, FormatSettings::DateTimeInputFormat date_time_input_format) const
    {
        ReadBufferFromMemory buf(data.data(), data.size());
        switch (date_time_input_format)
        {
            case FormatSettings::DateTimeInputFormat::Basic:
                if (tryReadDateTime64Text(value, scale, buf, time_zone) && buf.eof())
                    return true;
                break;
            case FormatSettings::DateTimeInputFormat::BestEffort:
                if (tryParseDateTime64BestEffort(value, scale, buf, time_zone, utc_time_zone) && buf.eof())
                    return true;
                break;
            case FormatSettings::DateTimeInputFormat::BestEffortUS:
                if (tryParseDateTime64BestEffortUS(value, scale, buf, time_zone, utc_time_zone) && buf.eof())
                    return true;
                break;
        }

        return false;
    }

private:
    UInt32 scale;
};

template <typename JSONParser, typename Type>
class EnumNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit EnumNode(const std::vector<std::pair<String, Type>> & name_value_pairs_) : name_value_pairs(name_value_pairs_)
    {
        for (const auto & name_value_pair : name_value_pairs)
        {
            name_to_value_map.emplace(name_value_pair.first, name_value_pair.second);
            only_values.emplace(name_value_pair.second);
        }
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            if (format_settings.null_as_default)
            {
                column.insertDefault();
                return true;
            }

            error = "cannot convert null to Enum value";
            return false;
        }

        auto & col_vec = assert_cast<ColumnVector<Type> &>(column);

        if (element.isInt64())
        {
            Type value;
            if (!accurate::convertNumeric(element.getInt64(), value) || !only_values.contains(value))
            {
                error = fmt::format("cannot convert value {} to enum: there is no such value in enum", element.getInt64());
                return false;
            }
            col_vec.insertValue(value);
            return true;
        }

        if (element.isUInt64())
        {
            Type value;
            if (!accurate::convertNumeric(element.getUInt64(), value) || !only_values.contains(value))
            {
                error = fmt::format("cannot convert value {} to enum: there is no such value in enum", element.getUInt64());
                return false;
            }
            col_vec.insertValue(value);
            return true;
        }

        if (element.isString())
        {
            auto value = name_to_value_map.find(element.getString());
            if (value == name_to_value_map.end())
            {
                error = fmt::format("cannot convert value {} to enum: there is no such value in enum", element.getString());
                return false;
            }
            col_vec.insertValue(value->second);
            return true;
        }

        error = fmt::format("cannot read Enum value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
        return false;
    }

private:
    std::vector<std::pair<String, Type>> name_value_pairs;
    std::unordered_map<std::string_view, Type> name_to_value_map;
    std::unordered_set<Type> only_values;
};

template <typename JSONParser>
class IPv4Node : public JSONExtractTreeNode<JSONParser>
{
public:
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        if (!element.isString())
        {
            error = fmt::format("cannot read IPv4 value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto data = element.getString();
        IPv4 value;
        if (!tryParse(value, data))
        {
            error = fmt::format("cannot parse IPv4 value here: {}", data);
            return false;
        }

        assert_cast<ColumnIPv4 &>(column).insert(value);
        return true;
    }

    static bool tryParse(IPv4 & value, std::string_view data)
    {
        ReadBufferFromMemory buf(data.data(), data.size());
        return tryReadIPv4Text(value, buf) && buf.eof();
    }
};

template <typename JSONParser>
class IPv6Node : public JSONExtractTreeNode<JSONParser>
{
public:
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings &,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        if (!element.isString())
        {
            error = fmt::format("cannot read IPv6 value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto data = element.getString();
        IPv6 value;
        if (!tryParse(value, data))
        {
            error = fmt::format("cannot parse IPv6 value here: {}", data);
            return false;
        }

        assert_cast<ColumnIPv6 &>(column).insert(value);
        return true;
    }


    static bool tryParse(IPv6 & value, std::string_view data)
    {
        ReadBufferFromMemory buf(data.data(), data.size());
        return tryReadIPv6Text(value, buf) && buf.eof();
    }
};

template <typename JSONParser>
class NullableNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit NullableNode(std::unique_ptr<JSONExtractTreeNode<JSONParser>> nested_) : nested(std::move(nested_)) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull())
        {
            column.insertDefault();
            return true;
        }

        auto & col_null = assert_cast<ColumnNullable &>(column);
        if (!nested->insertResultToColumn(col_null.getNestedColumn(), element, insert_settings, format_settings, error))
            return false;
        col_null.getNullMapColumn().insertValue(0);
        return true;
    }

private:
    std::unique_ptr<JSONExtractTreeNode<JSONParser>> nested;
};

template <typename JSONParser>
class LowCardinalityNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit LowCardinalityNode(bool is_nullable_, std::unique_ptr<JSONExtractTreeNode<JSONParser>> nested_)
        : is_nullable(is_nullable_), nested(std::move(nested_))
    {
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && (is_nullable || format_settings.null_as_default))
        {
            column.insertDefault();
            return true;
        }

        auto & col_lc = assert_cast<ColumnLowCardinality &>(column);
        auto tmp_nested = col_lc.getDictionary().getNestedColumn()->cloneEmpty();
        if (!nested->insertResultToColumn(*tmp_nested, element, insert_settings, format_settings, error))
            return false;

        col_lc.insertFromFullColumn(*tmp_nested, 0);
        return true;
    }

private:
    bool is_nullable;
    std::unique_ptr<JSONExtractTreeNode<JSONParser>> nested;
};

template <typename JSONParser>
class ArrayNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit ArrayNode(std::unique_ptr<JSONExtractTreeNode<JSONParser>> nested_) : nested(std::move(nested_)) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (element.isNull() && format_settings.null_as_default)
        {
            column.insertDefault();
            return true;
        }

        if (!element.isArray())
        {
            error = fmt::format("cannot read Array value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto array = element.getArray();

        auto & col_arr = assert_cast<ColumnArray &>(column);
        auto & data = col_arr.getData();
        size_t old_size = data.size();
        bool were_valid_elements = false;

        for (auto value : array)
        {
            if (nested->insertResultToColumn(data, value, insert_settings, format_settings, error))
            {
                were_valid_elements = true;
            }
            else if (insert_settings.insert_default_on_invalid_elements_in_complex_types)
            {
                data.insertDefault();
            }
            else
            {
                data.popBack(data.size() - old_size);
                return false;
            }
        }

        if (!were_valid_elements)
        {
            data.popBack(data.size() - old_size);
            return false;
        }

        col_arr.getOffsets().push_back(data.size());
        return true;
    }

private:
    std::unique_ptr<JSONExtractTreeNode<JSONParser>> nested;
};

template <typename JSONParser>
class TupleNode : public JSONExtractTreeNode<JSONParser>
{
public:
    TupleNode(std::vector<std::unique_ptr<JSONExtractTreeNode<JSONParser>>> nested_, const std::vector<String> & explicit_names_)
        : nested(std::move(nested_)), explicit_names(explicit_names_)
    {
        for (size_t i = 0; i != explicit_names.size(); ++i)
            name_to_index_map.emplace(explicit_names[i], i);
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        auto & tuple = assert_cast<ColumnTuple &>(column);
        size_t old_size = column.size();
        bool were_valid_elements = false;

        auto set_size = [&](size_t size)
        {
            for (size_t i = 0; i != tuple.tupleSize(); ++i)
            {
                auto & col = tuple.getColumn(i);
                if (col.size() != size)
                {
                    if (col.size() > size)
                        col.popBack(col.size() - size);
                    else
                        while (col.size() < size)
                            col.insertDefault();
                }
            }
        };

        if (element.isArray())
        {
            auto array = element.getArray();
            auto it = array.begin();

            for (size_t index = 0; (index != nested.size()) && (it != array.end()); ++index)
            {
                if (nested[index]->insertResultToColumn(tuple.getColumn(index), *it++, insert_settings, format_settings, error))
                {
                    were_valid_elements = true;
                }
                else if (insert_settings.insert_default_on_invalid_elements_in_complex_types)
                {
                    tuple.getColumn(index).insertDefault();
                }
                else
                {
                    set_size(old_size);
                    error += fmt::format("(during reading tuple {} element)", index);
                    return false;
                }
            }

            set_size(old_size + static_cast<size_t>(were_valid_elements));
            return were_valid_elements;
        }

        if (element.isObject())
        {
            auto object = element.getObject();
            if (name_to_index_map.empty())
            {
                auto it = object.begin();
                for (size_t index = 0; (index != nested.size()) && (it != object.end()); ++index)
                {
                    if (nested[index]->insertResultToColumn(tuple.getColumn(index), (*it++).second, insert_settings, format_settings, error))
                    {
                        were_valid_elements = true;
                    }
                    else if (insert_settings.insert_default_on_invalid_elements_in_complex_types)
                    {
                        tuple.getColumn(index).insertDefault();
                    }
                    else
                    {
                        set_size(old_size);
                        error += fmt::format("(during reading tuple {} element)", index);
                        return false;
                    }
                }
            }
            else
            {
                for (const auto & [key, value] : object)
                {
                    auto index = name_to_index_map.find(key);
                    if (index != name_to_index_map.end())
                    {
                        if (nested[index->second]->insertResultToColumn(tuple.getColumn(index->second), value, insert_settings, format_settings, error))
                        {
                            were_valid_elements = true;
                        }
                        else if (!insert_settings.insert_default_on_invalid_elements_in_complex_types)
                        {
                            set_size(old_size);
                            error += fmt::format("(during reading tuple element \"{}\")", key);
                            return false;
                        }
                    }
                }
            }

            set_size(old_size + static_cast<size_t>(were_valid_elements));
            return were_valid_elements;
        }

        error = fmt::format("cannot read Tuple value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
        return false;
    }

private:
    std::vector<std::unique_ptr<JSONExtractTreeNode<JSONParser>>> nested;
    std::vector<String> explicit_names;
    std::unordered_map<std::string_view, size_t> name_to_index_map;
};

template <typename JSONParser>
class MapNode : public JSONExtractTreeNode<JSONParser>
{
public:
    explicit MapNode(std::unique_ptr<JSONExtractTreeNode<JSONParser>> value_) : value(std::move(value_)) { }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        if (!element.isObject())
        {
            error = fmt::format("cannot read Map value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
            return false;
        }

        auto & map_col = assert_cast<ColumnMap &>(column);
        auto & offsets = map_col.getNestedColumn().getOffsets();
        auto & tuple_col = map_col.getNestedData();
        auto & key_col = tuple_col.getColumn(0);
        auto & value_col = tuple_col.getColumn(1);
        size_t old_size = tuple_col.size();

        auto object = element.getObject();
        auto it = object.begin();
        for (; it != object.end(); ++it)
        {
            auto pair = *it;

            /// Insert key
            key_col.insertData(pair.first.data(), pair.first.size());

            /// Insert value
            if (!value->insertResultToColumn(value_col, pair.second, insert_settings, format_settings, error))
            {
                if (insert_settings.insert_default_on_invalid_elements_in_complex_types)
                {
                    value_col.insertDefault();
                }
                else
                {
                    key_col.popBack(key_col.size() - offsets.back());
                    value_col.popBack(value_col.size() - offsets.back());
                    error += fmt::format("(during reading value of key \"{}\")", pair.first);
                    return false;
                }
            }
        }

        offsets.push_back(old_size + object.size());
        return true;
    }

private:
    std::unique_ptr<JSONExtractTreeNode<JSONParser>> value;
};

template <typename JSONParser>
class VariantNode : public JSONExtractTreeNode<JSONParser>
{
public:
    VariantNode(std::vector<std::unique_ptr<JSONExtractTreeNode<JSONParser>>> variant_nodes_, std::vector<size_t> order_)
        : variant_nodes(std::move(variant_nodes_)), order(std::move(order_))
    {
    }

    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        auto & column_variant = assert_cast<ColumnVariant &>(column);
        for (size_t i : order)
        {
            auto & variant = column_variant.getVariantByGlobalDiscriminator(i);
            if (variant_nodes[i]->insertResultToColumn(variant, element, insert_settings, format_settings, error))
            {
                column_variant.getLocalDiscriminators().push_back(column_variant.localDiscriminatorByGlobal(i));
                column_variant.getOffsets().push_back(variant.size() - 1);
                return true;
            }
        }

        error = fmt::format("cannot read Map value from JSON element: {}", jsonElementToString<JSONParser>(element, format_settings));
        return false;
    }

private:
    std::vector<std::unique_ptr<JSONExtractTreeNode<JSONParser>>> variant_nodes;
    /// Order in which we should try variants nodes.
    /// For example, String should be always the last one.
    std::vector<size_t> order;
};


template <typename JSONParser>
class DynamicNode : public JSONExtractTreeNode<JSONParser>
{
public:
    bool insertResultToColumn(
        IColumn & column,
        const typename JSONParser::Element & element,
        const JSONExtractInsertSettings & insert_settings,
        const FormatSettings & format_settings,
        String & error) const override
    {
        auto & column_dynamic = assert_cast<ColumnDynamic &>(column);
        /// First, check if element is NULL.
        if (element.isNull())
        {
            column_dynamic.insertDefault();
            return true;
        }

        auto & variant_column = column_dynamic.getVariantColumn();
        auto variant_info = column_dynamic.getVariantInfo();
        /// Second, infer ClickHouse type for this element and add it as a new variant.
        auto element_type = elementToDataType(element, format_settings);
        if (column_dynamic.addNewVariant(element_type))
        {
            auto node = buildJSONExtractTree<JSONParser>(element_type, "Dynamic inference");
            auto global_discriminator = variant_info.variant_name_to_discriminator[element_type->getName()];
            auto & variant = variant_column.getVariantByGlobalDiscriminator(global_discriminator);
            if (!node->insertResultToColumn(variant, element, insert_settings, format_settings, error))
                return false;
            variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(global_discriminator));
            variant_column.getOffsets().push_back(variant.size() - 1);
            return true;
        }

        /// We couldn't add new variant. Try to insert element into current variants.
        auto variant_node = buildJSONExtractTree<JSONParser>(variant_info.variant_type, "Dynamic inference");
        if (variant_node->insertResultToColumn(variant_column, element, insert_settings, format_settings, error))
            return true;

        /// We couldn't insert element into any existing variant, add String variant and read value as String.
        column_dynamic.addStringVariant();
        auto string_global_discriminator = variant_info.variant_name_to_discriminator["String"];
        auto & string_column = variant_column.getVariantByGlobalDiscriminator(string_global_discriminator);
        if (!getStringNode()->insertResultToColumn(string_column, element, insert_settings, format_settings, error))
            return false;
        variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(string_global_discriminator));
        variant_column.getOffsets().push_back(string_column.size() - 1);
        return true;
    }

    static const std::unique_ptr<JSONExtractTreeNode<JSONParser>> & getStringNode()
    {
        static const std::unique_ptr<JSONExtractTreeNode<JSONParser>> string_node
            = buildJSONExtractTree<JSONParser>(std::make_shared<DataTypeString>(), "Dynamic inference");
        return string_node;
    }

    static DataTypePtr elementToDataType(const typename JSONParser::Element & element, const FormatSettings & format_settings)
    {
        JSONInferenceInfo json_inference_info;
        auto type = elementToDataTypeImpl(element, format_settings, json_inference_info);
        transformFinalInferredJSONTypeIfNeeded(type, format_settings, &json_inference_info);
        return type;
    }

private:
    static DataTypePtr elementToDataTypeImpl(const typename JSONParser::Element & element, const FormatSettings & format_settings, JSONInferenceInfo & json_inference_info)
    {
        switch (element.type())
        {
            case ElementType::NULL_VALUE:
                return makeNullable(std::make_shared<DataTypeNothing>());
            case ElementType::BOOL:
                return DataTypeFactory::instance().get("Bool");
            case ElementType::INT64:
            {
                auto type = std::make_shared<DataTypeInt64>();
                if (element.getInt64() < 0)
                    json_inference_info.negative_integers.insert(type.get());
                return type;
            }
            case ElementType::UINT64:
                return std::make_shared<DataTypeUInt64>();
            case ElementType::DOUBLE:
                return std::make_shared<DataTypeFloat64>();
            case ElementType::STRING:
            {
                auto data = element.getString();

                if (auto type = tryInferDateOrDateTimeFromString(data, format_settings))
                    return type;

                if (format_settings.json.try_infer_numbers_from_strings)
                {
                    if (auto type = tryInferJSONNumberFromString(data, format_settings, &json_inference_info))
                    {
                        json_inference_info.numbers_parsed_from_json_strings.insert(type.get());
                        return type;
                    }
                }

                return std::make_shared<DataTypeString>();
            }
            case ElementType::ARRAY:
            {
                auto array = element.getArray();
                DataTypes types;
                types.reserve(array.size());
                for (auto value : array)
                    types.push_back(makeNullableSafe(elementToDataTypeImpl(value, format_settings, json_inference_info)));

                if (types.empty())
                    return std::make_shared<DataTypeArray>(makeNullable(std::make_shared<DataTypeNothing>()));

                if (checkIfTypesAreEqual(types))
                    return std::make_shared<DataTypeArray>(types.back());

                /// For JSON if we have not complete types, we should not try to transform them
                /// and return it as a Tuple.
                /// For example, if we have types [Nullable(Float64), Nullable(Nothing), Nullable(Float64)]
                /// it can be Array(Nullable(Float64)) or Tuple(Nullable(Float64), <some_type>, Nullable(Float64)) and
                /// we can't determine which one it is right now. But we will be able to do it later
                /// when we will have the final top level type.
                /// For example, we can have JSON element [[42.42, null, 43.43], [44.44, "Some string", 45.45]] and we should
                /// determine the type for this element as Tuple(Nullable(Float64), Nullable(String), Nullable(Float64)).
                for (const auto & type : types)
                {
                    if (!checkIfTypeIsComplete(type))
                        return std::make_shared<DataTypeTuple>(types);
                }

                auto types_copy = types;
                transformInferredJSONTypesIfNeeded(types_copy, format_settings, &json_inference_info);

                if (checkIfTypesAreEqual(types_copy))
                    return std::make_shared<DataTypeArray>(types_copy.back());

                return std::make_shared<DataTypeTuple>(types);
            }
            case ElementType::OBJECT: {
                /// TODO: Use new JSON type here when it's ready.
                return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), makeNullable(std::make_shared<DataTypeString>()));
            }
        }
    }
};

}

template <typename JSONParser>
std::unique_ptr<JSONExtractTreeNode<JSONParser>> buildJSONExtractTree(const DataTypePtr & type, const char * source_for_exception_message)
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8:
            return std::make_unique<NumericNode<JSONParser, UInt8>>(isBool(type));
        case TypeIndex::UInt16:
            return std::make_unique<NumericNode<JSONParser, UInt16>>();
        case TypeIndex::UInt32:
            return std::make_unique<NumericNode<JSONParser, UInt32>>();
        case TypeIndex::UInt64:
            return std::make_unique<NumericNode<JSONParser, UInt64>>();
        case TypeIndex::UInt128:
            return std::make_unique<NumericNode<JSONParser, UInt128>>();
        case TypeIndex::UInt256:
            return std::make_unique<NumericNode<JSONParser, UInt256>>();
        case TypeIndex::Int8:
            return std::make_unique<NumericNode<JSONParser, Int8>>();
        case TypeIndex::Int16:
            return std::make_unique<NumericNode<JSONParser, Int16>>();
        case TypeIndex::Int32:
            return std::make_unique<NumericNode<JSONParser, Int32>>();
        case TypeIndex::Int64:
            return std::make_unique<NumericNode<JSONParser, Int64>>();
        case TypeIndex::Int128:
            return std::make_unique<NumericNode<JSONParser, Int128>>();
        case TypeIndex::Int256:
            return std::make_unique<NumericNode<JSONParser, Int256>>();
        case TypeIndex::Float32:
            return std::make_unique<NumericNode<JSONParser, Float32>>();
        case TypeIndex::Float64:
            return std::make_unique<NumericNode<JSONParser, Float64>>();
        case TypeIndex::String:
            return std::make_unique<StringNode<JSONParser>>();
        case TypeIndex::FixedString:
            return std::make_unique<FixedStringNode<JSONParser>>(assert_cast<const DataTypeFixedString &>(*type).getN());
        case TypeIndex::UUID:
            return std::make_unique<UUIDNode<JSONParser>>();
        case TypeIndex::IPv4:
            return std::make_unique<IPv4Node<JSONParser>>();
        case TypeIndex::IPv6:
            return std::make_unique<IPv6Node<JSONParser>>();
        case TypeIndex::Date:;
            return std::make_unique<DateNode<JSONParser, DayNum, UInt16>>();
        case TypeIndex::Date32:
            return std::make_unique<DateNode<JSONParser, ExtendedDayNum, Int32>>();
        case TypeIndex::DateTime:
            return std::make_unique<DateTimeNode<JSONParser>>(assert_cast<const DataTypeDateTime &>(*type));
        case TypeIndex::DateTime64:
            return std::make_unique<DateTime64Node<JSONParser>>(assert_cast<const DataTypeDateTime64 &>(*type));
        case TypeIndex::Decimal32:
            return std::make_unique<DecimalNode<JSONParser, Decimal32>>(type);
        case TypeIndex::Decimal64:
            return std::make_unique<DecimalNode<JSONParser, Decimal64>>(type);
        case TypeIndex::Decimal128:
            return std::make_unique<DecimalNode<JSONParser, Decimal128>>(type);
        case TypeIndex::Decimal256:
            return std::make_unique<DecimalNode<JSONParser, Decimal256>>(type);
        case TypeIndex::Enum8:
            return std::make_unique<EnumNode<JSONParser, Int8>>(assert_cast<const DataTypeEnum8 &>(*type).getValues());
        case TypeIndex::Enum16:
            return std::make_unique<EnumNode<JSONParser, Int16>>(assert_cast<const DataTypeEnum16 &>(*type).getValues());
        case TypeIndex::LowCardinality:
        {
            /// To optimize inserting into LowCardinality we have special nodes for LowCardinality of numeric and string types.
            const auto & lc_type = assert_cast<const DataTypeLowCardinality &>(*type);
            auto dictionary_type = removeNullable(lc_type.getDictionaryType());
            bool is_nullable = lc_type.isLowCardinalityNullable();

            switch (dictionary_type->getTypeId())
            {
                case TypeIndex::UInt8:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, UInt8>>(is_nullable, isBool(type));
                case TypeIndex::UInt16:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, UInt16>>(is_nullable);
                case TypeIndex::UInt32:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, UInt32>>(is_nullable);
                case TypeIndex::UInt64:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, UInt64>>(is_nullable);
                case TypeIndex::Int8:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, Int8>>(is_nullable);
                case TypeIndex::Int16:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, Int16>>(is_nullable);
                case TypeIndex::Int32:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, Int32>>(is_nullable);
                case TypeIndex::Int64:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, Int64>>(is_nullable);
                case TypeIndex::Float32:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, Float32>>(is_nullable);
                case TypeIndex::Float64:
                    return std::make_unique<LowCardinalityNumericNode<JSONParser, Float64>>(is_nullable);
                case TypeIndex::String:
                    return std::make_unique<LowCardinalityStringNode<JSONParser>>(is_nullable);
                case TypeIndex::FixedString:
                    return std::make_unique<LowCardinalityFixedStringNode<JSONParser>>(is_nullable, assert_cast<const DataTypeFixedString &>(*dictionary_type).getN());
                case TypeIndex::UUID:
                    return std::make_unique<LowCardinalityUUIDNode<JSONParser>>(is_nullable);
                default:
                    return std::make_unique<LowCardinalityNode<JSONParser>>(is_nullable, buildJSONExtractTree<JSONParser>(dictionary_type, source_for_exception_message));
            }
        }
        case TypeIndex::Nullable:
            return std::make_unique<NullableNode<JSONParser>>(buildJSONExtractTree<JSONParser>(assert_cast<const DataTypeNullable &>(*type).getNestedType(), source_for_exception_message));
        case TypeIndex::Array:
            return std::make_unique<ArrayNode<JSONParser>>(buildJSONExtractTree<JSONParser>(assert_cast<const DataTypeArray &>(*type).getNestedType(), source_for_exception_message));
        case TypeIndex::Tuple:
        {
            const auto & tuple = assert_cast<const DataTypeTuple &>(*type);
            const auto & tuple_elements = tuple.getElements();
            std::vector<std::unique_ptr<JSONExtractTreeNode<JSONParser>>> elements;
            elements.reserve(tuple_elements.size());
            for (const auto & tuple_element : tuple_elements)
                elements.emplace_back(buildJSONExtractTree<JSONParser>(tuple_element, source_for_exception_message));
            return std::make_unique<TupleNode<JSONParser>>(std::move(elements), tuple.haveExplicitNames() ? tuple.getElementNames() : Strings{});
        }
        case TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            const auto & key_type = map_type.getKeyType();
            if (!isString(removeLowCardinality(key_type)))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "{} doesn't support the return type schema: {} with key type not String",
                    source_for_exception_message,
                    type->getName());

            const auto & value_type = map_type.getValueType();
            return std::make_unique<MapNode<JSONParser>>(buildJSONExtractTree<JSONParser>(value_type, source_for_exception_message));
        }
        case TypeIndex::Variant:
        {
            const auto & variant_type = assert_cast<const DataTypeVariant &>(*type);
            const auto & variants = variant_type.getVariants();
            std::vector<std::unique_ptr<JSONExtractTreeNode<JSONParser>>> variant_nodes;
            variant_nodes.reserve(variants.size());
            for (const auto & variant : variants)
                variant_nodes.push_back(buildJSONExtractTree<JSONParser>(variant, source_for_exception_message));
            return std::make_unique<VariantNode<JSONParser>>(std::move(variant_nodes), SerializationVariant::getVariantsDeserializeTextOrder(variants));
        }
        case TypeIndex::Dynamic:
            return std::make_unique<DynamicNode<JSONParser>>();
        default:
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} doesn't support the return type schema: {}",
                source_for_exception_message,
                type->getName());
    }
}

#if USE_SIMDJSON
template void jsonElementToString<SimdJSONParser>(const SimdJSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);
template std::unique_ptr<JSONExtractTreeNode<SimdJSONParser>> buildJSONExtractTree<SimdJSONParser>(const DataTypePtr & type, const char * source_for_exception_message);
#endif

#if USE_RAPIDJSON
template void jsonElementToString<RapidJSONParser>(const RapidJSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);
template std::unique_ptr<JSONExtractTreeNode<RapidJSONParser>> buildJSONExtractTree<RapidJSONParser>(const DataTypePtr & type, const char * source_for_exception_message);
template bool tryGetNumericValueFromJSONElement<RapidJSONParser, Float64>(Float64 & value, const RapidJSONParser::Element & element, bool convert_bool_to_integer, String & error);
#else
template void jsonElementToString<DummyJSONParser>(const DummyJSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);
template std::unique_ptr<JSONExtractTreeNode<DummyJSONParser>> buildJSONExtractTree<DummyJSONParser>(const DataTypePtr & type, const char * source_for_exception_message);
#endif

}
