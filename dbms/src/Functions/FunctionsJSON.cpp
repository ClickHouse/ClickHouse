#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>
#include <Common/config.h>

#if USE_SIMDJSON
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>


namespace DB
{
template <typename T>
class JSONNullableImplBase
{
public:
    static DataTypePtr getType() { return std::make_shared<DataTypeNullable>(std::make_shared<T>()); }

    static Field getDefault() { return {}; }
};

class JSONHasImpl : public JSONNullableImplBase<DataTypeUInt8>
{
public:
    static constexpr auto name{"JSONHas"};

    static Field getValue(ParsedJson::iterator &) { return {1}; }
};

class JSONLengthImpl : public JSONNullableImplBase<DataTypeUInt64>
{
public:
    static constexpr auto name{"JSONLength"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (!pjh.is_object_or_array())
            return getDefault();

        size_t size = 0;

        if (pjh.down())
        {
            ++size;
            while (pjh.next())
                ++size;
            if (pjh.get_scope_type() == '{')
                size /= 2;
        }

        return {size};
    }
};

class JSONTypeImpl
{
public:
    static constexpr auto name{"JSONType"};

    static DataTypePtr getType()
    {
        static const std::vector<std::pair<String, Int8>> values = {
            {"Array", '['},
            {"Object", '{'},
            {"String", '"'},
            {"Int", 'l'},
            {"Float",'d'},
            {"Bool", 'b'},
            {"Null",'n'},
        };
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum<Int8>>(values));
    }

    static Field getDefault() { return {}; }

    static Field getValue(ParsedJson::iterator & pjh)
    {
        switch (pjh.get_type())
        {
            case '[':
            case '{':
            case '"':
            case 'l':
            case 'd':
            case 'n':
                return {pjh.get_type()};
            case 't':
            case 'f':
                return {'b'};
            default:
                return {};
        }
    }
};

class JSONExtractImpl
{
public:
    static constexpr auto name{"JSONExtract"};

    static DataTypePtr getType(const DataTypePtr & type)
    {
        WhichDataType which{type};

        if (which.isNativeUInt() || which.isNativeInt() || which.isFloat() || which.isEnum() || which.isDateOrDateTime()
            || which.isStringOrFixedString() || which.isInterval())
            return std::make_shared<DataTypeNullable>(type);

        if (which.isArray())
        {
            auto array_type = static_cast<const DataTypeArray *>(type.get());

            return std::make_shared<DataTypeArray>(getType(array_type->getNestedType()));
        }

        if (which.isTuple())
        {
            auto tuple_type = static_cast<const DataTypeTuple *>(type.get());

            DataTypes types;
            types.reserve(tuple_type->getElements().size());

            for (const DataTypePtr & element : tuple_type->getElements())
            {
                types.push_back(getType(element));
            }

            return std::make_shared<DataTypeTuple>(std::move(types));
        }

        throw Exception{"Unsupported return type schema: " + type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    static Field getDefault(const DataTypePtr & type)
    {
        WhichDataType which{type};

        if (which.isNativeUInt() || which.isNativeInt() || which.isFloat() || which.isEnum() || which.isDateOrDateTime()
            || which.isStringOrFixedString() || which.isInterval())
            return {};

        if (which.isArray())
            return {Array{}};

        if (which.isTuple())
        {
            auto tuple_type = static_cast<const DataTypeTuple *>(type.get());

            Tuple tuple;
            tuple.toUnderType().reserve(tuple_type->getElements().size());

            for (const DataTypePtr & element : tuple_type->getElements())
                tuple.toUnderType().push_back(getDefault(element));

            return {tuple};
        }

        // should not reach
        throw Exception{"Unsupported return type schema: " + type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    static Field getValue(ParsedJson::iterator & pjh, const DataTypePtr & type)
    {
        WhichDataType which{type};

        if (which.isNativeUInt() || which.isNativeInt() || which.isEnum() || which.isDateOrDateTime() || which.isInterval())
        {
            if (pjh.is_integer())
                return {pjh.get_integer()};
            else
                return getDefault(type);
        }

        if (which.isFloat())
        {
            if (pjh.is_integer())
                return {static_cast<double>(pjh.get_integer())};
            else if (pjh.is_double())
                return {pjh.get_double()};
            else
                return getDefault(type);
        }

        if (which.isStringOrFixedString())
        {
            if (pjh.is_string())
                return {String{pjh.get_string()}};
            else
                return getDefault(type);
        }

        if (which.isArray())
        {
            if (!pjh.is_object_or_array())
                return getDefault(type);

            auto array_type = static_cast<const DataTypeArray *>(type.get());

            Array array;

            bool first = true;

            while (first ? pjh.down() : pjh.next())
            {
                first = false;

                ParsedJson::iterator pjh1{pjh};

                array.push_back(getValue(pjh1, array_type->getNestedType()));
            }

            return {array};
        }

        if (which.isTuple())
        {
            if (!pjh.is_object_or_array())
                return getDefault(type);

            auto tuple_type = static_cast<const DataTypeTuple *>(type.get());

            Tuple tuple;
            tuple.toUnderType().reserve(tuple_type->getElements().size());

            bool valid = true;
            bool first = true;

            for (const DataTypePtr & element : tuple_type->getElements())
            {
                if (valid)
                {
                    valid &= first ? pjh.down() : pjh.next();
                    first = false;

                    ParsedJson::iterator pjh1{pjh};

                    tuple.toUnderType().push_back(getValue(pjh1, element));
                }
                else
                    tuple.toUnderType().push_back(getDefault(element));
            }

            return {tuple};
        }

        // should not reach
        throw Exception{"Unsupported return type schema: " + type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }
};

class JSONExtractUIntImpl : public JSONNullableImplBase<DataTypeUInt64>
{
public:
    static constexpr auto name{"JSONExtractUInt"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_integer())
            return {pjh.get_integer()};
        else
            return getDefault();
    }
};

class JSONExtractIntImpl : public JSONNullableImplBase<DataTypeInt64>
{
public:
    static constexpr auto name{"JSONExtractInt"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_integer())
            return {pjh.get_integer()};
        else
            return getDefault();
    }
};

class JSONExtractFloatImpl : public JSONNullableImplBase<DataTypeFloat64>
{
public:
    static constexpr auto name{"JSONExtractFloat"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_double())
            return {pjh.get_double()};
        else
            return getDefault();
    }
};

class JSONExtractBoolImpl : public JSONNullableImplBase<DataTypeUInt8>
{
public:
    static constexpr auto name{"JSONExtractBool"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.get_type() == 't')
            return {1};
        else if (pjh.get_type() == 'f')
            return {0};
        else
            return getDefault();
    }
};

class JSONExtractRawImpl: public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name {"JSONExtractRaw"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        WriteBufferFromOwnString buf;
        traverse(pjh, buf);
        return {std::move(buf.str())};
    }

private:
    static void traverse(ParsedJson::iterator & pjh, WriteBuffer & buf)
    {
        switch (pjh.get_type())
        {
            case '{':
            {
                writeChar('{', buf);
                if (pjh.down())
                {
                    writeJSONString(pjh.get_string(), pjh.get_string() + pjh.get_string_length(), buf, format_settings());
                    writeChar(':', buf);
                    pjh.next();
                    traverse(pjh, buf);
                    while (pjh.next())
                    {
                        writeChar(',', buf);
                        writeJSONString(pjh.get_string(), pjh.get_string() + pjh.get_string_length(), buf, format_settings());
                        writeChar(':', buf);
                        pjh.next();
                        traverse(pjh, buf);
                    }
                    pjh.up();
                }
                writeChar('}', buf);
                break;
            }
            case '[':
            {
                writeChar('[', buf);
                if (pjh.down())
                {
                    traverse(pjh, buf);
                    while (pjh.next())
                    {
                        writeChar(',', buf);
                        traverse(pjh, buf);
                    }
                    pjh.up();
                }
                writeChar(']', buf);
                break;
            }
            case '"':
            {
                writeJSONString(pjh.get_string(), pjh.get_string() + pjh.get_string_length(), buf, format_settings());
                break;
            }
            case 'l':
            {
                writeIntText(pjh.get_integer(), buf);
                break;
            }
            case 'd':
            {
                writeFloatText(pjh.get_double(), buf);
                break;
            }
            case 't':
            {
                writeCString("true", buf);
                break;
            }
            case 'f':
            {
                writeCString("false", buf);
                break;
            }
            case 'n':
            {
                writeCString("null", buf);
                break;
            }
        }
    }

    static const FormatSettings & format_settings()
    {
        static const FormatSettings the_instance = []
        {
            FormatSettings settings;
            settings.json.escape_forward_slashes = false;
            return settings;
        }();
        return the_instance;
    }
};

class JSONExtractStringImpl : public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name{"JSONExtractString"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_string())
            return {String{pjh.get_string()}};
        else
            return getDefault();
    }
};

class JSONExtractKeyImpl : public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name{"JSONExtractKey"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.get_scope_type() == '{' && pjh.prev() && pjh.is_string())
            return {String{pjh.get_string()}};
        else
            return getDefault();
    }
};

}
#else
namespace DB
{
struct JSONHasImpl { static constexpr auto name{"JSONHas"}; };
struct JSONLengthImpl { static constexpr auto name{"JSONLength"}; };
struct JSONTypeImpl { static constexpr auto name{"JSONType"}; };
struct JSONExtractImpl { static constexpr auto name{"JSONExtract"}; };
struct JSONExtractUIntImpl { static constexpr auto name{"JSONExtractUInt"}; };
struct JSONExtractIntImpl { static constexpr auto name{"JSONExtractInt"}; };
struct JSONExtractFloatImpl { static constexpr auto name{"JSONExtractFloat"}; };
struct JSONExtractBoolImpl { static constexpr auto name{"JSONExtractBool"}; };
struct JSONExtractRawImpl { static constexpr auto name {"JSONExtractRaw"}; };
struct JSONExtractStringImpl { static constexpr auto name{"JSONExtractString"}; };
struct JSONExtractKeyImpl { static constexpr auto name{"JSONExtractKey"}; };
}
#endif

namespace DB
{

void registerFunctionsJSON(FunctionFactory & factory)
{
#if USE_SIMDJSON
    if (__builtin_cpu_supports("avx2"))
    {
        factory.registerFunction<FunctionJSONBase<JSONHasImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONLengthImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONTypeImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractImpl, true>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractUIntImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractIntImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractFloatImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractBoolImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractRawImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractStringImpl, false>>();
        factory.registerFunction<FunctionJSONBase<JSONExtractKeyImpl, false>>();
        return;
    }
#endif
    factory.registerFunction<FunctionJSONDummy<JSONHasImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONLengthImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONTypeImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractUIntImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractIntImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractFloatImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractBoolImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractRawImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractStringImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractKeyImpl>>();
}

}
