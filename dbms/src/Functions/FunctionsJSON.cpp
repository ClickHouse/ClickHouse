#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>
#include <Common/config.h>

#if USE_SIMDJSON
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>


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
    static constexpr auto name{"jsonHas"};

    static Field getValue(ParsedJson::iterator &) { return {1}; }
};

class JSONLengthImpl : public JSONNullableImplBase<DataTypeUInt64>
{
public:
    static constexpr auto name{"jsonLength"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (!pjh.is_object_or_array())
            return getDefault();

        size_t size = 0;

        if (pjh.down())
        {
            size += 1;

            while (pjh.next())
                size += 1;
        }

        return {size};
    }
};

class JSONTypeImpl : public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name{"jsonType"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        switch (pjh.get_type())
        {
            case '[':
                return "Array";
            case '{':
                return "Object";
            case '"':
                return "String";
            case 'l':
                return "Int64";
            case 'd':
                return "Float64";
            case 't':
                return "Bool";
            case 'f':
                return "Bool";
            case 'n':
                return "Null";
            default:
                return "Unknown";
        }
    }
};

class JSONExtractImpl
{
public:
    static constexpr auto name{"jsonExtract"};

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
    static constexpr auto name{"jsonExtractUInt"};

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
    static constexpr auto name{"jsonExtractInt"};

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
    static constexpr auto name{"jsonExtractFloat"};

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
    static constexpr auto name{"jsonExtractBool"};

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

// class JSONExtractRawImpl: public JSONNullableImplBase<DataTypeString>
// {
// public:
//     static constexpr auto name {"jsonExtractRaw"};

//     static Field getValue(ParsedJson::iterator & pjh)
//     {
//         //
//     }
// };

class JSONExtractStringImpl : public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name{"jsonExtractString"};

    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_string())
            return {String{pjh.get_string()}};
        else
            return getDefault();
    }
};

}
#else
namespace DB
{
struct JSONHasImpl { static constexpr auto name{"jsonHas"}; };
struct JSONLengthImpl { static constexpr auto name{"jsonLength"}; };
struct JSONTypeImpl { static constexpr auto name{"jsonType"}; };
struct JSONExtractImpl { static constexpr auto name{"jsonExtract"}; };
struct JSONExtractUIntImpl { static constexpr auto name{"jsonExtractUInt"}; };
struct JSONExtractIntImpl { static constexpr auto name{"jsonExtractInt"}; };
struct JSONExtractFloatImpl { static constexpr auto name{"jsonExtractFloat"}; };
struct JSONExtractBoolImpl { static constexpr auto name{"jsonExtractBool"}; };
//struct JSONExtractRawImpl { static constexpr auto name {"jsonExtractRaw"}; };
struct JSONExtractStringImpl { static constexpr auto name{"jsonExtractString"}; };
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
        // factory.registerFunction<FunctionJSONBase<
        //     JSONExtractRawImpl,
        //     false
        // >>();
        factory.registerFunction<FunctionJSONBase<JSONExtractStringImpl, false>>();
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
    //factory.registerFunction<FunctionJSONDummy<JSONExtractRawImpl>>();
    factory.registerFunction<FunctionJSONDummy<JSONExtractStringImpl>>();
}

}
