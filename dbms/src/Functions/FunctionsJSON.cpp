#include <Functions/FunctionsJSON.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
template <typename T>
class JSONNullableImplBase
{
public:
    static DataTypePtr getType() { return std::make_shared<DataTypeNullable>(std::make_shared<T>()); }

#ifdef __AVX2__
    static Field getDefault() { return {}; }
#endif
};

class JSONHasImpl : public JSONNullableImplBase<DataTypeUInt8>
{
public:
    static constexpr auto name{"jsonHas"};

#ifdef __AVX2__
    static Field getValue(ParsedJson::iterator &) { return {1}; }
#endif
};

class JSONLengthImpl : public JSONNullableImplBase<DataTypeUInt64>
{
public:
    static constexpr auto name{"jsonLength"};

#ifdef __AVX2__
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
#endif
};

class JSONTypeImpl : public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name{"jsonType"};

#ifdef __AVX2__
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
                return "Nothing";
            default:
                return "Unknown";
        }
    }
#endif
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

#ifdef __AVX2__
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
#endif
};

class JSONExtractUIntImpl : public JSONNullableImplBase<DataTypeUInt64>
{
public:
    static constexpr auto name{"jsonExtractUInt"};

#ifdef __AVX2__
    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_integer())
            return {pjh.get_integer()};
        else
            return getDefault();
    }
#endif
};

class JSONExtractIntImpl : public JSONNullableImplBase<DataTypeInt64>
{
public:
    static constexpr auto name{"jsonExtractInt"};

#ifdef __AVX2__
    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_integer())
            return {pjh.get_integer()};
        else
            return getDefault();
    }
#endif
};

class JSONExtractFloatImpl : public JSONNullableImplBase<DataTypeFloat64>
{
public:
    static constexpr auto name{"jsonExtractFloat"};

#ifdef __AVX2__
    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_double())
            return {pjh.get_double()};
        else
            return getDefault();
    }
#endif
};

class JSONExtractBoolImpl : public JSONNullableImplBase<DataTypeUInt8>
{
public:
    static constexpr auto name{"jsonExtractBool"};

#ifdef __AVX2__
    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.get_type() == 't')
            return {1};
        else if (pjh.get_type() == 'f')
            return {0};
        else
            return getDefault();
    }
#endif
};

// class JSONExtractRawImpl: public JSONNullableImplBase<DataTypeString>
// {
// public:
//     static constexpr auto name {"jsonExtractRaw"};

// #ifdef __AVX2__
//     static Field getValue(ParsedJson::iterator & pjh)
//     {
//         //
//     }
// #endif
// };

class JSONExtractStringImpl : public JSONNullableImplBase<DataTypeString>
{
public:
    static constexpr auto name{"jsonExtractString"};

#ifdef __AVX2__
    static Field getValue(ParsedJson::iterator & pjh)
    {
        if (pjh.is_string())
            return {String{pjh.get_string()}};
        else
            return getDefault();
    }
#endif
};

void registerFunctionsJSON(FunctionFactory & factory)
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
}
}
