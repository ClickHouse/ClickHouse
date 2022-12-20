#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include "DataTypes/DataTypeArray.h"
#include <simdjson.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using namespace simdjson;


static Field deserializeAsTuple(ondemand::value & /*val*/, [[maybe_unused]] const DataTypePtr & type)
{
    assert(typeid_cast<const DataTypeTuple *>(type.get()));
    Tuple res;

    // const auto & jtype = doc.type().value_unsafe();
    return res;
}

static Field deserializeAsArray(ondemand::value & val, [[maybe_unused]] const DataTypePtr & type)
{
    assert(typeid_cast<const DataTypeArray *>(type.get()));
    const auto & nested_type = typeid_cast<const DataTypeArray *>(type.get())->getNestedType();

    Array res;

    const auto & jtype = val.type().value_unsafe();
    switch (jtype)
    {
        case ondemand::json_type::array: {
            auto && jarray = val.get_array().value_unsafe();
            res.reserve(jarray.count_elements());
            for (auto && jelem : jarray)
            {
                auto field = deserialize(jelem, nested_type);
                res.emplace_back(std::move(field));
            }
            break;
        }
        default:
            return res;
    }
    return res;
}

static Field deserializeAsMap(ondemand::value & /*value*/, [[maybe_unused]] const DataTypePtr & type)
{
    assert(typeid_cast<const DataTypeTuple *>(type.get()));
    Tuple res;

    // const auto & jtype = doc.type().value_unsafe();
    return {};
}

static Field deserializeAsString(ondemand::value & val, [[maybe_unused]] const DataTypePtr & type)
{
    assert(typeid_cast<const DataTypeString *>(type.get()) || typeid_cast<const DataTypeFixedString *>(type.get()));

    const auto & jtype = val.type().value_unsafe();
    switch (jtype)
    {
        case ondemand::json_type::string: {
            return val.get_string().value_unsafe();
        }
        case ondemand::json_type::number: {
            const auto & num_type = val.get_number_type().value_unsafe();
            switch (num_type)
            {
                case ondemand::number_type::floating_point_number:
                    return std::to_string(val.get_double().value_unsafe());
                case ondemand::number_type::unsigned_integer:
                    return std::to_string(val.get_uint64().value_unsafe());
                case ondemand::number_type::signed_integer:
                    return std::to_string(val.get_int64().value_unsafe());
            }
        }
        case ondemand::json_type::boolean: {
            return val.get_bool().value_unsafe() ? "true" : "false";
        }
        default:
            /// TODO: consider fixed string
            return "";
    }
}

template <typename T>
static Field deserializeAsNumber(ondemand::value & val, [[maybe_unused]] const DataTypePtr & type)
{
    assert(typeid_cast<const DataTypeNumber<T> *>(type.get()));

    const auto & jtype = val.type().value_unsafe();
    switch (jtype)
    {
        case ondemand::json_type::number: {
            const auto & num_type = val.get_number_type().value_unsafe();
            switch (num_type)
            {
                case ondemand::number_type::floating_point_number:
                    return T(val.get_double().value_unsafe());
                case ondemand::number_type::unsigned_integer:
                    return T(val.get_uint64().value_unsafe());
                case ondemand::number_type::signed_integer:
                    return T(val.get_int64().value_unsafe());
            }
        }
        case ondemand::json_type::boolean: {
            return val.get_bool().value_unsafe() ? T(1) : T(0);
        }
        case ondemand::json_type::string: {
            if constexpr (is_integer_v<T>)
            {
                using IntType = NearestFieldType<T>;
                IntType int_val;
                ReadBufferFromString rb(val.get_string().value_unsafe());
                if (tryReadIntText(int_val, rb))
                    return T(int_val);
            }

            Float64 float_val;
            ReadBufferFromString rb(val.get_string().value_unsafe());
            if (tryReadFloatText(float_val, rb))
                return T(float_val);

            return T(0);
        }
        default:
            return T(0);
    }
}

[[maybe_unused]] static Field deserialize(ondemand::value & val, const DataTypePtr & type)
{
    const auto json_result = val.type();
    if (json_result.error())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong json type, error code:{}", json_result.error());
    const WhichDataType which(type);

#define IMPLEMENT_DESERIALIZE_AS_NUMBER(TYPE) \
    if (which.is##TYPE()) \
        return deserializeAsNumber<TYPE>(val, type);

#define ENUMERATE_NUMBER_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64) \
    M(UInt128) \
    M(UInt256) \
    M(Int128) \
    M(Int256)

    ENUMERATE_NUMBER_TYPES(IMPLEMENT_DESERIALIZE_AS_NUMBER)

    if (which.isDecimal() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deseralize json as decimal not supported yet");

    if (which.isStringOrFixedString())
        return deserializeAsString(val, type);

    if (which.isArray())
        return deserializeAsArray(val, type);

    if (which.isTuple())
        return deserializeAsTuple(val, type);

    if (which.isMap())
        return deserializeAsMap(val, type);

    return {};
}


class FunctionFromJSONString : public IFunction
{
public:
    static constexpr auto name = "fromJSONString";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionFromJSONString>(context); }

    explicit FunctionFromJSONString(ContextPtr) { }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * col_type = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!col_type)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} should be a constant string", getName());
        const auto type_name = col_type->getValue<String>();
        const auto type = DataTypeFactory::instance().get(type_name);
        return type;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        return {};
        /*
       auto res = ColumnString::create();
        ColumnString::Chars & data_to = res->getChars();
        ColumnString::Offsets & offsets_to = res->getOffsets();
        offsets_to.resize(input_rows_count);

        auto serializer = arguments[0].type->getDefaultSerialization();
        WriteBufferFromVector<ColumnString::Chars> json(data_to);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            serializer->serializeTextJSON(*arguments[0].column, i, json, format_settings);
            writeChar(0, json);
            offsets_to[i] = json.count();
        }

        json.finalize();
        return res;
        */
    }
};
}
