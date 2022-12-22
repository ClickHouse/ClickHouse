#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeMap.h"
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
        if (!typeid_cast<const DataTypeString * >(arguments[0].type.get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}. Must be String",
                arguments[0].type->getName(),
                getName());

        const auto * col_type = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!col_type)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} should be a constant string", getName());

        const auto type_name = col_type->getValue<String>();
        return DataTypeFactory::instance().get(type_name);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto * col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        auto col_res = result_type->createColumn();
        col_res->reserve(input_rows_count);

        ondemand::parser parser;
        Field field;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str_ref = col->getDataAt(i);
            padded_string json(str_ref.data, str_ref.size);

            auto doc = parser.iterate(json);
            if (!doc.error())
            {
                field = deserializeValue(doc.value_unsafe(), result_type);
                col_res->insert(field);
            }
            else
                col_res->insertDefault();
        }
        return std::move(col_res);
    }

private:
    template <typename DocOrVal>
    static Field deserializeValueAsTuple(DocOrVal & val, const DataTypePtr & type)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        if (!tuple_type->haveSubtypes())
            return tuple_type->getDefault();
        const auto & elem_types = tuple_type->getElements();
        const auto & elem_names = tuple_type->getElementNames();

        bool tuple_named = tuple_type->haveExplicitNames();
        const auto jtype(val.type().value_unsafe());
        switch (jtype)
        {
            case ondemand::json_type::object: {
                if (!tuple_named)
                    return tuple_type->getDefault();

                Tuple res;
                res.reserve(elem_types.size());
                auto jmap(val.get_object().value_unsafe());
                for (size_t i = 0; i < elem_names.size(); ++i)
                {
                    auto field = jmap.find_field(elem_names[i]);
                    if (field.error())
                        res.emplace_back(elem_types[i]->getDefault());
                    else
                    {
                        auto elem_val(field.value_unsafe());
                        res.emplace_back(deserializeValue(elem_val, elem_types[i]));
                    }
                }
                return std::move(res);
            }
            case ondemand::json_type::array: {
                auto jarray(val.get_array().value_unsafe());
                if (jarray.count_elements() != elem_types.size())
                    return tuple_type->getDefault();

                Tuple res;
                res.reserve(elem_types.size());
                for (size_t i = 0; i < elem_types.size(); ++i)
                {
                    auto elem_val(jarray.at(i).value_unsafe());
                    res.emplace_back(deserializeValue(elem_val, elem_types[i]));
                }
                return std::move(res);
            }
            default:
                break;
        }
        return tuple_type->getDefault();
    }

    template <typename DocOrVal>
    static Field deserializeValueAsArray(DocOrVal & val, const DataTypePtr & type)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        if (!array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());
        const auto & nested_type = array_type->getNestedType();

        const auto jtype = val.type().value_unsafe();
        switch (jtype)
        {
            case ondemand::json_type::array: {
                Array res;
                auto jarray(val.get_array().value_unsafe());
                res.reserve(jarray.count_elements());
                for (auto jelem : jarray)
                {
                    auto field = deserializeValue(jelem.value_unsafe(), nested_type);
                    res.emplace_back(std::move(field));
                }
                return std::move(res);
            }
            default:
                break;
        }
        return array_type->getDefault();
    }

    template <typename DocOrVal>
    static Field deserializeValueAsMap(DocOrVal & val, const DataTypePtr & type)
    {
        const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
        if (!map_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());
        const auto & key_type = map_type->getKeyType();
        const auto & val_type = map_type->getValueType();

        const auto jtype = val.type().value_unsafe();
        switch (jtype)
        {
            case ondemand::json_type::object: {
                auto jmap(val.get_object().value_unsafe());
                // Map res(jmap.count_fields());
                // size_t index = 0;
                Map res;
                res.reserve(jmap.count_fields());
                for (auto field: jmap)
                {
                    Tuple kv(2);

                    auto k(field.unescaped_key().value_unsafe());
                    // kv.emplace_back(deserializeKey(k, key_type));
                    kv[0] = deserializeKey(k, key_type);

                    auto v(field.value().value_unsafe());
                    // kv.emplace_back(deserializeValue(v, val_type));
                    kv[1] = deserializeValue(v, val_type);

                    res.emplace_back(std::move(kv));
                    // res[index++] = std::move(kv);
                }
                return std::move(res);
            }
            default:
                break;
        }
        return map_type->getDefault();
    }

    template <typename DocOrVal>
    static Field deserializeValueAsString(DocOrVal & val, const DataTypePtr & type)
    {
        const auto * string_type = typeid_cast<const DataTypeString *>(type.get());
        const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(type.get());
        if (!string_type && !fixed_string_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        const auto jtype = val.type().value_unsafe();
        switch (jtype)
        {
            case ondemand::json_type::string: {
                return val.get_string().value_unsafe();
            }
            case ondemand::json_type::number: {
                const auto & num_type = val.get_number_type().value_unsafe();
                switch (num_type)
                {
                    case ondemand::number_type::signed_integer:
                        return std::to_string(val.get_int64().value_unsafe());
                    case ondemand::number_type::unsigned_integer:
                        return std::to_string(val.get_uint64().value_unsafe());
                    case ondemand::number_type::floating_point_number:
                        return std::to_string(val.get_double().value_unsafe());
                }
            }
            case ondemand::json_type::boolean: {
                return val.get_bool().value_unsafe() ? "true" : "false";
            }
            case ondemand::json_type::object: {
                auto jmap(val.get_object().value_unsafe());
                return jmap.raw_json().value_unsafe();
            }
            case ondemand::json_type::array: {
                auto jarray(val.get_array().value_unsafe());
                return jarray.raw_json().value_unsafe();
            }
            default:
                break;
        }
        return string_type ? string_type->getDefault() : fixed_string_type->getDefault();
    }

    template <typename DocOrVal, typename T>
    static Field deserializeValueAsNumber(DocOrVal & val, const DataTypePtr & type)
    {
        const auto * number_type = typeid_cast<const DataTypeNumber<T> *>(type.get());
        if (!number_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        const auto jtype = val.type().value_unsafe();
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
                break;
            }
            case ondemand::json_type::boolean: {
                return val.get_bool().value_unsafe() ? T(1) : T(0);
            }
            case ondemand::json_type::string: {
                if constexpr (is_integer_v<T>)
                {
                    using NearestIntType = NearestFieldType<T>;
                    NearestIntType int_val;
                    ReadBufferFromString rb(val.get_string().value_unsafe());
                    if (tryReadIntText(int_val, rb))
                        return int_val;
                }

                Float64 float_val;
                ReadBufferFromString rb(val.get_string().value_unsafe());
                if (tryReadFloatText(float_val, rb))
                    return T(float_val);
                break;
            }
            default:
                break;
        }
        return T(0);
    }

    template <typename T>
    static Field deserializeKeyAsInteger(const std::string_view & key, const DataTypePtr & type)
    {
        const auto * int_type = typeid_cast<const DataTypeNumber<T> *>(type.get());
        if (!int_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        using NearestIntType = NearestFieldType<T>;
        NearestIntType int_val;
        ReadBufferFromString rb(key);
        if (tryReadIntText(int_val, rb))
            return int_val;
        return NearestIntType(0);
    }

    static Field deserializeKey(const std::string_view & key, const DataTypePtr & type)
    {
        WhichDataType which(type);
        if (which.isStringOrFixedString())
            return key;

#define IMPLEMENT_DESERIALIZE_AS_INTEGER(TYPE) \
    if (which.is##TYPE()) \
        return deserializeKeyAsInteger<TYPE>(key, type);

#define ENUMERATE_INTEGER_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(UInt128) \
    M(UInt256) \
    M(Int128) \
    M(Int256)

        ENUMERATE_INTEGER_TYPES(IMPLEMENT_DESERIALIZE_AS_INTEGER)

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialize key as {} not supported yet", type->getName());
    }

    template <typename DocOrVal>
    static Field deserializeValue(DocOrVal & val, const DataTypePtr & type)
    {
        const WhichDataType which(type);

#define IMPLEMENT_DESERIALIZE_AS_NUMBER(TYPE) \
    if (which.is##TYPE()) \
        return deserializeValueAsNumber<DocOrVal, TYPE>(val, type);

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

        if (which.isStringOrFixedString())
            return deserializeValueAsString(val, type);

        if (which.isMap())
            return deserializeValueAsMap(val, type);

        if (which.isArray())
            return deserializeValueAsArray(val, type);

        if (which.isTuple())
            return deserializeValueAsTuple(val, type);

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialize value as {} not supported yet", type->getName());
    }
};

void registerFunctionFromJSONString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFromJSONString>();
}

}
