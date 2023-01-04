#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeLowCardinality.h>
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
                deserializeValue(doc.value_unsafe(), result_type, field);
                col_res->insert(field);
            }
            else
                col_res->insertDefault();
        }
        return std::move(col_res);
    }

private:
    template <typename DocOrVal>
    static void deserializeValueAsTuple(DocOrVal & val, const DataTypePtr & type, Field & res)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        if (!tuple_type->haveSubtypes())
        {
            res = Tuple();
            return;
        }

        const auto & elem_types = tuple_type->getElements();
        const auto & elem_names = tuple_type->getElementNames();

        bool tuple_named = tuple_type->haveExplicitNames();
        const auto jtype(val.type().value_unsafe());
        switch (jtype)
        {
            case ondemand::json_type::object: {
                if (!tuple_named)
                {
                    res = tuple_type->getDefault();
                    return;
                }

                Tuple tuple(elem_types.size());
                auto jmap(val.get_object().value_unsafe());
                for (size_t i = 0; i < elem_names.size(); ++i)
                {
                    auto elem_jval = jmap.find_field(elem_names[i]);
                    if (elem_jval.error())
                        tuple[i] = elem_types[i]->getDefault();
                    else
                    {
                        auto elem_val(elem_jval.value_unsafe());
                        deserializeValue(elem_val, elem_types[i], tuple[i]);
                    }
                }
                res = std::move(tuple);
                return;
            }
            case ondemand::json_type::array: {
                ondemand::array jarray(val.get_array().value_unsafe());
                if (jarray.count_elements() != elem_types.size())
                {
                    res = tuple_type->getDefault();
                    return;
                }

                size_t index = 0;
                Tuple tuple(elem_types.size());
                for (auto jelem : jarray)
                {
                    deserializeValue(jelem.value_unsafe(), elem_types[index], tuple[index]);
                    ++index;
                }
                res = std::move(tuple);
                return;
            }
            default:
                break;
        }
        res = tuple_type->getDefault();
    }

    template <typename DocOrVal>
    static void deserializeValueAsArray(DocOrVal & val, const DataTypePtr & type, Field & res)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
        if (!array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());
        const auto & nested_type = array_type->getNestedType();

        const auto jtype = val.type().value_unsafe();
        switch (jtype)
        {
            case ondemand::json_type::array: {
                auto jarray(val.get_array().value_unsafe());
                Array array(jarray.count_elements());
                size_t index = 0;
                for (auto jelem : jarray)
                    deserializeValue(jelem.value_unsafe(), nested_type, array[index++]);

                res = std::move(array);
                return;
            }
            default:
                break;
        }
        res = array_type->getDefault();
    }

    template <typename DocOrVal>
    static void deserializeValueAsMap(DocOrVal & val, const DataTypePtr & type, Field & res)
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
                Map map(jmap.count_fields());
                size_t index = 0;
                for (auto field: jmap)
                {
                    Tuple kv(2);

                    auto k(field.unescaped_key().value_unsafe());
                    deserializeKey(k, key_type, kv[0]);

                    auto v(field.value().value_unsafe());
                    deserializeValue(v, val_type, kv[1]);

                    map[index++] = std::move(kv);
                }
                res = std::move(map);
                return;
            }
            default:
                break;
        }
        res = map_type->getDefault();
    }

    template <typename DocOrVal>
    static void deserializeValueAsString(DocOrVal & val, const DataTypePtr & type, Field & res)
    {
        const auto * string_type = typeid_cast<const DataTypeString *>(type.get());
        const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(type.get());
        if (!string_type && !fixed_string_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        const auto jtype = val.type().value_unsafe();
        switch (jtype)
        {
            case ondemand::json_type::string: {
                res = val.get_string().value_unsafe();
                return;
            }
            case ondemand::json_type::number: {
                if constexpr (std::is_same_v<DocOrVal, ondemand::value>)
                    res = val.raw_json_token();
                else
                    res = val.raw_json().value_unsafe();
                return;
            }
            case ondemand::json_type::boolean: {
                res = val.get_bool().value_unsafe() ? "true" : "false";
                return;
            }
            case ondemand::json_type::object: {
                auto jmap(val.get_object().value_unsafe());
                res = jmap.raw_json().value_unsafe();
                return;
            }
            case ondemand::json_type::array: {
                auto jarray(val.get_array().value_unsafe());
                res = jarray.raw_json().value_unsafe();
                return;
            }
            default:
                break;
        }

        if (string_type)
            res = "";
        else
            res = fixed_string_type->getDefault();
    }

    template <typename DocOrVal, typename T>
    static void deserializeValueAsNumber(DocOrVal & val, const DataTypePtr & type, Field & res)
    {
        const auto * number_type = typeid_cast<const DataTypeNumber<T> *>(type.get());
        if (!number_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        const auto jtype = val.type().value_unsafe();
        switch (jtype)
        {
            case ondemand::json_type::number: {
                const auto num_type(val.get_number_type().value_unsafe());
                switch (num_type)
                {
                    case ondemand::number_type::floating_point_number:
                        res = T(val.get_double().value_unsafe());
                        return;
                    case ondemand::number_type::unsigned_integer:
                        res = T(val.get_uint64().value_unsafe());
                        return;
                    case ondemand::number_type::signed_integer:
                        res = T(val.get_int64().value_unsafe());
                        return;
                }
                break;
            }
            case ondemand::json_type::boolean: {
                res = val.get_bool().value_unsafe() ? T(1) : T(0);
                return;
            }
            case ondemand::json_type::string: {
                if constexpr (is_integer<T>)
                {
                    using NearestIntType = NearestFieldType<T>;
                    NearestIntType int_val;
                    ReadBufferFromString rb(val.get_string().value_unsafe());
                    if (tryReadIntText(int_val, rb))
                    {
                        res = std::move(int_val);
                        return;
                    }
                }

                Float64 float_val;
                ReadBufferFromString rb(val.get_string().value_unsafe());
                if (tryReadFloatText(float_val, rb))
                {
                    res = T(float_val);
                    return;
                }
                break;
            }
            default:
                break;
        }
        res = T(0);
    }

    template <typename T>
    static void deserializeKeyAsInteger(const std::string_view & key, const DataTypePtr & type, Field & res)
    {
        const auto * int_type = typeid_cast<const DataTypeNumber<T> *>(type.get());
        if (!int_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type {}", type->getName());

        using NearestIntType = NearestFieldType<T>;
        NearestIntType int_val;
        ReadBufferFromString rb(key);
        res = tryReadIntText(int_val, rb) ? int_val : NearestIntType(0);
    }

    static void deserializeKey(const std::string_view & key, const DataTypePtr & type, Field & res)
    {
        if (type->lowCardinality())
            return deserializeKey(key, removeLowCardinality(type), res);

        if (type->isNullable())
            return deserializeKey(key, removeNullable(type), res);

        WhichDataType which(type);
        if (which.isStringOrFixedString())
        {
            if (which.isFixedString())
            {
                size_t n = typeid_cast<const DataTypeFixedString *>(type.get())->getN();
                res = key.size() > n ? key.substr(0, n) : key;
            }
            else
                res = key;
            return;
        }

#define IMPLEMENT_DESERIALIZE_AS_INTEGER(TYPE) \
    if (which.is##TYPE()) \
        return deserializeKeyAsInteger<TYPE>(key, type, res);

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

        res = type->getDefault();
    }

    template <typename DocOrVal>
    static void deserializeValue(DocOrVal & val, const DataTypePtr & type, Field & res)
    {
        if (type->lowCardinality())
            return deserializeValue(val, removeLowCardinality(type), res);

        if (type->isNullable())
            return deserializeValue(val, removeNullable(type), res);

        const WhichDataType which(type);

#define IMPLEMENT_DESERIALIZE_AS_NUMBER(TYPE) \
    if (which.is##TYPE()) \
        return deserializeValueAsNumber<DocOrVal, TYPE>(val, type, res);

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
        {
            deserializeValueAsString(val, type, res);

            if (which.isFixedString())
            {
                /// We must truncate string when its size exceeds N
                size_t n = typeid_cast<const DataTypeFixedString *>(type.get())->getN();
                const auto & str = res.get<String>();
                if (str.size() > n)
                    res = str.substr(0, n);
            }
            return;
        }

        if (which.isMap())
            return deserializeValueAsMap(val, type, res);

        if (which.isArray())
            return deserializeValueAsArray(val, type, res);

        if (which.isTuple())
            return deserializeValueAsTuple(val, type, res);

        res = type->getDefault();
    }
};

REGISTER_FUNCTION(FromJSONString)
{
    factory.registerFunction<FunctionFromJSONString>();
}

}
