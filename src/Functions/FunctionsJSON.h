#pragma once

#include <type_traits>
#include <boost/tti/has_member_function.hpp>

#include <base/range.h>

#include <Common/CPUID.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/AccurateComparison.h>
#include <Core/Settings.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/Serializations/SerializationDecimal.h>

#include <Functions/IFunction.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Functions/FunctionHelpers.h>

#include <IO/readDecimalText.h>
#include <Interpreters/Context.h>


#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
concept HasIndexOperator = requires (T t)
{
    t[0];
};

/// Functions to parse JSONs and extract values from it.
/// The first argument of all these functions gets a JSON,
/// after that there are any number of arguments specifying path to a desired part from the JSON's root.
/// For example,
/// select JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100

class FunctionJSONHelpers
{
public:
    template <typename Name, template<typename> typename Impl, class JSONParser>
    class Executor
    {
    public:
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", String(Name::name));

            const auto & first_column = arguments[0];
            if (!isString(first_column.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The first argument of function {} should be a string containing JSON, illegal type: "
                                "{}", String(Name::name), first_column.type->getName());

            const ColumnPtr & arg_json = first_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_string)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", arg_json->getName());

            const ColumnString::Chars & chars = col_json_string->getChars();
            const ColumnString::Offsets & offsets = col_json_string->getOffsets();

            size_t num_index_arguments = Impl<JSONParser>::getNumberOfIndexArguments(arguments);
            std::vector<Move> moves = prepareMoves(Name::name, arguments, 1, num_index_arguments);

            /// Preallocate memory in parser if necessary.
            JSONParser parser;
            if constexpr (has_member_function_reserve<void (JSONParser::*)(size_t)>::value)
            {
                size_t max_size = calculateMaxSize(offsets);
                if (max_size)
                    parser.reserve(max_size);
            }

            Impl<JSONParser> impl;

            /// prepare() does Impl-specific preparation before handling each row.
            if constexpr (has_member_function_prepare<void (Impl<JSONParser>::*)(const char *, const ColumnsWithTypeAndName &, const DataTypePtr &)>::value)
                impl.prepare(Name::name, arguments, result_type);

            using Element = typename JSONParser::Element;

            Element document;
            bool document_ok = false;
            if (col_json_const)
            {
                std::string_view json{reinterpret_cast<const char *>(chars.data()), offsets[0] - 1};
                document_ok = parser.parse(json, document);
            }

            for (const auto i : collections::range(0, input_rows_count))
            {
                if (!col_json_const)
                {
                    std::string_view json{reinterpret_cast<const char *>(&chars[offsets[i - 1]]), offsets[i] - offsets[i - 1] - 1};
                    document_ok = parser.parse(json, document);
                }

                bool added_to_column = false;
                if (document_ok)
                {
                    /// Perform moves.
                    Element element;
                    std::string_view last_key;
                    bool moves_ok = performMoves<JSONParser>(arguments, i, document, moves, element, last_key);

                    if (moves_ok)
                        added_to_column = impl.insertResultToColumn(*to, element, last_key);
                }

                /// We add default value (=null or zero) if something goes wrong, we don't throw exceptions in these JSON functions.
                if (!added_to_column)
                    to->insertDefault();
            }
            return to;
        }
    };

private:
    BOOST_TTI_HAS_MEMBER_FUNCTION(reserve)
    BOOST_TTI_HAS_MEMBER_FUNCTION(prepare)

    /// Represents a move of a JSON iterator described by a single argument passed to a JSON function.
    /// For example, the call JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1)
    /// contains two moves: {MoveType::ConstKey, "b"} and {MoveType::ConstIndex, 1}.
    /// Keys and indices can be nonconst, in this case they are calculated for each row.
    enum class MoveType
    {
        Key,
        Index,
        ConstKey,
        ConstIndex,
    };

    struct Move
    {
        explicit Move(MoveType type_, size_t index_ = 0) : type(type_), index(index_) {}
        Move(MoveType type_, const String & key_) : type(type_), key(key_) {}
        MoveType type;
        size_t index = 0;
        String key;
    };

    static std::vector<FunctionJSONHelpers::Move> prepareMoves(
        const char * function_name,
        const ColumnsWithTypeAndName & columns,
        size_t first_index_argument,
        size_t num_index_arguments)
    {
        std::vector<Move> moves;
        moves.reserve(num_index_arguments);
        for (const auto i : collections::range(first_index_argument, first_index_argument + num_index_arguments))
        {
            const auto & column = columns[i];
            if (!isString(column.type) && !isNativeInteger(column.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The argument {} of function {} should be a string specifying key "
                                "or an integer specifying index, illegal type: {}",
                                std::to_string(i + 1), String(function_name), column.type->getName());

            if (column.column && isColumnConst(*column.column))
            {
                const auto & column_const = assert_cast<const ColumnConst &>(*column.column);
                if (isString(column.type))
                    moves.emplace_back(MoveType::ConstKey, column_const.getValue<String>());
                else
                    moves.emplace_back(MoveType::ConstIndex, column_const.getInt(0));
            }
            else
            {
                if (isString(column.type))
                    moves.emplace_back(MoveType::Key, "");
                else
                    moves.emplace_back(MoveType::Index, 0);
            }
        }
        return moves;
    }


    /// Performs moves of types MoveType::Index and MoveType::ConstIndex.
    template <typename JSONParser>
    static bool performMoves(const ColumnsWithTypeAndName & arguments, size_t row,
                             const typename JSONParser::Element & document, const std::vector<Move> & moves,
                             typename JSONParser::Element & element, std::string_view & last_key)
    {
        typename JSONParser::Element res_element = document;
        std::string_view key;

        for (size_t j = 0; j != moves.size(); ++j)
        {
            switch (moves[j].type)
            {
                case MoveType::ConstIndex:
                {
                    if (!moveToElementByIndex<JSONParser>(res_element, static_cast<int>(moves[j].index), key))
                        return false;
                    break;
                }
                case MoveType::ConstKey:
                {
                    key = moves[j].key;
                    if (!moveToElementByKey<JSONParser>(res_element, key))
                        return false;
                    break;
                }
                case MoveType::Index:
                {
                    Int64 index = (*arguments[j + 1].column)[row].get<Int64>();
                    if (!moveToElementByIndex<JSONParser>(res_element, static_cast<int>(index), key))
                        return false;
                    break;
                }
                case MoveType::Key:
                {
                    key = arguments[j + 1].column->getDataAt(row).toView();
                    if (!moveToElementByKey<JSONParser>(res_element, key))
                        return false;
                    break;
                }
            }
        }

        element = res_element;
        last_key = key;
        return true;
    }

    template <typename JSONParser>
    static bool moveToElementByIndex(typename JSONParser::Element & element, int index, std::string_view & out_key)
    {
        if (element.isArray())
        {
            auto array = element.getArray();
            if (index >= 0)
                --index;
            else
                index += array.size();

            if (static_cast<size_t>(index) >= array.size())
                return false;
            element = array[index];
            out_key = {};
            return true;
        }

        if constexpr (HasIndexOperator<typename JSONParser::Object>)
        {
            if (element.isObject())
            {
                auto object = element.getObject();
                if (index >= 0)
                    --index;
                else
                    index += object.size();

                if (static_cast<size_t>(index) >= object.size())
                    return false;
                std::tie(out_key, element) = object[index];
                return true;
            }
        }

        return {};
    }

    /// Performs moves of types MoveType::Key and MoveType::ConstKey.
    template <typename JSONParser>
    static bool moveToElementByKey(typename JSONParser::Element & element, std::string_view key)
    {
        if (!element.isObject())
            return false;
        auto object = element.getObject();
        return object.find(key, element);
    }

    static size_t calculateMaxSize(const ColumnString::Offsets & offsets)
    {
        size_t max_size = 0;
        for (const auto i : collections::range(0, offsets.size()))
        {
            size_t size = offsets[i] - offsets[i - 1];
            if (max_size < size)
                max_size = size;
        }
        if (max_size)
            --max_size;
        return max_size;
    }

};

template <typename T>
class JSONExtractImpl;

template <typename T>
class JSONExtractKeysAndValuesImpl;

/**
* Functions JSONExtract and JSONExtractKeysAndValues force the return type - it is specified in the last argument.
* For example - `SELECT JSONExtract(materialize('{"a": 131231, "b": 1234}'), 'b', 'LowCardinality(FixedString(4))')`
* But by default ClickHouse decides on its own whether the return type will be LowCardinality based on the types of
* input arguments.
* And for these specific functions we cannot rely on this mechanism, so these functions have their own implementation -
* just convert all of the LowCardinality input columns to full ones, execute and wrap the resulting column in LowCardinality
* if needed.
*/
template <template<typename> typename Impl>
constexpr bool functionForcesTheReturnType()
{
    return std::is_same_v<Impl<void>, JSONExtractImpl<void>> || std::is_same_v<Impl<void>, JSONExtractKeysAndValuesImpl<void>>;
}

template <typename Name, template<typename> typename Impl>
class ExecutableFunctionJSON : public IExecutableFunction
{

public:
    explicit ExecutableFunctionJSON(const NullPresence & null_presence_, bool allow_simdjson_, const DataTypePtr & json_return_type_)
        : null_presence(null_presence_), allow_simdjson(allow_simdjson_), json_return_type(json_return_type_)
    {
    }

    String getName() const override { return Name::name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override
    {
        return !functionForcesTheReturnType<Impl>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        if constexpr (functionForcesTheReturnType<Impl>())
        {
            ColumnsWithTypeAndName columns_without_low_cardinality = arguments;

            for (auto & column : columns_without_low_cardinality)
            {
                column.column = recursiveRemoveLowCardinality(column.column);
                column.type = recursiveRemoveLowCardinality(column.type);
            }

            ColumnsWithTypeAndName temporary_columns = null_presence.has_nullable ? createBlockWithNestedColumns(columns_without_low_cardinality) : columns_without_low_cardinality;
            ColumnPtr temporary_result = chooseAndRunJSONParser(temporary_columns, json_return_type, input_rows_count);

            if (null_presence.has_nullable)
                temporary_result = wrapInNullable(temporary_result, columns_without_low_cardinality, result_type, input_rows_count);

            if (result_type->lowCardinality())
                temporary_result = recursiveLowCardinalityTypeConversion(temporary_result, json_return_type, result_type);

            return temporary_result;
        }
        else
        {
            ColumnsWithTypeAndName temporary_columns = null_presence.has_nullable ? createBlockWithNestedColumns(arguments) : arguments;
            ColumnPtr temporary_result = chooseAndRunJSONParser(temporary_columns, json_return_type, input_rows_count);

            if (null_presence.has_nullable)
                temporary_result = wrapInNullable(temporary_result, arguments, result_type, input_rows_count);

            if (result_type->lowCardinality())
                temporary_result = recursiveLowCardinalityTypeConversion(temporary_result, json_return_type, result_type);

            return temporary_result;
        }
    }

private:

    ColumnPtr
    chooseAndRunJSONParser(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
#if USE_SIMDJSON
        if (allow_simdjson)
            return FunctionJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count);
#endif

#if USE_RAPIDJSON
        return FunctionJSONHelpers::Executor<Name, Impl, RapidJSONParser>::run(arguments, result_type, input_rows_count);
#else
        return FunctionJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count);
#endif
    }

    NullPresence null_presence;
    bool allow_simdjson;
    DataTypePtr json_return_type;
};


template <typename Name, template<typename> typename Impl>
class FunctionBaseFunctionJSON : public IFunctionBase
{
public:
    explicit FunctionBaseFunctionJSON(
        const NullPresence & null_presence_,
        bool allow_simdjson_,
        DataTypes argument_types_,
        DataTypePtr return_type_,
        DataTypePtr json_return_type_)
        : null_presence(null_presence_)
        , allow_simdjson(allow_simdjson_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
        , json_return_type(std::move(json_return_type_))
    {
    }

    String getName() const override { return Name::name; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSON<Name, Impl>>(null_presence, allow_simdjson, json_return_type);
    }

private:
    NullPresence null_presence;
    bool allow_simdjson;
    DataTypes argument_types;
    DataTypePtr return_type;
    DataTypePtr json_return_type;
};

/// We use IFunctionOverloadResolver instead of IFunction to handle non-default NULL processing.
/// Both NULL and JSON NULL should generate NULL value. If any argument is NULL, return NULL.
template <typename Name, template<typename> typename Impl>
class JSONOverloadResolver : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = Name::name;

    String getName() const override { return name; }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<JSONOverloadResolver>(context_);
    }

    explicit JSONOverloadResolver(ContextPtr context_) : WithContext(context_) {}

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override
    {
        return !functionForcesTheReturnType<Impl>();
    }

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const override
    {
        bool has_nothing_argument = false;
        for (const auto & arg : arguments)
            has_nothing_argument |= isNothing(arg.type);

        DataTypePtr json_return_type = Impl<DummyJSONParser>::getReturnType(Name::name, createBlockWithNestedColumns(arguments));
        NullPresence null_presence = getNullPresense(arguments);
        DataTypePtr return_type;
        if (has_nothing_argument)
            return_type = std::make_shared<DataTypeNothing>();
        else if (null_presence.has_null_constant)
            return_type = makeNullable(std::make_shared<DataTypeNothing>());
        else if (null_presence.has_nullable)
            return_type = makeNullable(json_return_type);
        else
            return_type = json_return_type;

        /// Top-level LowCardinality columns are processed outside JSON parser.
        json_return_type = removeLowCardinality(json_return_type);

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.emplace_back(argument.type);
        return std::make_unique<FunctionBaseFunctionJSON<Name, Impl>>(
                null_presence, getContext()->getSettingsRef().allow_simdjson, argument_types, return_type, json_return_type);
    }
};

struct NameJSONHas { static constexpr auto name{"JSONHas"}; };
struct NameIsValidJSON { static constexpr auto name{"isValidJSON"}; };
struct NameJSONLength { static constexpr auto name{"JSONLength"}; };
struct NameJSONKey { static constexpr auto name{"JSONKey"}; };
struct NameJSONType { static constexpr auto name{"JSONType"}; };
struct NameJSONExtractInt { static constexpr auto name{"JSONExtractInt"}; };
struct NameJSONExtractUInt { static constexpr auto name{"JSONExtractUInt"}; };
struct NameJSONExtractFloat { static constexpr auto name{"JSONExtractFloat"}; };
struct NameJSONExtractBool { static constexpr auto name{"JSONExtractBool"}; };
struct NameJSONExtractString { static constexpr auto name{"JSONExtractString"}; };
struct NameJSONExtract { static constexpr auto name{"JSONExtract"}; };
struct NameJSONExtractKeysAndValues { static constexpr auto name{"JSONExtractKeysAndValues"}; };
struct NameJSONExtractRaw { static constexpr auto name{"JSONExtractRaw"}; };
struct NameJSONExtractArrayRaw { static constexpr auto name{"JSONExtractArrayRaw"}; };
struct NameJSONExtractKeysAndValuesRaw { static constexpr auto name{"JSONExtractKeysAndValuesRaw"}; };
struct NameJSONExtractKeys { static constexpr auto name{"JSONExtractKeys"}; };


template <typename JSONParser>
class JSONHasImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element &, std::string_view)
    {
        ColumnVector<UInt8> & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(1);
        return true;
    }
};


template <typename JSONParser>
class IsValidJSONImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
        {
            /// IsValidJSON() shouldn't get parameters other than JSON.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs exactly one argument",
                            String(function_name));
        }
        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName &) { return 0; }

    static bool insertResultToColumn(IColumn & dest, const Element &, std::string_view)
    {
        /// This function is called only if JSON is valid.
        /// If JSON isn't valid then `FunctionJSON::Executor::run()` adds default value (=zero) to `dest` without calling this function.
        ColumnVector<UInt8> & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(1);
        return true;
    }
};


template <typename JSONParser>
class JSONLengthImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeUInt64>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        size_t size;
        if (element.isArray())
            size = element.getArray().size();
        else if (element.isObject())
            size = element.getObject().size();
        else
            return false;

        ColumnVector<UInt64> & col_vec = assert_cast<ColumnVector<UInt64> &>(dest);
        col_vec.insertValue(size);
        return true;
    }
};


template <typename JSONParser>
class JSONKeyImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element &, std::string_view last_key)
    {
        if (last_key.empty())
            return false;
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(last_key.data(), last_key.size());
        return true;
    }
};


template <typename JSONParser>
class JSONTypeImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        static const std::vector<std::pair<String, Int8>> values = {
            {"Array", '['},
            {"Object", '{'},
            {"String", '"'},
            {"Int64", 'i'},
            {"UInt64", 'u'},
            {"Double", 'd'},
            {"Bool", 'b'},
            {"Null", 0}, /// the default value for the column.
        };
        return std::make_shared<DataTypeEnum<Int8>>(values);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        UInt8 type;
        switch (element.type())
        {
            case ElementType::INT64:
                type = 'i';
                break;
            case ElementType::UINT64:
                type = 'u';
                break;
            case ElementType::DOUBLE:
                type = 'd';
                break;
            case ElementType::STRING:
                type = '"';
                break;
            case ElementType::ARRAY:
                type = '[';
                break;
            case ElementType::OBJECT:
                type = '{';
                break;
            case ElementType::BOOL:
                type = 'b';
                break;
            case ElementType::NULL_VALUE:
                type = 0;
                break;
        }

        ColumnVector<Int8> & col_vec = assert_cast<ColumnVector<Int8> &>(dest);
        col_vec.insertValue(type);
        return true;
    }
};


template <typename JSONParser, typename NumberType, bool convert_bool_to_integer = false>
class JSONExtractNumericImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeNumber<NumberType>>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        NumberType value;

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
                    return false;
                break;
            case ElementType::UINT64:
                if (!accurate::convertNumeric<UInt64, NumberType, false>(element.getUInt64(), value))
                    return false;
                break;
            case ElementType::INT64:
                if (!accurate::convertNumeric<Int64, NumberType, false>(element.getInt64(), value))
                    return false;
                break;
            case ElementType::BOOL:
                if constexpr (is_integer<NumberType> && convert_bool_to_integer)
                {
                    value = static_cast<NumberType>(element.getBool());
                    break;
                }
                return false;
            case ElementType::STRING:
            {
                auto rb = ReadBufferFromMemory{element.getString()};
                if constexpr (std::is_floating_point_v<NumberType>)
                {
                    if (!tryReadFloatText(value, rb) || !rb.eof())
                        return false;
                }
                else
                {
                    if (tryReadIntText(value, rb) && rb.eof())
                        break;

                    /// Try to parse float and convert it to integer.
                    Float64 tmp_float;
                    rb.position() = rb.buffer().begin();
                    if (!tryReadFloatText(tmp_float, rb) || !rb.eof())
                        return false;

                    if (!accurate::convertNumeric<Float64, NumberType, false>(tmp_float, value))
                        return false;
                }
                break;
            }
            default:
                return false;
        }

        if (dest.getDataType() == TypeIndex::LowCardinality)
        {
            ColumnLowCardinality & col_low = assert_cast<ColumnLowCardinality &>(dest);
            col_low.insertData(reinterpret_cast<const char *>(&value), sizeof(value));
        }
        else
        {
            auto & col_vec = assert_cast<ColumnVector<NumberType> &>(dest);
            col_vec.insertValue(value);
        }
        return true;
    }
};


template <typename JSONParser>
using JSONExtractInt64Impl = JSONExtractNumericImpl<JSONParser, Int64>;
template <typename JSONParser>
using JSONExtractUInt64Impl = JSONExtractNumericImpl<JSONParser, UInt64>;
template <typename JSONParser>
using JSONExtractFloat64Impl = JSONExtractNumericImpl<JSONParser, Float64>;


template <typename JSONParser>
class JSONExtractBoolImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        bool value;
        switch (element.type())
        {
            case ElementType::BOOL:
                value = element.getBool();
                break;
            case ElementType::INT64:
                value = element.getInt64() != 0;
                break;
            case ElementType::UINT64:
                value = element.getUInt64() != 0;
                break;
            default:
                return false;
        }

        auto & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(static_cast<UInt8>(value));
        return true;
    }
};

template <typename JSONParser>
class JSONExtractRawImpl;

template <typename JSONParser>
class JSONExtractStringImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        if (element.isNull())
            return false;

        if (!element.isString())
            return JSONExtractRawImpl<JSONParser>::insertResultToColumn(dest, element, {});

        auto str = element.getString();

        if (dest.getDataType() == TypeIndex::LowCardinality)
        {
            ColumnLowCardinality & col_low = assert_cast<ColumnLowCardinality &>(dest);
            col_low.insertData(str.data(), str.size());
        }
        else
        {
            ColumnString & col_str = assert_cast<ColumnString &>(dest);
            col_str.insertData(str.data(), str.size());
        }
        return true;
    }
};

/// Nodes of the extract tree. We need the extract tree to extract from JSON complex values containing array, tuples or nullables.
template <typename JSONParser>
struct JSONExtractTree
{
    using Element = typename JSONParser::Element;

    class Node
    {
    public:
        Node() = default;
        virtual ~Node() = default;
        virtual bool insertResultToColumn(IColumn &, const Element &) = 0;
    };

    template <typename NumberType>
    class NumericNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            return JSONExtractNumericImpl<JSONParser, NumberType, true>::insertResultToColumn(dest, element, {});
        }
    };

    class LowCardinalityFixedStringNode : public Node
    {
    public:
        explicit LowCardinalityFixedStringNode(const size_t fixed_length_) : fixed_length(fixed_length_) { }
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            // If element is an object we delegate the insertion to JSONExtractRawImpl
            if (element.isObject())
                return JSONExtractRawImpl<JSONParser>::insertResultToLowCardinalityFixedStringColumn(dest, element, fixed_length);
            else if (!element.isString())
                return false;

            auto str = element.getString();
            if (str.size() > fixed_length)
                return false;

            // For the non low cardinality case of FixedString, the padding is done in the FixedString Column implementation.
            // In order to avoid having to pass the data to a FixedString Column and read it back (which would slow down the execution)
            // the data is padded here and written directly to the Low Cardinality Column
            if (str.size() == fixed_length)
            {
                assert_cast<ColumnLowCardinality &>(dest).insertData(str.data(), str.size());
            }
            else
            {
                String padded_str(str);
                padded_str.resize(fixed_length, '\0');

                assert_cast<ColumnLowCardinality &>(dest).insertData(padded_str.data(), padded_str.size());
            }
            return true;
        }

    private:
        const size_t fixed_length;
    };

    class UUIDNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (!element.isString())
                return false;

            auto uuid = parseFromString<UUID>(element.getString());
            if (dest.getDataType() == TypeIndex::LowCardinality)
            {
                ColumnLowCardinality & col_low = assert_cast<ColumnLowCardinality &>(dest);
                col_low.insertData(reinterpret_cast<const char *>(&uuid), sizeof(uuid));
            }
            else
            {
                assert_cast<ColumnUUID &>(dest).insert(uuid);
            }
            return true;
        }
    };

    template <typename DecimalType>
    class DecimalNode : public Node
    {
    public:
        explicit DecimalNode(DataTypePtr data_type_) : data_type(data_type_) {}
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            const auto * type = assert_cast<const DataTypeDecimal<DecimalType> *>(data_type.get());

            DecimalType value{};

            switch (element.type())
            {
                case ElementType::DOUBLE:
                    value = convertToDecimal<DataTypeNumber<Float64>, DataTypeDecimal<DecimalType>>(
                        element.getDouble(), type->getScale());
                    break;
                case ElementType::UINT64:
                    value = convertToDecimal<DataTypeNumber<UInt64>, DataTypeDecimal<DecimalType>>(
                        element.getUInt64(), type->getScale());
                    break;
                case ElementType::INT64:
                    value = convertToDecimal<DataTypeNumber<Int64>, DataTypeDecimal<DecimalType>>(
                        element.getInt64(), type->getScale());
                    break;
                case ElementType::STRING: {
                    auto rb = ReadBufferFromMemory{element.getString()};
                    if (!SerializationDecimal<DecimalType>::tryReadText(value, rb, DecimalUtils::max_precision<DecimalType>, type->getScale()))
                        return false;
                    break;
                }
                default:
                    return false;
            }

            assert_cast<ColumnDecimal<DecimalType> &>(dest).insertValue(value);
            return true;
        }

    private:
        DataTypePtr data_type;
    };

    class StringNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            return JSONExtractStringImpl<JSONParser>::insertResultToColumn(dest, element, {});
        }
    };

    class FixedStringNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (element.isNull())
                return false;

            if (!element.isString())
                return JSONExtractRawImpl<JSONParser>::insertResultToFixedStringColumn(dest, element, {});

            auto str = element.getString();
            auto & col_str = assert_cast<ColumnFixedString &>(dest);
            if (str.size() > col_str.getN())
                return false;
            col_str.insertData(str.data(), str.size());

            return true;
        }
    };

    template <typename Type>
    class EnumNode : public Node
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

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            auto & col_vec = assert_cast<ColumnVector<Type> &>(dest);

            if (element.isInt64())
            {
                Type value;
                if (!accurate::convertNumeric(element.getInt64(), value) || !only_values.contains(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (element.isUInt64())
            {
                Type value;
                if (!accurate::convertNumeric(element.getUInt64(), value) || !only_values.contains(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (element.isString())
            {
                auto value = name_to_value_map.find(element.getString());
                if (value == name_to_value_map.end())
                    return false;
                col_vec.insertValue(value->second);
                return true;
            }

            return false;
        }

    private:
        std::vector<std::pair<String, Type>> name_value_pairs;
        std::unordered_map<std::string_view, Type> name_to_value_map;
        std::unordered_set<Type> only_values;
    };

    class NullableNode : public Node
    {
    public:
        explicit NullableNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (dest.getDataType() == TypeIndex::LowCardinality)
            {
                /// We do not need to handle nullability in that case
                /// because nested node handles LowCardinality columns and will call proper overload of `insertData`
                return nested->insertResultToColumn(dest, element);
            }

            ColumnNullable & col_null = assert_cast<ColumnNullable &>(dest);
            if (!nested->insertResultToColumn(col_null.getNestedColumn(), element))
                return false;
            col_null.getNullMapColumn().insertValue(0);
            return true;
        }

    private:
        std::unique_ptr<Node> nested;
    };

    class ArrayNode : public Node
    {
    public:
        explicit ArrayNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (!element.isArray())
                return false;

            auto array = element.getArray();

            ColumnArray & col_arr = assert_cast<ColumnArray &>(dest);
            auto & data = col_arr.getData();
            size_t old_size = data.size();
            bool were_valid_elements = false;

            for (auto value : array)
            {
                if (nested->insertResultToColumn(data, value))
                    were_valid_elements = true;
                else
                    data.insertDefault();
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
        std::unique_ptr<Node> nested;
    };

    class TupleNode : public Node
    {
    public:
        TupleNode(std::vector<std::unique_ptr<Node>> nested_, const std::vector<String> & explicit_names_) : nested(std::move(nested_)), explicit_names(explicit_names_)
        {
            for (size_t i = 0; i != explicit_names.size(); ++i)
                name_to_index_map.emplace(explicit_names[i], i);
        }

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            ColumnTuple & tuple = assert_cast<ColumnTuple &>(dest);
            size_t old_size = dest.size();
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
                    if (nested[index]->insertResultToColumn(tuple.getColumn(index), *it++))
                        were_valid_elements = true;
                    else
                        tuple.getColumn(index).insertDefault();
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
                        if (nested[index]->insertResultToColumn(tuple.getColumn(index), (*it++).second))
                            were_valid_elements = true;
                        else
                            tuple.getColumn(index).insertDefault();
                    }
                }
                else
                {
                    for (const auto & [key, value] : object)
                    {
                        auto index = name_to_index_map.find(key);
                        if (index != name_to_index_map.end())
                        {
                            if (nested[index->second]->insertResultToColumn(tuple.getColumn(index->second), value))
                                were_valid_elements = true;
                        }
                    }
                }

                set_size(old_size + static_cast<size_t>(were_valid_elements));
                return were_valid_elements;
            }

            return false;
        }

    private:
        std::vector<std::unique_ptr<Node>> nested;
        std::vector<String> explicit_names;
        std::unordered_map<std::string_view, size_t> name_to_index_map;
    };

    class MapNode : public Node
    {
    public:
        MapNode(std::unique_ptr<Node> key_, std::unique_ptr<Node> value_) : key(std::move(key_)), value(std::move(value_)) { }

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (!element.isObject())
                return false;

            ColumnMap & map_col = assert_cast<ColumnMap &>(dest);
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
                if (!value->insertResultToColumn(value_col, pair.second))
                    value_col.insertDefault();
            }

            offsets.push_back(old_size + object.size());
            return true;
        }

    private:
        std::unique_ptr<Node> key;
        std::unique_ptr<Node> value;
    };

    class VariantNode : public Node
    {
    public:
        VariantNode(std::vector<std::unique_ptr<Node>> variant_nodes_, std::vector<size_t> order_) : variant_nodes(std::move(variant_nodes_)), order(std::move(order_)) { }

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            auto & column_variant = assert_cast<ColumnVariant &>(dest);
            for (size_t i : order)
            {
                auto & variant = column_variant.getVariantByGlobalDiscriminator(i);
                if (variant_nodes[i]->insertResultToColumn(variant, element))
                {
                    column_variant.getLocalDiscriminators().push_back(column_variant.localDiscriminatorByGlobal(i));
                    column_variant.getOffsets().push_back(variant.size() - 1);
                    return true;
                }
            }

            return false;
        }

    private:
        std::vector<std::unique_ptr<Node>> variant_nodes;
        /// Order in which we should try variants nodes.
        /// For example, String should be always the last one.
        std::vector<size_t> order;
    };

    static std::unique_ptr<Node> build(const char * function_name, const DataTypePtr & type)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::UInt8: return std::make_unique<NumericNode<UInt8>>();
            case TypeIndex::UInt16: return std::make_unique<NumericNode<UInt16>>();
            case TypeIndex::UInt32: return std::make_unique<NumericNode<UInt32>>();
            case TypeIndex::UInt64: return std::make_unique<NumericNode<UInt64>>();
            case TypeIndex::UInt128: return std::make_unique<NumericNode<UInt128>>();
            case TypeIndex::UInt256: return std::make_unique<NumericNode<UInt256>>();
            case TypeIndex::Int8: return std::make_unique<NumericNode<Int8>>();
            case TypeIndex::Int16: return std::make_unique<NumericNode<Int16>>();
            case TypeIndex::Int32: return std::make_unique<NumericNode<Int32>>();
            case TypeIndex::Int64: return std::make_unique<NumericNode<Int64>>();
            case TypeIndex::Int128: return std::make_unique<NumericNode<Int128>>();
            case TypeIndex::Int256: return std::make_unique<NumericNode<Int256>>();
            case TypeIndex::Float32: return std::make_unique<NumericNode<Float32>>();
            case TypeIndex::Float64: return std::make_unique<NumericNode<Float64>>();
            case TypeIndex::String: return std::make_unique<StringNode>();
            case TypeIndex::FixedString: return std::make_unique<FixedStringNode>();
            case TypeIndex::UUID: return std::make_unique<UUIDNode>();
            case TypeIndex::LowCardinality:
            {
                // The low cardinality case is treated in two different ways:
                // For FixedString type, an especial class is implemented for inserting the data in the destination column,
                // as the string length must be passed in order to check and pad the incoming data.
                // For the rest of low cardinality types, the insertion is done in their corresponding class, adapting the data
                // as needed for the insertData function of the ColumnLowCardinality.
                auto dictionary_type = typeid_cast<const DataTypeLowCardinality *>(type.get())->getDictionaryType();
                if ((*dictionary_type).getTypeId() == TypeIndex::FixedString)
                {
                    auto fixed_length = typeid_cast<const DataTypeFixedString *>(dictionary_type.get())->getN();
                    return std::make_unique<LowCardinalityFixedStringNode>(fixed_length);
                }
                return build(function_name, dictionary_type);
            }
            case TypeIndex::Decimal256: return std::make_unique<DecimalNode<Decimal256>>(type);
            case TypeIndex::Decimal128: return std::make_unique<DecimalNode<Decimal128>>(type);
            case TypeIndex::Decimal64: return std::make_unique<DecimalNode<Decimal64>>(type);
            case TypeIndex::Decimal32: return std::make_unique<DecimalNode<Decimal32>>(type);
            case TypeIndex::Enum8:
                return std::make_unique<EnumNode<Int8>>(static_cast<const DataTypeEnum8 &>(*type).getValues());
            case TypeIndex::Enum16:
                return std::make_unique<EnumNode<Int16>>(static_cast<const DataTypeEnum16 &>(*type).getValues());
            case TypeIndex::Nullable:
            {
                return std::make_unique<NullableNode>(build(function_name, static_cast<const DataTypeNullable &>(*type).getNestedType()));
            }
            case TypeIndex::Array:
            {
                return std::make_unique<ArrayNode>(build(function_name, static_cast<const DataTypeArray &>(*type).getNestedType()));
            }
            case TypeIndex::Tuple:
            {
                const auto & tuple = static_cast<const DataTypeTuple &>(*type);
                const auto & tuple_elements = tuple.getElements();
                std::vector<std::unique_ptr<Node>> elements;
                elements.reserve(tuple_elements.size());
                for (const auto & tuple_element : tuple_elements)
                    elements.emplace_back(build(function_name, tuple_element));
                return std::make_unique<TupleNode>(std::move(elements), tuple.haveExplicitNames() ? tuple.getElementNames() : Strings{});
            }
            case TypeIndex::Map:
            {
                const auto & map_type = static_cast<const DataTypeMap &>(*type);
                const auto & key_type = map_type.getKeyType();
                if (!isString(removeLowCardinality(key_type)))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} doesn't support the return type schema: {} with key type not String",
                        String(function_name),
                        type->getName());

                const auto & value_type = map_type.getValueType();
                return std::make_unique<MapNode>(build(function_name, key_type), build(function_name, value_type));
            }
            case TypeIndex::Variant:
            {
                const auto & variant_type = static_cast<const DataTypeVariant &>(*type);
                const auto & variants = variant_type.getVariants();
                std::vector<std::unique_ptr<Node>> variant_nodes;
                variant_nodes.reserve(variants.size());
                for (const auto & variant : variants)
                    variant_nodes.push_back(build(function_name, variant));
                return std::make_unique<VariantNode>(std::move(variant_nodes), SerializationVariant::getVariantsDeserializeTextOrder(variants));
            }
            default:
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Function {} doesn't support the return type schema: {}",
                                String(function_name), type->getName());
        }
    }
};


template <typename JSONParser>
class JSONExtractImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments", String(function_name));

        const auto & col = arguments.back();
        const auto * col_type_const = typeid_cast<const ColumnConst *>(col.column.get());
        if (!col_type_const || !isString(col.type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The last argument of function {} should "
                            "be a constant string specifying the return data type, illegal value: {}",
                            String(function_name), col.name);

        return DataTypeFactory::instance().get(col_type_const->getValue<String>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 2; }

    void prepare(const char * function_name, const ColumnsWithTypeAndName &, const DataTypePtr & result_type)
    {
        extract_tree = JSONExtractTree<JSONParser>::build(function_name, result_type);
    }

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        return extract_tree->insertResultToColumn(dest, element);
    }

protected:
    std::unique_ptr<typename JSONExtractTree<JSONParser>::Node> extract_tree;
};


template <typename JSONParser>
class JSONExtractKeysAndValuesImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments", String(function_name));

        const auto & col = arguments.back();
        const auto * col_type_const = typeid_cast<const ColumnConst *>(col.column.get());
        if (!col_type_const || !isString(col.type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "The last argument of function {} should "
                            "be a constant string specifying the values' data type, illegal value: {}",
                            String(function_name), col.name);

        DataTypePtr key_type = std::make_unique<DataTypeString>();
        DataTypePtr value_type = DataTypeFactory::instance().get(col_type_const->getValue<String>());
        DataTypePtr tuple_type = std::make_unique<DataTypeTuple>(DataTypes{key_type, value_type});
        return std::make_unique<DataTypeArray>(tuple_type);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 2; }

    void prepare(const char * function_name, const ColumnsWithTypeAndName &, const DataTypePtr & result_type)
    {
        const auto tuple_type = typeid_cast<const DataTypeArray *>(result_type.get())->getNestedType();
        const auto value_type = typeid_cast<const DataTypeTuple *>(tuple_type.get())->getElements()[1];
        extract_tree = JSONExtractTree<JSONParser>::build(function_name, value_type);
    }

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        if (!element.isObject())
            return false;

        auto object = element.getObject();

        auto & col_arr = assert_cast<ColumnArray &>(dest);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        size_t old_size = col_tuple.size();
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_value = col_tuple.getColumn(1);

        for (const auto & [key, value] : object)
        {
            if (extract_tree->insertResultToColumn(col_value, value))
                col_key.insertData(key.data(), key.size());
        }

        if (col_tuple.size() == old_size)
            return false;

        col_arr.getOffsets().push_back(col_tuple.size());
        return true;
    }

private:
    std::unique_ptr<typename JSONExtractTree<JSONParser>::Node> extract_tree;
};


template <typename JSONParser>
class JSONExtractRawImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        if (dest.getDataType() == TypeIndex::LowCardinality)
        {
            ColumnString::Chars chars;
            WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
            traverse(element, buf);
            buf.finalize();
            assert_cast<ColumnLowCardinality &>(dest).insertData(reinterpret_cast<const char *>(chars.data()), chars.size());
        }
        else
        {
            ColumnString & col_str = assert_cast<ColumnString &>(dest);
            auto & chars = col_str.getChars();
            WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
            traverse(element, buf);
            buf.finalize();
            chars.push_back(0);
            col_str.getOffsets().push_back(chars.size());
        }
        return true;
    }

    // We use insertResultToFixedStringColumn in case we are inserting raw data in a FixedString column
    static bool insertResultToFixedStringColumn(IColumn & dest, const Element & element, std::string_view)
    {
        ColumnFixedString::Chars chars;
        WriteBufferFromVector<ColumnFixedString::Chars> buf(chars, AppendModeTag());
        traverse(element, buf);
        buf.finalize();

        auto & col_str = assert_cast<ColumnFixedString &>(dest);

        if (chars.size() > col_str.getN())
            return false;

        chars.resize_fill(col_str.getN());
        col_str.insertData(reinterpret_cast<const char *>(chars.data()), chars.size());


        return true;
    }

    // We use insertResultToLowCardinalityFixedStringColumn in case we are inserting raw data in a Low Cardinality FixedString column
    static bool insertResultToLowCardinalityFixedStringColumn(IColumn & dest, const Element & element, size_t fixed_length)
    {
        if (element.getObject().size() > fixed_length)
            return false;

        ColumnFixedString::Chars chars;
        WriteBufferFromVector<ColumnFixedString::Chars> buf(chars, AppendModeTag());
        traverse(element, buf);
        buf.finalize();

        if (chars.size() > fixed_length)
            return false;
        chars.resize_fill(fixed_length);
        assert_cast<ColumnLowCardinality &>(dest).insertData(reinterpret_cast<const char *>(chars.data()), chars.size());

        return true;
    }

private:
    static void traverse(const Element & element, WriteBuffer & buf)
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
            writeJSONString(element.getString(), buf, formatSettings());
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
                traverse(value, buf);
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
                writeJSONString(key, buf, formatSettings());
                writeChar(':', buf);
                traverse(value, buf);
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

    static const FormatSettings & formatSettings()
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


template <typename JSONParser>
class JSONExtractArrayRawImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        if (!element.isArray())
            return false;

        auto array = element.getArray();
        ColumnArray & col_res = assert_cast<ColumnArray &>(dest);

        for (auto value : array)
            JSONExtractRawImpl<JSONParser>::insertResultToColumn(col_res.getData(), value, {});

        col_res.getOffsets().push_back(col_res.getOffsets().back() + array.size());
        return true;
    }
};


template <typename JSONParser>
class JSONExtractKeysAndValuesRawImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        DataTypePtr string_type = std::make_unique<DataTypeString>();
        DataTypePtr tuple_type = std::make_unique<DataTypeTuple>(DataTypes{string_type, string_type});
        return std::make_unique<DataTypeArray>(tuple_type);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        if (!element.isObject())
            return false;

        auto object = element.getObject();

        auto & col_arr = assert_cast<ColumnArray &>(dest);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_value = assert_cast<ColumnString &>(col_tuple.getColumn(1));

        for (const auto & [key, value] : object)
        {
            col_key.insertData(key.data(), key.size());
            JSONExtractRawImpl<JSONParser>::insertResultToColumn(col_value, value, {});
        }

        col_arr.getOffsets().push_back(col_arr.getOffsets().back() + object.size());
        return true;
    }
};

template <typename JSONParser>
class JSONExtractKeysImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_unique<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view)
    {
        if (!element.isObject())
            return false;

        auto object = element.getObject();

        ColumnArray & col_res = assert_cast<ColumnArray &>(dest);
        auto & col_key = assert_cast<ColumnString &>(col_res.getData());

        for (const auto & [key, value] : object)
        {
            col_key.insertData(key.data(), key.size());
        }

        col_res.getOffsets().push_back(col_res.getOffsets().back() + object.size());
        return true;
    }
};

}
