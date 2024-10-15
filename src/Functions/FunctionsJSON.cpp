#include <type_traits>
#include <boost/tti/has_member_function.hpp>

#include <base/range.h>

#include <Formats/JSONExtractTree.h>
#include <Formats/FormatFactory.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/AccurateComparison.h>
#include <Core/Settings.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Functions/FunctionHelpers.h>

#include <Interpreters/Context.h>

#include "config.h"


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_simdjson;
}

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
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, const FormatSettings & format_settings)
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

            String error;
            for (size_t i = 0; i < input_rows_count; ++i)
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
                        added_to_column = impl.insertResultToColumn(*to, element, last_key, format_settings, error);
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
    enum class MoveType : uint8_t
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
                    Int64 index = (*arguments[j + 1].column)[row].safeGet<Int64>();
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
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t size = offsets[i] - offsets[i - 1];
            max_size = std::max(max_size, size);
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
    explicit ExecutableFunctionJSON(const NullPresence & null_presence_, bool allow_simdjson_, const DataTypePtr & json_return_type_, const FormatSettings & format_settings_)
        : null_presence(null_presence_), allow_simdjson(allow_simdjson_), json_return_type(json_return_type_), format_settings(format_settings_)
    {
        /// Don't escape forward slashes during converting JSON elements to raw string.
        format_settings.json.escape_forward_slashes = false;
        /// Don't insert default values on null during traversing the JSON element.
        /// We allow to insert null only to Nullable columns in JSONExtract functions.
        format_settings.null_as_default = false;
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
            return FunctionJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count, format_settings);
#endif

#if USE_RAPIDJSON
        return FunctionJSONHelpers::Executor<Name, Impl, RapidJSONParser>::run(arguments, result_type, input_rows_count, format_settings);
#else
        return FunctionJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count, format_settings);
#endif
    }

    NullPresence null_presence;
    bool allow_simdjson;
    DataTypePtr json_return_type;
    FormatSettings format_settings;
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
        DataTypePtr json_return_type_,
        const FormatSettings & format_settings_)
        : null_presence(null_presence_)
        , allow_simdjson(allow_simdjson_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
        , json_return_type(std::move(json_return_type_))
        , format_settings(format_settings_)
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
        return std::make_unique<ExecutableFunctionJSON<Name, Impl>>(null_presence, allow_simdjson, json_return_type, format_settings);
    }

private:
    NullPresence null_presence;
    bool allow_simdjson;
    DataTypes argument_types;
    DataTypePtr return_type;
    DataTypePtr json_return_type;
    FormatSettings format_settings;
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

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.emplace_back(argument.type);
        return std::make_unique<FunctionBaseFunctionJSON<Name, Impl>>(
            null_presence, getContext()->getSettingsRef()[Setting::allow_simdjson], argument_types, return_type, json_return_type, getFormatSettings(getContext()));
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

    static bool insertResultToColumn(IColumn & dest, const Element &, std::string_view, const FormatSettings &, String &)
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

    static bool insertResultToColumn(IColumn & dest, const Element &, std::string_view, const FormatSettings &, String &)
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings &, String &)
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

    static bool insertResultToColumn(IColumn & dest, const Element &, std::string_view last_key, const FormatSettings &, String &)
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings &, String &)
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

    static const std::unique_ptr<JSONExtractTreeNode<JSONParser>> & getInsertNode()
    {
        static const std::unique_ptr<JSONExtractTreeNode<JSONParser>> node = buildJSONExtractTree<JSONParser>(std::make_shared<DataTypeNumber<NumberType>>());
    }

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings &, String & error)
    {
        NumberType value;

        if (!tryGetNumericValueFromJSONElement<JSONParser, NumberType>(value, element, convert_bool_to_integer, /*allow_type_conversion=*/true, error))
            return false;
        auto & col_vec = assert_cast<ColumnVector<NumberType> &>(dest);
        col_vec.insertValue(value);
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings &, String &)
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings & format_settings, String & error)
    {
        if (element.isNull())
            return false;

        if (!element.isString())
            return JSONExtractRawImpl<JSONParser>::insertResultToColumn(dest, element, {}, format_settings, error);

        auto str = element.getString();
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(str.data(), str.size());
        return true;
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
        extract_tree = buildJSONExtractTree<JSONParser>(result_type, function_name);
        insert_settings.insert_default_on_invalid_elements_in_complex_types = true;
    }

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings & format_settings, String & error)
    {
        return extract_tree->insertResultToColumn(dest, element, insert_settings, format_settings, error);
    }

protected:
    std::unique_ptr<JSONExtractTreeNode<JSONParser>> extract_tree;
    JSONExtractInsertSettings insert_settings;
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
        extract_tree = buildJSONExtractTree<JSONParser>(value_type, function_name);
        insert_settings.insert_default_on_invalid_elements_in_complex_types = true;
    }

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings & format_settings, String & error)
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
            if (extract_tree->insertResultToColumn(col_value, value, insert_settings, format_settings, error))
                col_key.insertData(key.data(), key.size());
        }

        if (col_tuple.size() == old_size)
            return false;

        col_arr.getOffsets().push_back(col_tuple.size());
        return true;
    }

private:
    std::unique_ptr<JSONExtractTreeNode<JSONParser>> extract_tree;
    JSONExtractInsertSettings insert_settings;
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings & format_settings, String &)
    {
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        auto & chars = col_str.getChars();
        WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
        jsonElementToString<JSONParser>(element, buf, format_settings);
        buf.finalize();
        chars.push_back(0);
        col_str.getOffsets().push_back(chars.size());
        return true;
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings & format_settings, String & error)
    {
        if (!element.isArray())
            return false;

        auto array = element.getArray();
        ColumnArray & col_res = assert_cast<ColumnArray &>(dest);

        for (auto value : array)
            JSONExtractRawImpl<JSONParser>::insertResultToColumn(col_res.getData(), value, {}, format_settings, error);

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

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings & format_settings, String & error)
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
            JSONExtractRawImpl<JSONParser>::insertResultToColumn(col_value, value, {}, format_settings, error);
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

    bool insertResultToColumn(IColumn & dest, const Element & element, std::string_view, const FormatSettings &, String &)
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

REGISTER_FUNCTION(JSON)
{
    factory.registerFunction<JSONOverloadResolver<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameIsValidJSON, IsValidJSONImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractArrayRaw, JSONExtractArrayRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeysAndValuesRaw, JSONExtractKeysAndValuesRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeys, JSONExtractKeysImpl>>();
}

}
