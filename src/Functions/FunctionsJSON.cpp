#include <type_traits>

#include <base/range.h>

#include <Common/CpuId.h>
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
#include <Columns/ColumnObject.h>

#include <DataTypes/Serializations/SerializationDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/ObjectUtils.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Functions/FunctionHelpers.h>

#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>


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

template <typename T>
concept HasMemberReserve = requires (T t)
{
    t.reserve(0);
};

static Int64 adjustIndex(Int64 index, UInt64 array_size)
{
    return index >= 0 ? index - 1 : index + array_size;
}

static bool isDummyTuple(const IDataType & type)
{
    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type);
    if (!type_tuple || !type_tuple->haveExplicitNames())
        return false;

    const auto & tuple_names = type_tuple->getElementNames();
    return tuple_names.size() == 1
        && tuple_names[0] == ColumnObject::COLUMN_NAME_DUMMY
        && isUInt8(type_tuple->getElement(0));
}

/// Functions to parse JSONs and extract values from it.
/// The first argument of all these functions gets a JSON,
/// after that there are any number of arguments specifying path to a desired part from the JSON's root.
/// For example,
/// select JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100

class FunctionJSONHelpers
{
public:
    template <typename Name, template<typename> typename Impl, class JSONParser>
    class ExecutorString
    {
    public:
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.empty())
                throw Exception{"Function " + String(Name::name) + " requires at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

            const auto & first_column = arguments[0];
            if (!isString(first_column.type))
                throw Exception{"The first argument of function " + String(Name::name) + " should be a string containing JSON, illegal type: " + first_column.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            const ColumnPtr & arg_json = first_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_string)
                throw Exception{"Illegal column " + arg_json->getName(), ErrorCodes::ILLEGAL_COLUMN};

            const ColumnString::Chars & chars = col_json_string->getChars();
            const ColumnString::Offsets & offsets = col_json_string->getOffsets();

            Impl<JSONElementIterator<JSONParser>> impl;
            size_t num_index_arguments = impl.getNumberOfIndexArguments(arguments);
            std::vector<Move> moves = prepareMoves(Name::name, arguments, 1, num_index_arguments);

            /// Preallocate memory in parser if necessary.
            JSONParser parser;

            if constexpr (HasMemberReserve<JSONParser>)
            {
                size_t max_size = calculateMaxSize(offsets);
                if (max_size)
                    parser.reserve(max_size);
            }

            constexpr bool has_member_prepare = requires
            {
                impl.prepare("", DataTypePtr{});
            };

            /// prepare() does Impl-specific preparation before handling each row.
            if constexpr (has_member_prepare)
                impl.prepare(Name::name, result_type);

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
                    JSONElementIterator<JSONParser> iterator(document);
                    bool moves_ok = performMoves(arguments, i, moves, iterator);

                    if (moves_ok)
                        added_to_column = impl.insertResultToColumn(*to, iterator);
                }

                /// We add default value (=null or zero) if something goes wrong, we don't throw exceptions in these JSON functions.
                if (!added_to_column)
                    to->insertDefault();
            }
            return to;
        }
    };

    template <typename Name, template<typename> typename Impl>
    class ExecutorObject
    {
    public:
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires at least one argument", Name::name);

            const auto & first_column = arguments[0];
            if (!isObject(first_column.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument of function {} should be an Object, illegal type: {}",
                    Name::name, first_column.type->getName());

            const auto & arg_json = first_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_object
                = typeid_cast<const ColumnObject *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_object)
                throw Exception{ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", arg_json->getName()};

            Impl<ObjectIterator> impl;

            size_t num_index_arguments = impl.getNumberOfIndexArguments(arguments);
            std::vector<Move> moves = prepareMoves(Name::name, arguments, 1, num_index_arguments);
            auto [column_tuple, type_tuple] = unflattenObjectToTuple(*col_json_object);

            constexpr bool has_member_prepare = requires
            {
                impl.prepare("", DataTypePtr{});
            };

            if constexpr (has_member_prepare)
                impl.prepare(Name::name, result_type);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                bool added_to_column = false;

                ObjectIterator iterator(type_tuple, column_tuple, col_json_const ? 0 : i);
                bool moves_ok = performMoves(arguments, i, moves, iterator);

                if (moves_ok)
                    added_to_column = impl.insertResultToColumn(*to, iterator);

                /// We add default value (=null or zero) if something goes wrong, we don't throw exceptions in these JSON functions.
                if (!added_to_column)
                    to->insertDefault();
            }

            return to;
        }
    };

    template <typename JSONParser>
    struct JSONElementIterator
    {
    public:
        using Element = typename JSONParser::Element;

        explicit JSONElementIterator(Element element_) : element(std::move(element_)) {}

        /// Performs moves of types MoveType::Key and MoveType::ConstKey
        bool moveToElementByKey(std::string_view key)
        {
            if (!element.isObject())
                return false;

            auto object = element.getObject();
            if (!object.find(key, element))
                return false;

            last_key = key;
            return true;
        }

        bool moveToElementByIndex(Int64 index)
        {
            if (element.isArray())
            {
                auto array = element.getArray();
                index = adjustIndex(index, array.size());

                if (static_cast<size_t>(index) >= array.size())
                    return false;

                element = array[index];
                last_key = {};
                return true;
            }

            if constexpr (HasIndexOperator<typename JSONParser::Object>)
            {
                if (element.isObject())
                {
                    auto object = element.getObject();
                    index = adjustIndex(index, object.size());

                    if (static_cast<size_t>(index) >= object.size())
                        return false;

                    std::tie(last_key, element) = object[index];
                    return true;
                }
            }

            return false;
        }

        const Element & getElement() const { return element; }
        std::string_view getLastKey() const { return last_key; }

    private:
        Element element;
        std::string_view last_key;
    };

    struct ObjectIterator
    {
    public:
        ObjectIterator(DataTypePtr type_, ColumnPtr column_, Int64 row_)
            : type(std::move(type_)), column(std::move(column_)), row(row_)
        {
        }

        /// Performs moves of types MoveType::Key and MoveType::ConstKey
        bool moveToElementByKey(std::string_view key)
        {
            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
            if (!type_tuple || !type_tuple->haveExplicitNames())
                return false;

            auto pos = type_tuple->tryGetPositionByName(key);
            if (!pos)
                return false;

            type = type_tuple->getElement(*pos);
            column = assert_cast<const ColumnTuple &>(*column).getColumnPtr(*pos);
            last_key = key;
            return true;
        }

        bool moveToElementByIndex(Int64 index)
        {
            if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
            {
                if (isDummyTuple(*type))
                    return false;

                const auto & elements = type_tuple->getElements();
                index = adjustIndex(index, elements.size());
                if (static_cast<UInt64>(index) >= elements.size())
                    return false;

                type = elements[index];
                column = assert_cast<const ColumnTuple &>(*column).getColumnPtr(index);
                last_key = type_tuple->getElementNames()[index];
                return true;
            }

            const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
            if (!type_array)
                return false;

            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            const auto & offsets = column_array.getOffsets();

            UInt64 array_size = offsets[row] - offsets[row - 1];
            index = adjustIndex(index, array_size);
            if (static_cast<UInt64>(index) >= array_size)
                return false;

            type = type_array->getNestedType();
            column = column_array.getDataPtr();
            row = offsets[row - 1] + index;
            last_key = {};
            return true;
        }

        void setRow(Int64 row_) { row = row_; }

        const DataTypePtr & getType() const { return type; }
        const ColumnPtr & getColumn() const { return column; }
        Int64 getRow() const { return row; }
        std::string_view getLastKey() const { return last_key; }

    private:
        DataTypePtr type;
        ColumnPtr column;
        Int64 row = 0;
        std::string_view last_key;
    };

private:
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
            if (!isString(column.type) && !isInteger(column.type))
                throw Exception{"The argument " + std::to_string(i + 1) + " of function " + String(function_name)
                                    + " should be a string specifying key or an integer specifying index, illegal type: " + column.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

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
    template <typename Iterator>
    static bool performMoves(
        const ColumnsWithTypeAndName & arguments, size_t row,
        const std::vector<Move> & moves, Iterator & iterator)
    {
        for (size_t j = 0; j != moves.size(); ++j)
        {
            switch (moves[j].type)
            {
                case MoveType::ConstIndex:
                {
                    if (!iterator.moveToElementByIndex(moves[j].index))
                        return false;
                    break;
                }
                case MoveType::ConstKey:
                {
                    if (!iterator.moveToElementByKey(moves[j].key))
                        return false;
                    break;
                }
                case MoveType::Index:
                {
                    Int64 index = (*arguments[j + 1].column)[row].get<Int64>();
                    if (!iterator.moveToElementByIndex(index))
                        return false;
                    break;
                }
                case MoveType::Key:
                {
                    auto key = (*arguments[j + 1].column).getDataAt(row).toView();
                    if (!iterator.moveToElementByKey(key))
                        return false;
                    break;
                }
            }
        }

        return true;
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


template <typename Name, template<typename> typename Impl>
class ExecutableFunctionJSONString : public IExecutableFunction, WithContext
{
public:
    explicit ExecutableFunctionJSONString(const NullPresence & null_presence_, bool allow_simdjson_, const DataTypePtr & json_return_type_)
        : null_presence(null_presence_), allow_simdjson(allow_simdjson_), json_return_type(json_return_type_)
    {
    }

    String getName() const override { return Name::name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        ColumnsWithTypeAndName temporary_columns = null_presence.has_nullable ? createBlockWithNestedColumns(arguments) : arguments;
        ColumnPtr temporary_result = chooseAndRunJSONParser(temporary_columns, json_return_type, input_rows_count);
        if (null_presence.has_nullable)
            return wrapInNullable(temporary_result, arguments, result_type, input_rows_count);
        return temporary_result;
    }

private:

    ColumnPtr
    chooseAndRunJSONParser(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
#if USE_SIMDJSON
        if (allow_simdjson)
            return FunctionJSONHelpers::ExecutorString<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count);
#endif

#if USE_RAPIDJSON
        return FunctionJSONHelpers::ExecutorString<Name, Impl, RapidJSONParser>::run(arguments, result_type, input_rows_count);
#else
        return FunctionJSONHelpers::ExecutorString<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count);
#endif
    }

    NullPresence null_presence;
    bool allow_simdjson;
    DataTypePtr json_return_type;
};

template <typename Name, template<typename> typename Impl>
class ExecutableFunctionJSONObject : public IExecutableFunction, WithContext
{

public:
    explicit ExecutableFunctionJSONObject(const NullPresence & null_presence_, const DataTypePtr & json_return_type_)
        : null_presence(null_presence_), json_return_type(json_return_type_)
    {
    }

    String getName() const override { return Name::name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (null_presence.has_null_constant)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        assert(!arguments.empty());
        auto temp_arguments = null_presence.has_nullable ? createBlockWithNestedColumns(arguments) : arguments;

        const auto & type_object = assert_cast<const DataTypeObject &>(*temp_arguments[0].type);
        const auto & arg_object = temp_arguments[0].column;
        const auto * column_const = typeid_cast<const ColumnConst *>(arg_object.get());
        const auto * column_object = typeid_cast<const ColumnObject *>(
            column_const ? column_const->getDataColumnPtr().get() : arg_object.get());

        assert(column_object);
        if (column_object->hasNullableSubcolumns())
        {
            auto non_nullable_object = ColumnObject::create(false);
            for (const auto & entry : column_object->getSubcolumns())
            {
                auto new_subcolumn = recursiveAssumeNotNullable(entry->data.getFinalizedColumnPtr());
                non_nullable_object->addSubcolumn(entry->path, new_subcolumn->assumeMutable());
            }

            temp_arguments[0].type = std::make_shared<DataTypeObject>(type_object.getSchemaFormat(), false);
            temp_arguments[0].column = std::move(non_nullable_object);

            if (column_const)
                temp_arguments[0].column = ColumnConst::create(temp_arguments[0].column, column_const->size());
        }

        auto temporary_result = FunctionJSONHelpers::ExecutorObject<Name, Impl>::run(temp_arguments, json_return_type, input_rows_count);
        if (null_presence.has_nullable)
            return wrapInNullable(temporary_result, arguments, result_type, input_rows_count);
        return temporary_result;
    }

private:
    NullPresence null_presence;
    DataTypePtr json_return_type;
};


template <typename Name>
class FunctionBaseFunctionJSON : public IFunctionBase
{
public:
    String getName() const override { return Name::name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return return_type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

protected:
    explicit FunctionBaseFunctionJSON(
        const NullPresence & null_presence_,
        DataTypes argument_types_,
        DataTypePtr return_type_,
        DataTypePtr json_return_type_)
        : null_presence(null_presence_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
        , json_return_type(std::move(json_return_type_))
    {
    }

    NullPresence null_presence;
    DataTypes argument_types;
    DataTypePtr return_type;
    DataTypePtr json_return_type;
};

template <typename Name, template<typename> typename Impl>
class FunctionBaseFunctionJSONString : public FunctionBaseFunctionJSON<Name>
{
public:
    template <typename... Args>
    explicit FunctionBaseFunctionJSONString(bool allow_simdjson_, Args &&... args)
        : FunctionBaseFunctionJSON<Name>{std::forward<Args>(args)...}
        , allow_simdjson(allow_simdjson_)
    {
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSONString<Name, Impl>>(this->null_presence, this->allow_simdjson, this->json_return_type);
    }

private:
    bool allow_simdjson;
};

template <typename Name, template<typename> typename Impl>
class FunctionBaseFunctionJSONObject : public FunctionBaseFunctionJSON<Name>
{
public:
    template <typename... Args>
    explicit FunctionBaseFunctionJSONObject(Args &&... args)
        : FunctionBaseFunctionJSON<Name>{std::forward<Args>(args)...}
    {
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSONObject<Name, Impl>>(this->null_presence, this->json_return_type);
    }
};

using ObjectIterator = FunctionJSONHelpers::ObjectIterator;

template <typename Iterator>
concept IsObjectIterator = std::is_same_v<Iterator, ObjectIterator>;

template<typename JSONParser>
using ElementIterator = FunctionJSONHelpers::JSONElementIterator<JSONParser>;

template <typename Iterator>
concept IsElementIterator =
    std::is_same_v<Iterator, ElementIterator<DummyJSONParser>>
#if USE_SIMDJSON
    || std::is_same_v<Iterator, ElementIterator<SimdJSONParser>>
#endif
#if USE_RAPIDJSON
    || std::is_same_v<Iterator, ElementIterator<RapidJSONParser>>
#endif
    ;

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

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const override
    {
        bool has_nothing_argument = false;
        for (const auto & arg : arguments)
            has_nothing_argument |= isNothing(arg.type);

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least one argument", Name::name);

        const auto & first_column = arguments[0];
        auto first_type_base = removeNullable(removeLowCardinality(first_column.type));

        bool is_string = isString(first_type_base);
        bool is_object = isObject(first_type_base);

        if (!is_string && !is_object && !isNothing(first_type_base))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The first argument of function {} should be a string containing JSON or Object, illegal type: {}",
                Name::name, first_column.type->getName());

        auto json_return_type = Impl<ObjectIterator>::getReturnType(Name::name, createBlockWithNestedColumns(arguments));

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

        if (is_string)
            return std::make_unique<FunctionBaseFunctionJSONString<Name, Impl>>(
                getContext()->getSettingsRef().allow_simdjson, null_presence, argument_types, return_type, json_return_type);
        else
            return std::make_unique<FunctionBaseFunctionJSONObject<Name, Impl>>(
                null_presence, argument_types, return_type, json_return_type);
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

template <typename Iterator>
class JSONHasImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator &)
    {
        auto & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(1);
        return true;
    }
};

template <typename Iterator>
class IsValidJSONImpl
{
public:
    static DataTypePtr getReturnType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
            /// IsValidJSON() shouldn't get parameters other than JSON.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs exactly one argument", function_name);

        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName &) { return 0; }

    static bool insertResultToColumn(IColumn & dest, const Iterator &)
    {
        /// This function is called only if JSON is valid.
        /// If JSON isn't valid then `FunctionJSON::Executor::run()` adds default value (=zero) to `dest` without calling this function.
        auto & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(1);
        return true;
    }
};

template <typename Iterator>
class JSONLengthImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeUInt64>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();

        size_t size;
        if (element.isArray())
            size = element.getArray().size();
        else if (element.isObject())
            size = element.getObject().size();
        else
            return false;

        auto & col_vec = assert_cast<ColumnVector<UInt64> &>(dest);
        col_vec.insertValue(size);
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        auto & to_vec = assert_cast<ColumnVector<UInt64> &>(dest);

        const auto * column_array = typeid_cast<const ColumnArray *>(iterator.getColumn().get());
        if (column_array)
        {
            const auto & offsets = column_array->getOffsets();
            auto row = iterator.getRow();
            UInt64 size = offsets[row] - offsets[row - 1];
            to_vec.insertValue(size);
            return true;
        }

        const auto * column_tuple = typeid_cast<const ColumnTuple *>(iterator.getColumn().get());
        if (column_tuple)
        {
            if (isDummyTuple(*iterator.getType()))
                return false;

            UInt64 size = column_tuple->getColumns().size();
            to_vec.insertValue(size);
            return true;
        }

        return false;
    }
};

template <typename Iterator>
class JSONKeyImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator)
    {
        auto last_key = iterator.getLastKey();
        if (last_key.empty())
            return false;

        auto & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(last_key.data(), last_key.size());
        return true;
    }
};

template <typename Iterator>
class JSONTypeImpl
{
public:
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

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        UInt8 type;
        if (element.isInt64())
            type = 'i';
        else if (element.isUInt64())
            type = 'u';
        else if (element.isDouble())
            type = 'd';
        else if (element.isBool())
            type = 'b';
        else if (element.isString())
            type = '"';
        else if (element.isArray())
            type = '[';
        else if (element.isObject())
            type = '{';
        else if (element.isNull())
            type = 0;
        else
            return false;

        auto & col_vec = assert_cast<ColumnVector<Int8> &>(dest);
        col_vec.insertValue(type);
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        WhichDataType which(iterator.getType());
        UInt8 type;

        if (which.isNativeInt())
            type = 'i';
        else if (which.isNativeUInt())
            type = 'u';
        else if (which.isFloat())
            type = 'd';
        else if (which.isString())
            type = '"';
        else if (which.isArray())
            type = '[';
        else if (which.isTuple())
            type = '{';
        else
            return false;

        auto & to_vec = assert_cast<ColumnVector<Int8> &>(dest);
        to_vec.insertValue(type);
        return true;
    }
};

template <typename Iterator, typename NumberType, bool convert_bool_to_integer = false>
class JSONExtractNumericImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeNumber<NumberType>>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        NumberType value;

        if (element.isInt64())
        {
            if (!accurate::convertNumeric(element.getInt64(), value))
                return false;
        }
        else if (element.isUInt64())
        {
            if (!accurate::convertNumeric(element.getUInt64(), value))
                return false;
        }
        else if (element.isDouble())
        {
            if constexpr (std::is_floating_point_v<NumberType>)
            {
                /// We permit inaccurate conversion of double to float.
                /// Example: double 0.1 from JSON is not representable in float.
                /// But it will be more convenient for user to perform conversion.
                value = static_cast<NumberType>(element.getDouble());
            }
            else if (!accurate::convertNumeric(element.getDouble(), value))
                return false;
        }
        else if (element.isBool() && is_integer<NumberType> && convert_bool_to_integer)
        {
            value = static_cast<NumberType>(element.getBool());
        }
        else
            return false;

        auto & col_vec = assert_cast<ColumnVector<NumberType> &>(dest);
        col_vec.insertValue(value);
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto & from = iterator.getColumn();
        UInt64 row = iterator.getRow();

        bool converted = false;
        NumberType value{};

    #define DISPATCH(TYPE) \
        if (const auto * from_vec = typeid_cast<const ColumnVector<TYPE> *>(from.get())) \
            converted = accurate::convertNumeric(from_vec->getData()[row], value);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
    #undef DISPATCH

        if (converted)
        {
            auto & to_vec = assert_cast<ColumnVector<NumberType> &>(dest);
            to_vec.insertValue(value);
        }

        return converted;
    }
};

#define DISPATCH(TYPE) \
    template <typename Iterator> \
    using JSONExtract##TYPE##Impl = JSONExtractNumericImpl<Iterator, TYPE>;
FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH


template <typename Iterator>
class JSONExtractBoolImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        if (!element.isBool())
            return false;

        auto & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(static_cast<UInt8>(element.getBool()));
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        return JSONExtractUInt8Impl<Iterator>::insertResultToColumn(dest, iterator);
    }
};

template <typename Iterator>
class JSONExtractRawImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        auto & col_str = assert_cast<ColumnString &>(dest);
        auto & chars = col_str.getChars();
        WriteBufferFromVector<ColumnString::Chars> buf(chars, AppendModeTag());
        traverse(element, buf);
        buf.finalize();
        chars.push_back(0);
        col_str.getOffsets().push_back(chars.size());
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto & type = iterator.getType();
        const auto & column = iterator.getColumn();
        UInt64 row = iterator.getRow();

        auto serialization = type->getDefaultSerialization();
        auto & to_string = assert_cast<ColumnString &>(dest);
        auto & to_chars = to_string.getChars();
        auto & to_offsets = to_string.getOffsets();

        WriteBufferFromVector buf(to_chars, AppendModeTag{});

        if (isDummyTuple(*type))
            writeString("{}", buf);
        else
            serialization->serializeTextJSON(*column, row, buf, formatSettings());

        writeChar(0, buf);
        buf.finalize();
        to_offsets.push_back(to_chars.size());
        return true;
    }

private:
    template <typename Element>
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
            settings.json.named_tuples_as_objects = true;
            return settings;
        }();
        return the_instance;
    }
};

template <typename Iterator>
class JSONExtractStringImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        if (element.isNull())
            return false;

        if (!element.isString())
            return JSONExtractRawImpl<Iterator>::insertResultToColumn(dest, iterator);

        auto str = element.getString();
        auto & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(str.data(), str.size());
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto & column = iterator.getColumn();
        UInt64 row = iterator.getRow();

        if (const auto * column_string = typeid_cast<const ColumnString *>(column.get()))
        {
            dest.insertFrom(*column_string, row);
            return true;
        }

        return JSONExtractRawImpl<Iterator>::insertResultToColumn(dest, iterator);
    }
};

/// Nodes of the extract tree. We need the extract tree to extract from JSON complex values containing array, tuples or nullables.
template <typename Iterator>
struct JSONExtractTree
{
    class Node
    {
    public:
        Node() = default;
        virtual ~Node() = default;
        virtual bool insertResultToColumn(IColumn &, const Iterator &) = 0;
    };

    template <typename NumberType>
    class NumericNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            return JSONExtractNumericImpl<Iterator, NumberType, true>::insertResultToColumn(dest, iterator);
        }
    };

    class LowCardinalityNode : public Node
    {
    public:
        LowCardinalityNode(DataTypePtr dictionary_type_, std::unique_ptr<Node> impl_)
            : dictionary_type(std::move(dictionary_type_)), impl(std::move(impl_))
        {
        }

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            auto from_col = dictionary_type->createColumn();
            if (impl->insertResultToColumn(*from_col, iterator))
            {
                std::string_view value = from_col->getDataAt(0).toView();
                assert_cast<ColumnLowCardinality &>(dest).insertData(value.data(), value.size());
                return true;
            }
            return false;
        }
    private:
        DataTypePtr dictionary_type;
        std::unique_ptr<Node> impl;
    };

    class UUIDNodeString : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto & element = iterator.getElement();
            if (!element.isString())
                return false;

            auto uuid = parseFromString<UUID>(element.getString());
            assert_cast<ColumnUUID &>(dest).insert(uuid);
            return true;
        }
    };

    class UUIDNodeObject : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            if (!isString(iterator.getType()))
                return false;

            const auto & column_string = assert_cast<const ColumnString &>(*iterator.getColumn());
            auto uuid = parseFromString<UUID>(column_string.getDataAt(iterator.getRow()).toView());
            assert_cast<ColumnUUID &>(dest).insert(uuid);
            return true;
        }
    };

    using UUIDNode = std::conditional_t<IsObjectIterator<Iterator>, UUIDNodeObject, UUIDNodeString>;

    template <typename DecimalType>
    class DecimalNodeString : public Node
    {
    public:
        explicit DecimalNodeString(DataTypePtr data_type_) : data_type(std::move(data_type_)) {}

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto & element = iterator.getElement();
            const auto * type = assert_cast<const DataTypeDecimal<DecimalType> *>(data_type.get());

            DecimalType result{};
            bool converted = false;
            if (element.isDouble())
                converted = tryConvertToDecimal<DataTypeNumber<Float64>, DataTypeDecimal<DecimalType>>(element.getDouble(), type->getScale(), result);
            else if (element.isInt64())
                converted = tryConvertToDecimal<DataTypeNumber<Int64>, DataTypeDecimal<DecimalType>>(element.getInt64(), type->getScale(), result);
            else if (element.isUInt64())
                converted = tryConvertToDecimal<DataTypeNumber<UInt64>, DataTypeDecimal<DecimalType>>(element.getUInt64(), type->getScale(), result);

            if (converted)
                assert_cast<ColumnDecimal<DecimalType> &>(dest).insert(result);

            return converted;
        }

    private:
        DataTypePtr data_type;
    };

    template <typename DecimalType>
    class DecimalNodeObject: public Node
    {
    public:
        explicit DecimalNodeObject(DataTypePtr data_type_) : data_type(std::move(data_type_)) {}

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<DecimalType> *>(data_type.get());
            const auto & from = iterator.getColumn();

            DecimalType result{};
            bool converted = false;

        #define DISPATCH(TYPE) \
            if (const auto * from_vec = typeid_cast<const ColumnVector<TYPE> *>(from.get())) \
                converted = tryConvertToDecimal<DataTypeNumber<TYPE>, DataTypeDecimal<DecimalType>>( \
                    from_vec->getData()[iterator.getRow()], decimal_type->getScale(), result);
        FOR_BASIC_NUMERIC_TYPES(DISPATCH)
        #undef DISPATCH

            if (converted)
                assert_cast<ColumnDecimal<DecimalType> &>(dest).insert(result);

            return converted;
        }

    private:
        DataTypePtr data_type;
    };

    template <typename T>
    using DecimalNode = std::conditional_t<IsObjectIterator<Iterator>, DecimalNodeObject<T>, DecimalNodeString<T>>;

    class StringNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            return JSONExtractStringImpl<Iterator>::insertResultToColumn(dest, iterator);
        }
    };

    class FixedStringNodeString : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto & element = iterator.getElement();
            if (!element.isString())
                return false;

            auto & col_str = assert_cast<ColumnFixedString &>(dest);
            auto str = element.getString();
            if (str.size() > col_str.getN())
                return false;

            col_str.insertData(str.data(), str.size());
            return true;
        }
    };

    class FixedStringNodeObject : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            if (!isString(iterator.getType()))
                return false;

            auto & to_string = assert_cast<ColumnFixedString &>(dest);
            const auto & from_string = assert_cast<const ColumnString &>(*iterator.getColumn());
            auto str = from_string.getDataAt(iterator.getRow());
            if (str.size > to_string.getN())
                return false;

            to_string.insertData(str.data, str.size);
            return true;
        }
    };

    using FixedStringNode = std::conditional_t<IsObjectIterator<Iterator>, FixedStringNodeObject, FixedStringNodeString>;

    template <typename Type>
    class EnumNodeBase : public Node
    {
    protected:
        explicit EnumNodeBase(const std::vector<std::pair<String, Type>> & name_value_pairs_)
            : name_value_pairs(name_value_pairs_)
        {
            for (const auto & name_value_pair : name_value_pairs)
            {
                name_to_value_map.emplace(name_value_pair.first, name_value_pair.second);
                only_values.emplace(name_value_pair.second);
            }
        }

        std::vector<std::pair<String, Type>> name_value_pairs;
        std::unordered_map<std::string_view, Type> name_to_value_map;
        std::unordered_set<Type> only_values;
    };

    template <typename Type>
    class EnumNodeString : public EnumNodeBase<Type>
    {
    public:
        explicit EnumNodeString(const std::vector<std::pair<String, Type>> & name_value_pairs_)
            : EnumNodeBase<Type>(name_value_pairs_)
        {
        }

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto & element = iterator.getElement();
            auto & col_vec = assert_cast<ColumnVector<Type> &>(dest);

            if (element.isInt64())
            {
                Type value;
                if (!accurate::convertNumeric(element.getInt64(), value) || !this->only_values.contains(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (element.isUInt64())
            {
                Type value;
                if (!accurate::convertNumeric(element.getUInt64(), value) || !this->only_values.contains(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (element.isString())
            {
                auto value = this->name_to_value_map.find(element.getString());
                if (value == this->name_to_value_map.end())
                    return false;
                col_vec.insertValue(value->second);
                return true;
            }

            return false;
        }
    };

    template <typename Type>
    class EnumNodeObject : public EnumNodeBase<Type>
    {
    public:
        explicit EnumNodeObject(const std::vector<std::pair<String, Type>> & name_value_pairs_)
            : EnumNodeBase<Type>(name_value_pairs_)
        {
        }

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            auto & to_vec = assert_cast<ColumnVector<Type> &>(dest);
            const auto & from = iterator.getColumn();
            auto row = iterator.getRow();

            if (const auto * from_string = typeid_cast<const ColumnString *>(from.get()))
            {
                auto data_ref = from_string->getDataAt(row);
                auto it = this->name_to_value_map.find(data_ref.toView());
                if (it == this->name_to_value_map.end())
                    return false;

                to_vec.insertValue(it->second);
                return true;
            }

            bool converted = false;
            Type value{};

        #define DISPATCH(TYPE) \
            if (const auto * from_vec = typeid_cast<const ColumnVector<TYPE> *>(from.get())) \
                converted = accurate::convertNumeric(from_vec->getData()[row], value);
        FOR_BASIC_NUMERIC_TYPES(DISPATCH)
        #undef DISPATCH

            if (converted && this->only_values.contains(value))
            {
                to_vec.insertValue(value);
                return true;
            }

            return false;
        }
    };

    template <typename T>
    using EnumNode = std::conditional_t<IsObjectIterator<Iterator>, EnumNodeObject<T>, EnumNodeString<T>>;

    class NullableNode : public Node
    {
    public:
        explicit NullableNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            auto & col_null = assert_cast<ColumnNullable &>(dest);
            if (!nested->insertResultToColumn(col_null.getNestedColumn(), iterator))
                return false;

            col_null.getNullMapColumn().insertValue(0);
            return true;
        }

    private:
        std::unique_ptr<Node> nested;
    };

    class ArrayNodeString : public Node
    {
    public:
        explicit ArrayNodeString(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto & element = iterator.getElement();
            if (!element.isArray())
                return false;

            auto array = element.getArray();

            auto & col_arr = assert_cast<ColumnArray &>(dest);
            auto & data = col_arr.getData();
            size_t old_size = data.size();
            bool were_valid_elements = false;

            for (auto value : array)
            {
                if (nested->insertResultToColumn(data, Iterator{value}))
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

    class ArrayNodeObject : public Node
    {
    public:
        explicit ArrayNodeObject(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            const auto * from_array = typeid_cast<const ColumnArray *>(iterator.getColumn().get());
            if (!from_array)
                return false;

            auto & to_arr = assert_cast<ColumnArray &>(dest);
            auto & to_data = to_arr.getData();

            auto row = iterator.getRow();
            const auto & from_array_type = assert_cast<const DataTypeArray &>(*iterator.getType());
            const auto & from_offsets = from_array->getOffsets();

            size_t old_size = to_data.size();
            bool were_valid_elements = false;
            Iterator nested_iter(from_array_type.getNestedType(), from_array->getDataPtr(), 0);

            for (size_t i = from_offsets[row - 1]; i < from_offsets[row]; ++i)
            {
                nested_iter.setRow(i);
                if (nested->insertResultToColumn(to_data, nested_iter))
                    were_valid_elements = true;
                else
                    to_data.insertDefault();
            }

            if (!were_valid_elements)
            {
                to_data.popBack(to_data.size() - old_size);
                return false;
            }

            to_arr.getOffsets().push_back(to_data.size());
            return true;
        }

    private:
        std::unique_ptr<Node> nested;
    };

    using ArrayNode = std::conditional_t<IsObjectIterator<Iterator>, ArrayNodeObject, ArrayNodeString>;

    class TupleNodeBase : public Node
    {
    protected:
        TupleNodeBase(std::vector<std::unique_ptr<Node>> nested_, const std::vector<String> & explicit_names_)
            : nested(std::move(nested_)), explicit_names(explicit_names_)
        {
            for (size_t i = 0; i != explicit_names.size(); ++i)
                name_to_index_map.emplace(explicit_names[i], i);
        }

        std::vector<std::unique_ptr<Node>> nested;
        std::vector<String> explicit_names;
        std::unordered_map<std::string_view, size_t> name_to_index_map;
    };

    class TupleNodeString : public TupleNodeBase
    {
    public:
        TupleNodeString(std::vector<std::unique_ptr<Node>> nested_, const std::vector<String> & explicit_names_)
            : TupleNodeBase(std::move(nested_), explicit_names_)
        {
        }

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            auto & tuple = assert_cast<ColumnTuple &>(dest);
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

            const auto & element = iterator.getElement();

            if (element.isArray())
            {
                auto array = element.getArray();
                auto it = array.begin();

                for (size_t index = 0; (index != this->nested.size()) && (it != array.end()); ++index)
                {
                    if (this->nested[index]->insertResultToColumn(tuple.getColumn(index), Iterator{*it++}))
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
                if (this->name_to_index_map.empty())
                {
                    auto it = object.begin();
                    for (size_t index = 0; (index != this->nested.size()) && (it != object.end()); ++index)
                    {
                        if (this->nested[index]->insertResultToColumn(tuple.getColumn(index), Iterator{(*it++).second}))
                            were_valid_elements = true;
                        else
                            tuple.getColumn(index).insertDefault();
                    }
                }
                else
                {
                    for (const auto & [key, value] : object)
                    {
                        auto index = this->name_to_index_map.find(key);
                        if (index != this->name_to_index_map.end())
                        {
                            if (this->nested[index->second]->insertResultToColumn(tuple.getColumn(index->second), Iterator{value}))
                                were_valid_elements = true;
                        }
                    }
                }

                set_size(old_size + static_cast<size_t>(were_valid_elements));
                return were_valid_elements;
            }

            return false;
        }
    };

    class TupleNodeObject : public TupleNodeBase
    {
    public:
        TupleNodeObject(std::vector<std::unique_ptr<Node>> nested_, const std::vector<String> & explicit_names_)
            : TupleNodeBase(std::move(nested_), explicit_names_)
        {
        }

        bool insertResultToColumn(IColumn & dest, const Iterator & iterator) override
        {
            auto & to_tuple = assert_cast<ColumnTuple &>(dest);
            size_t old_size = dest.size();

            const auto * from_tuple_type = typeid_cast<const DataTypeTuple *>(iterator.getType().get());
            if (!from_tuple_type)
                return false;

            const auto & from_tuple = assert_cast<const ColumnTuple &>(*iterator.getColumn());
            const auto & from_elements = from_tuple_type->getElements();

            if (this->name_to_index_map.empty())
            {
                size_t i = 0;
                for (; i < std::min(from_elements.size(), this->nested.size()); ++i)
                {
                    Iterator elem_iter(from_elements[i], from_tuple.getColumnPtr(i), iterator.getRow());
                    if (!this->nested[i]->insertResultToColumn(to_tuple.getColumn(i), elem_iter))
                        to_tuple.getColumn(i).insertDefault();
                }

                for (; i < this->nested.size(); ++i)
                    to_tuple.getColumn(i).insertDefault();
            }
            else
            {
                const auto & from_names = from_tuple_type->getElementNames();

                for (size_t i = 0 ; i < from_names.size(); ++i)
                {
                    auto it = this->name_to_index_map.find(from_names[i]);
                    if (it != this->name_to_index_map.end())
                    {
                        Iterator elem_iter(from_elements[i], from_tuple.getColumnPtr(i), iterator.getRow());
                        this->nested[it->second]->insertResultToColumn(to_tuple.getColumn(it->second), elem_iter);
                    }
                }

                for (size_t i = 0; i < this->nested.size(); ++i)
                {
                    auto & element = to_tuple.getColumn(i);
                    if (element.size() == old_size)
                        element.insertDefault();
                }
            }

            return true;
        }
    };

    using TupleNode = std::conditional_t<IsObjectIterator<Iterator>, TupleNodeObject, TupleNodeString>;

    static std::unique_ptr<Node> build(const char * function_name, const DataTypePtr & type)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::UInt8: return std::make_unique<NumericNode<UInt8>>();
            case TypeIndex::UInt16: return std::make_unique<NumericNode<UInt16>>();
            case TypeIndex::UInt32: return std::make_unique<NumericNode<UInt32>>();
            case TypeIndex::UInt64: return std::make_unique<NumericNode<UInt64>>();
            case TypeIndex::Int8: return std::make_unique<NumericNode<Int8>>();
            case TypeIndex::Int16: return std::make_unique<NumericNode<Int16>>();
            case TypeIndex::Int32: return std::make_unique<NumericNode<Int32>>();
            case TypeIndex::Int64: return std::make_unique<NumericNode<Int64>>();
            case TypeIndex::Float32: return std::make_unique<NumericNode<Float32>>();
            case TypeIndex::Float64: return std::make_unique<NumericNode<Float64>>();
            case TypeIndex::String: return std::make_unique<StringNode>();
            case TypeIndex::FixedString: return std::make_unique<FixedStringNode>();
            case TypeIndex::UUID: return std::make_unique<UUIDNode>();
            case TypeIndex::LowCardinality:
            {
                auto dictionary_type = typeid_cast<const DataTypeLowCardinality *>(type.get())->getDictionaryType();
                auto impl = build(function_name, dictionary_type);
                return std::make_unique<LowCardinalityNode>(dictionary_type, std::move(impl));
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
            default:
                throw Exception{"Function " + String(function_name) + " doesn't support the return type schema: " + type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
    }
};

template <typename Iterator>
class JSONExtractImpl
{
public:
    static DataTypePtr getReturnType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least two arguments", function_name);

        const auto & col = arguments.back();
        const auto * col_type_const = typeid_cast<const ColumnConst *>(col.column.get());
        if (!col_type_const || !isString(col.type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The last argument of function {} should be a constant string specifying the return data type, illegal value: {}",
                function_name, col.name);

        return DataTypeFactory::instance().get(col_type_const->getValue<String>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 2; }

    void prepare(const char * function_name, const DataTypePtr & result_type)
    {
        extract_tree = JSONExtractTree<Iterator>::build(function_name, result_type);
    }

    bool insertResultToColumn(IColumn & dest, const Iterator & iterator)
    {
        return extract_tree->insertResultToColumn(dest, iterator);
    }

protected:
    std::unique_ptr<typename JSONExtractTree<Iterator>::Node> extract_tree;
};

template <typename Iterator>
class JSONExtractKeysAndValuesImpl
{
public:
    static DataTypePtr getReturnType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least two arguments", function_name);

        const auto & col = arguments.back();
        const auto * col_type_const = typeid_cast<const ColumnConst *>(col.column.get());
        if (!col_type_const || !isString(col.type))
            throw Exception{ErrorCodes::ILLEGAL_COLUMN,
                "The last argument of function {} should be a constant string specifying the values' data type, illegal value: {}",
                function_name, col.name};

        DataTypePtr key_type = std::make_unique<DataTypeString>();
        DataTypePtr value_type = DataTypeFactory::instance().get(col_type_const->getValue<String>());
        DataTypePtr tuple_type = std::make_unique<DataTypeTuple>(DataTypes{key_type, value_type});
        return std::make_unique<DataTypeArray>(tuple_type);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 2; }

    void prepare(const char * function_name, const DataTypePtr & result_type)
    {
        const auto tuple_type = typeid_cast<const DataTypeArray *>(result_type.get())->getNestedType();
        const auto value_type = typeid_cast<const DataTypeTuple *>(tuple_type.get())->getElements()[1];
        extract_tree = JSONExtractTree<Iterator>::build(function_name, value_type);
    }

    bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
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
            Iterator elem_iter(value);
            if (extract_tree->insertResultToColumn(col_value, elem_iter))
                col_key.insertData(key.data(), key.size());
        }

        if (col_tuple.size() == old_size)
            return false;

        col_arr.getOffsets().push_back(col_tuple.size());
        return true;
    }

    bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto * from_tuple_type = typeid_cast<const DataTypeTuple *>(iterator.getType().get());
        if (!from_tuple_type || !from_tuple_type->haveExplicitNames())
            return false;

        auto & to_array = assert_cast<ColumnArray &>(dest);
        auto & to_tuple = assert_cast<ColumnTuple &>(to_array.getData());

        size_t old_size = to_tuple.size();
        auto & to_key = assert_cast<ColumnString &>(to_tuple.getColumn(0));
        auto & to_value = to_tuple.getColumn(1);

        const auto & from_tuple = assert_cast<const ColumnTuple &>(*iterator.getColumn());
        const auto & from_names = from_tuple_type->getElementNames();
        const auto & from_elements = from_tuple_type->getElements();

        for (size_t i = 0; i < from_names.size(); ++i)
        {
            Iterator elem_iter(from_elements[i], from_tuple.getColumnPtr(i), iterator.getRow());
            if (extract_tree->insertResultToColumn(to_value, elem_iter))
                to_key.insertData(from_names[i].data(), from_names[i].size());
        }

        if (to_tuple.size() == old_size)
            return false;

        to_array.getOffsets().push_back(to_tuple.size());
        return true;
    }

private:
    std::unique_ptr<typename JSONExtractTree<Iterator>::Node> extract_tree;
};

template <typename Iterator>
class JSONExtractArrayRawImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        if (!element.isArray())
            return false;

        auto array = element.getArray();
        auto & col_res = assert_cast<ColumnArray &>(dest);

        for (auto && value : array)
        {
            Iterator elem_iter(std::move(value));
            JSONExtractRawImpl<Iterator>::insertResultToColumn(col_res.getData(), elem_iter);
        }

        col_res.getOffsets().push_back(col_res.getOffsets().back() + array.size());
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto * type_array = typeid_cast<const DataTypeArray *>(iterator.getType().get());
        if (!type_array)
            return false;

        auto row = iterator.getRow();
        const auto & from_array = assert_cast<const ColumnArray &>(*iterator.getColumn());
        const auto & offsets = from_array.getOffsets();

        ObjectIterator nested_iter(type_array->getNestedType(), from_array.getDataPtr(), 0);

        auto & to_array = assert_cast<ColumnArray &>(dest);
        for (size_t i = offsets[row - 1]; i < offsets[row]; ++i)
        {
            nested_iter.setRow(i);
            JSONExtractRawImpl<Iterator>::insertResultToColumn(to_array.getData(), nested_iter);
        }

        UInt64 array_size = offsets[row] - offsets[row - 1];
        to_array.getOffsets().push_back(to_array.getOffsets().back() + array_size);
        return true;
    }
};

template <typename Iterator>
class JSONExtractKeysAndValuesRawImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        DataTypePtr string_type = std::make_unique<DataTypeString>();
        DataTypePtr tuple_type = std::make_unique<DataTypeTuple>(DataTypes{string_type, string_type});
        return std::make_unique<DataTypeArray>(tuple_type);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
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
            Iterator value_iter(value);
            JSONExtractRawImpl<Iterator>::insertResultToColumn(col_value, value_iter);
        }

        col_arr.getOffsets().push_back(col_arr.getOffsets().back() + object.size());
        return true;
    }

    static bool insertResultToColumn(IColumn & dest, ObjectIterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(iterator.getType().get());
        if (!type_tuple || !type_tuple->haveExplicitNames())
            return false;

        auto row = iterator.getRow();
        const auto & from_tuple = assert_cast<const ColumnTuple &>(*iterator.getColumn());

        auto & to_array = assert_cast<ColumnArray &>(dest);
        auto & to_tuple = assert_cast<ColumnTuple &>(to_array.getData());
        auto & to_key = assert_cast<ColumnString &>(to_tuple.getColumn(0));
        auto & to_value = assert_cast<ColumnString &>(to_tuple.getColumn(1));

        const auto & element_names = type_tuple->getElementNames();
        const auto & element_types = type_tuple->getElements();

        for (size_t i = 0; i < element_names.size(); ++i)
        {
            ObjectIterator elem_iter(element_types[i], from_tuple.getColumnPtr(i), row);

            to_key.insertData(element_names[i].data(), element_names[i].size());
            JSONExtractRawImpl<Iterator>::insertResultToColumn(to_value, elem_iter);
        }

        to_array.getOffsets().push_back(to_array.getOffsets().back() + element_names.size());
        return true;
    }
};

template <typename Iterator>
class JSONExtractKeysImpl
{
public:
    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_unique<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Iterator & iterator) requires IsElementIterator<Iterator>
    {
        const auto & element = iterator.getElement();
        if (!element.isObject())
            return false;

        auto object = element.getObject();

        auto & col_res = assert_cast<ColumnArray &>(dest);
        auto & col_key = assert_cast<ColumnString &>(col_res.getData());

        for (const auto & [key, value] : object)
            col_key.insertData(key.data(), key.size());

        col_res.getOffsets().push_back(col_res.getOffsets().back() + object.size());
        return true;
    }

    bool insertResultToColumn(IColumn & dest, const ObjectIterator & iterator) requires IsObjectIterator<Iterator>
    {
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(iterator.getType().get());
        if (!type_tuple || !type_tuple->haveExplicitNames())
            return false;

        auto & to_array = assert_cast<ColumnArray &>(dest);
        auto & to_string = assert_cast<ColumnString &>(to_array.getData());

        const auto & tuple_names = type_tuple->getElementNames();
        for (const auto & name : tuple_names)
            to_string.insertData(name.data(), name.size());

        auto & to_offsets = to_array.getOffsets();
        to_offsets.push_back(to_offsets.back() + tuple_names.size());
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
