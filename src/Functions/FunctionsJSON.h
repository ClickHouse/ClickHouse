#pragma once

#include <Functions/IFunction.h>
#include <Core/AccurateComparison.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/SimdJSONParser.h>
#include <Functions/RapidJSONParser.h>
#include <Common/CpuId.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
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
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <common/range.h>
#include <type_traits>
#include <boost/tti/has_member_function.hpp>

#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
    class Executor
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
                std::string_view json{reinterpret_cast<const char *>(&chars[0]), offsets[0] - 1};
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

    template <class T, class = void>
    struct has_index_operator : std::false_type {};

    template <class T>
    struct has_index_operator<T, std::void_t<decltype(std::declval<T>()[0])>> : std::true_type {};

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
        Move(MoveType type_, size_t index_ = 0) : type(type_), index(index_) {}
        Move(MoveType type_, const String & key_) : type(type_), key(key_) {}
        MoveType type;
        size_t index = 0;
        String key;
    };

    static std::vector<Move> prepareMoves(const char * function_name, const ColumnsWithTypeAndName & columns, size_t first_index_argument, size_t num_index_arguments);

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
                    if (!moveToElementByIndex<JSONParser>(res_element, moves[j].index, key))
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
                    if (!moveToElementByIndex<JSONParser>(res_element, index, key))
                        return false;
                    break;
                }
                case MoveType::Key:
                {
                    key = std::string_view{(*arguments[j + 1].column).getDataAt(row)};
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

        if constexpr (has_index_operator<typename JSONParser::Object>::value)
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
    static bool moveToElementByKey(typename JSONParser::Element & element, const std::string_view & key)
    {
        if (!element.isObject())
            return false;
        auto object = element.getObject();
        return object.find(key, element);
    }

    static size_t calculateMaxSize(const ColumnString::Offsets & offsets);
};


template <typename Name, template<typename> typename Impl>
class FunctionJSON : public IFunction, WithContext
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionJSON>(context_); }
    FunctionJSON(ContextPtr context_) : WithContext(context_) {}

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return Impl<DummyJSONParser>::getReturnType(Name::name, arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Choose JSONParser.
#if USE_SIMDJSON
        if (getContext()->getSettingsRef().allow_simdjson)
            return FunctionJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count);
#endif

#if USE_RAPIDJSON
        return FunctionJSONHelpers::Executor<Name, Impl, RapidJSONParser>::run(arguments, result_type, input_rows_count);
#else
        return FunctionJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count);
#endif
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


template <typename JSONParser>
class JSONHasImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element &, const std::string_view &)
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
            throw Exception{"Function " + String(function_name) + " needs exactly one argument",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
        }
        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName &) { return 0; }

    static bool insertResultToColumn(IColumn & dest, const Element &, const std::string_view &)
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
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

    static bool insertResultToColumn(IColumn & dest, const Element &, const std::string_view & last_key)
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
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
                value = element.getDouble();
            }
            else if (!accurate::convertNumeric(element.getDouble(), value))
                return false;
        }
        else if (element.isBool() && is_integer_v<NumberType> && convert_bool_to_integer)
        {
            value = static_cast<NumberType>(element.getBool());
        }
        else
            return false;

        auto & col_vec = assert_cast<ColumnVector<NumberType> &>(dest);
        col_vec.insertValue(value);
        return true;
    }
};


template <typename JSONParser>
using JSONExtractInt8Impl = JSONExtractNumericImpl<JSONParser, Int8>;
template <typename JSONParser>
using JSONExtractUInt8Impl = JSONExtractNumericImpl<JSONParser, UInt8>;
template <typename JSONParser>
using JSONExtractInt16Impl = JSONExtractNumericImpl<JSONParser, Int16>;
template <typename JSONParser>
using JSONExtractUInt16Impl = JSONExtractNumericImpl<JSONParser, UInt16>;
template <typename JSONParser>
using JSONExtractInt32Impl = JSONExtractNumericImpl<JSONParser, Int32>;
template <typename JSONParser>
using JSONExtractUInt32Impl = JSONExtractNumericImpl<JSONParser, UInt32>;
template <typename JSONParser>
using JSONExtractInt64Impl = JSONExtractNumericImpl<JSONParser, Int64>;
template <typename JSONParser>
using JSONExtractUInt64Impl = JSONExtractNumericImpl<JSONParser, UInt64>;
template <typename JSONParser>
using JSONExtractFloat32Impl = JSONExtractNumericImpl<JSONParser, Float32>;
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
        if (!element.isBool())
            return false;

        auto & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(static_cast<UInt8>(element.getBool()));
        return true;
    }
};


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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
        if (!element.isString())
            return false;

        auto str = element.getString();
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(str.data(), str.size());
        return true;
    }
};

template <typename JSONParser>
class JSONExtractRawImpl;

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

    class LowCardinalityNode : public Node
    {
    public:
        LowCardinalityNode(DataTypePtr dictionary_type_, std::unique_ptr<Node> impl_)
            : dictionary_type(dictionary_type_), impl(std::move(impl_)) {}
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            auto from_col = dictionary_type->createColumn();
            if (impl->insertResultToColumn(*from_col, element))
            {
                StringRef value = from_col->getDataAt(0);
                assert_cast<ColumnLowCardinality &>(dest).insertData(value.data, value.size);
                return true;
            }
            return false;
        }
    private:
        DataTypePtr dictionary_type;
        std::unique_ptr<Node> impl;
    };

    class UUIDNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (!element.isString())
                return false;

            auto uuid = parseFromString<UUID>(element.getString());
            assert_cast<ColumnUUID &>(dest).insert(uuid);
            return true;
        }
    };

    template <typename DecimalType>
    class DecimalNode : public Node
    {
    public:
        DecimalNode(DataTypePtr data_type_) : data_type(data_type_) {}
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
            if (!element.isDouble())
                return false;

            const auto * type = assert_cast<const DataTypeDecimal<DecimalType> *>(data_type.get());
            auto result = convertToDecimal<DataTypeNumber<Float64>, DataTypeDecimal<DecimalType>>(element.getDouble(), type->getScale());
            assert_cast<ColumnDecimal<DecimalType> &>(dest).insert(result);
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
            if (element.isString())
                return JSONExtractStringImpl<JSONParser>::insertResultToColumn(dest, element, {});
            else if (element.isNull())
                return false;
            else
                return JSONExtractRawImpl<JSONParser>::insertResultToColumn(dest, element, {});
        }
    };

    class FixedStringNode : public Node
    {
    public:
        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
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

    template <typename Type>
    class EnumNode : public Node
    {
    public:
        EnumNode(const std::vector<std::pair<String, Type>> & name_value_pairs_) : name_value_pairs(name_value_pairs_)
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
                if (!accurate::convertNumeric(element.getInt64(), value) || !only_values.count(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (element.isUInt64())
            {
                Type value;
                if (!accurate::convertNumeric(element.getUInt64(), value) || !only_values.count(value))
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
        NullableNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool insertResultToColumn(IColumn & dest, const Element & element) override
        {
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
        ArrayNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

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
                    for (auto [key, value] : object)
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
                for (const auto & tuple_element : tuple_elements)
                    elements.emplace_back(build(function_name, tuple_element));
                return std::make_unique<TupleNode>(std::move(elements), tuple.haveExplicitNames() ? tuple.getElementNames() : Strings{});
            }
            default:
                throw Exception{"Function " + String(function_name) + " doesn't support the return type schema: " + type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
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
            throw Exception{"Function " + String(function_name) + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto & col = arguments.back();
        auto col_type_const = typeid_cast<const ColumnConst *>(col.column.get());
        if (!col_type_const || !isString(col.type))
            throw Exception{"The last argument of function " + String(function_name)
                                + " should be a constant string specifying the return data type, illegal value: " + col.name,
                            ErrorCodes::ILLEGAL_COLUMN};

        return DataTypeFactory::instance().get(col_type_const->getValue<String>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 2; }

    void prepare(const char * function_name, const ColumnsWithTypeAndName &, const DataTypePtr & result_type)
    {
        extract_tree = JSONExtractTree<JSONParser>::build(function_name, result_type);
    }

    bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
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
            throw Exception{"Function " + String(function_name) + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto & col = arguments.back();
        auto col_type_const = typeid_cast<const ColumnConst *>(col.column.get());
        if (!col_type_const || !isString(col.type))
            throw Exception{"The last argument of function " + String(function_name)
                                + " should be a constant string specifying the values' data type, illegal value: " + col.name,
                            ErrorCodes::ILLEGAL_COLUMN};

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

    bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
        if (!element.isObject())
            return false;

        auto object = element.getObject();

        auto & col_arr = assert_cast<ColumnArray &>(dest);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        size_t old_size = col_tuple.size();
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_value = col_tuple.getColumn(1);

        for (auto [key, value] : object)
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        auto & chars = col_str.getChars();
        WriteBufferFromVector<ColumnString::Chars> buf(chars, WriteBufferFromVector<ColumnString::Chars>::AppendModeTag());
        traverse(element, buf);
        buf.finalize();
        chars.push_back(0);
        col_str.getOffsets().push_back(chars.size());
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
            writeJSONString(element.getString(), buf, format_settings());
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
                writeJSONString(key, buf, format_settings());
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

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
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

    bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
        if (!element.isObject())
            return false;

        auto object = element.getObject();

        auto & col_arr = assert_cast<ColumnArray &>(dest);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_value = assert_cast<ColumnString &>(col_tuple.getColumn(1));

        for (auto [key, value] : object)
        {
            col_key.insertData(key.data(), key.size());
            JSONExtractRawImpl<JSONParser>::insertResultToColumn(col_value, value, {});
        }

        col_arr.getOffsets().push_back(col_arr.getOffsets().back() + object.size());
        return true;
    }
};

}
