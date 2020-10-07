#pragma once

#include <Functions/IFunctionImpl.h>
#include <Core/AccurateComparison.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/SimdJSONParser.h>
#include <Functions/RapidJSONParser.h>
#include <Common/CpuId.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <ext/range.h>

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
template <typename Name, template<typename> typename Impl>
class FunctionJSON : public IFunction
{
public:
    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionJSON>(context_); }
    FunctionJSON(const Context & context_) : context(context_) {}

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return Impl<DummyJSONParser>::getType(Name::name, arguments);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_pos, size_t input_rows_count) override
    {
        /// Choose JSONParser.
#if USE_SIMDJSON
        if (context.getSettingsRef().allow_simdjson && Cpu::CpuFlagsCache::have_SSE42 && Cpu::CpuFlagsCache::have_PCLMUL)
        {
            Executor<SimdJSONParser>::run(block, arguments, result_pos, input_rows_count);
            return;
        }
#endif
#if USE_RAPIDJSON
        Executor<RapidJSONParser>::run(block, arguments, result_pos, input_rows_count);
#else
        Executor<DummyJSONParser>::run(block, arguments, result_pos, input_rows_count);
#endif
    }

private:
    const Context & context;

    template <typename JSONParser>
    class Executor
    {
    public:
        static void run(Block & block, const ColumnNumbers & arguments, size_t result_pos, size_t input_rows_count)
        {
            MutableColumnPtr to{block.getByPosition(result_pos).type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.size() < 1)
                throw Exception{"Function " + String(Name::name) + " requires at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

            const auto & first_column = block.getByPosition(arguments[0]);
            if (!isString(first_column.type))
                throw Exception{"The first argument of function " + String(Name::name) + " should be a string containing JSON, illegal type: " + first_column.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            const ColumnPtr & arg_json = first_column.column;
            auto col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            auto col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_string)
                throw Exception{"Illegal column " + arg_json->getName(), ErrorCodes::ILLEGAL_COLUMN};

            const ColumnString::Chars & chars = col_json_string->getChars();
            const ColumnString::Offsets & offsets = col_json_string->getOffsets();

            std::vector<Move> moves = prepareListOfMoves(block, arguments);

            /// Preallocate memory in parser if necessary.
            JSONParser parser;
            if (parser.need_preallocate)
                parser.preallocate(calculateMaxSize(offsets));

            Impl<JSONParser> impl;

            /// prepare() does Impl-specific preparation before handling each row.
            impl.prepare(Name::name, block, arguments, result_pos);

            bool json_parsed_ok = false;
            if (col_json_const)
            {
                StringRef json{reinterpret_cast<const char *>(&chars[0]), offsets[0] - 1};
                json_parsed_ok = parser.parse(json);
            }

            for (const auto i : ext::range(0, input_rows_count))
            {
                if (!col_json_const)
                {
                    StringRef json{reinterpret_cast<const char *>(&chars[offsets[i - 1]]), offsets[i] - offsets[i - 1] - 1};
                    json_parsed_ok = parser.parse(json);
                }

                bool ok = json_parsed_ok;
                if (ok)
                {
                    auto it = parser.getRoot();

                    /// Perform moves.
                    for (size_t j = 0; (j != moves.size()) && ok; ++j)
                    {
                        switch (moves[j].type)
                        {
                            case MoveType::ConstIndex:
                                ok = moveIteratorToElementByIndex(it, moves[j].index);
                                break;
                            case MoveType::ConstKey:
                                ok = moveIteratorToElementByKey(it, moves[j].key);
                                break;
                            case MoveType::Index:
                            {
                                const Field field = (*block.getByPosition(arguments[j + 1]).column)[i];
                                ok = moveIteratorToElementByIndex(it, field.get<Int64>());
                                break;
                            }
                            case MoveType::Key:
                            {
                                const Field field = (*block.getByPosition(arguments[j + 1]).column)[i];
                                ok = moveIteratorToElementByKey(it, field.get<String>().data());
                                break;
                            }
                        }
                    }

                    if (ok)
                        ok = impl.addValueToColumn(*to, it);
                }

                /// We add default value (=null or zero) if something goes wrong, we don't throw exceptions in these JSON functions.
                if (!ok)
                    to->insertDefault();
            }
            block.getByPosition(result_pos).column = std::move(to);
        }

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
            Move(MoveType type_, size_t index_ = 0) : type(type_), index(index_) {}
            Move(MoveType type_, const String & key_) : type(type_), key(key_) {}
            MoveType type;
            size_t index = 0;
            String key;
        };

        static std::vector<Move> prepareListOfMoves(Block & block, const ColumnNumbers & arguments)
        {
            constexpr size_t num_extra_arguments = Impl<JSONParser>::num_extra_arguments;
            const size_t num_moves = arguments.size() - num_extra_arguments - 1;
            std::vector<Move> moves;
            moves.reserve(num_moves);
            for (const auto i : ext::range(0, num_moves))
            {
                const auto & column = block.getByPosition(arguments[i + 1]);
                if (!isString(column.type) && !isInteger(column.type))
                    throw Exception{"The argument " + std::to_string(i + 2) + " of function " + String(Name::name)
                                        + " should be a string specifying key or an integer specifying index, illegal type: " + column.type->getName(),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

                if (isColumnConst(*column.column))
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

        using Iterator = typename JSONParser::Iterator;

        /// Performs moves of types MoveType::Index and MoveType::ConstIndex.
        static bool moveIteratorToElementByIndex(Iterator & it, int index)
        {
            if (JSONParser::isArray(it))
            {
                if (index > 0)
                    return JSONParser::arrayElementByIndex(it, index - 1);
                else
                    return JSONParser::arrayElementByIndex(it, JSONParser::sizeOfArray(it) + index);
            }
            if (JSONParser::isObject(it))
            {
                if (index > 0)
                    return JSONParser::objectMemberByIndex(it, index - 1);
                else
                    return JSONParser::objectMemberByIndex(it, JSONParser::sizeOfObject(it) + index);
            }
            return false;
        }

        /// Performs moves of types MoveType::Key and MoveType::ConstKey.
        static bool moveIteratorToElementByKey(Iterator & it, const String & key)
        {
            if (JSONParser::isObject(it))
                return JSONParser::objectMemberByName(it, key);
            return false;
        }

        static size_t calculateMaxSize(const ColumnString::Offsets & offsets)
        {
            size_t max_size = 0;
            for (const auto i : ext::range(0, offsets.size()))
                if (max_size < offsets[i] - offsets[i - 1])
                    max_size = offsets[i] - offsets[i - 1];

            if (max_size < 1)
                max_size = 1;
            return max_size;
        }
    };
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
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator &)
    {
        ColumnVector<UInt8> & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(1);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser>
class IsValidJSONImpl
{
public:
    static DataTypePtr getType(const char * function_name, const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
        {
            /// IsValidJSON() shouldn't get parameters other than JSON.
            throw Exception{"Function " + String(function_name) + " needs exactly one argument",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
        }
        return std::make_shared<DataTypeUInt8>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator &)
    {
        /// This function is called only if JSON is valid.
        /// If JSON isn't valid then `FunctionJSON::Executor::run()` adds default value (=zero) to `dest` without calling this function.
        ColumnVector<UInt8> & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(1);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser>
class JSONLengthImpl
{
public:
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeUInt64>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        size_t size;
        if (JSONParser::isArray(it))
            size = JSONParser::sizeOfArray(it);
        else if (JSONParser::isObject(it))
            size = JSONParser::sizeOfObject(it);
        else
            return false;

        ColumnVector<UInt64> & col_vec = assert_cast<ColumnVector<UInt64> &>(dest);
        col_vec.insertValue(size);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser>
class JSONKeyImpl
{
public:
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        if (!JSONParser::isObjectMember(it))
            return false;
        StringRef key = JSONParser::getKey(it);
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(key.data, key.size);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser>
class JSONTypeImpl
{
public:
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
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

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        UInt8 type;
        if (JSONParser::isInt64(it))
            type = 'i';
        else if (JSONParser::isUInt64(it))
            type = 'u';
        else if (JSONParser::isDouble(it))
            type = 'd';
        else if (JSONParser::isBool(it))
            type = 'b';
        else if (JSONParser::isString(it))
            type = '"';
        else if (JSONParser::isArray(it))
            type = '[';
        else if (JSONParser::isObject(it))
            type = '{';
        else if (JSONParser::isNull(it))
            type = 0;
        else
            return false;

        ColumnVector<Int8> & col_vec = assert_cast<ColumnVector<Int8> &>(dest);
        col_vec.insertValue(type);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser, typename NumberType, bool convert_bool_to_integer = false>
class JSONExtractNumericImpl
{
public:
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeNumber<NumberType>>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        NumberType value;

        if (JSONParser::isInt64(it))
        {
            if (!accurate::convertNumeric(JSONParser::getInt64(it), value))
                return false;
        }
        else if (JSONParser::isUInt64(it))
        {
            if (!accurate::convertNumeric(JSONParser::getUInt64(it), value))
                return false;
        }
        else if (JSONParser::isDouble(it))
        {
            if (!accurate::convertNumeric(JSONParser::getDouble(it), value))
                return false;
        }
        else if (JSONParser::isBool(it) && is_integral_v<NumberType> && convert_bool_to_integer)
            value = static_cast<NumberType>(JSONParser::getBool(it));
        else
            return false;

        auto & col_vec = assert_cast<ColumnVector<NumberType> &>(dest);
        col_vec.insertValue(value);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
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
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        if (!JSONParser::isBool(it))
            return false;

        auto & col_vec = assert_cast<ColumnVector<UInt8> &>(dest);
        col_vec.insertValue(static_cast<UInt8>(JSONParser::getBool(it)));
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser>
class JSONExtractStringImpl
{
public:
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        if (!JSONParser::isString(it))
            return false;

        StringRef str = JSONParser::getString(it);
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(str.data, str.size);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


/// Nodes of the extract tree. We need the extract tree to extract from JSON complex values containing array, tuples or nullables.
template <typename JSONParser>
struct JSONExtractTree
{
    using Iterator = typename JSONParser::Iterator;

    class Node
    {
    public:
        Node() {}
        virtual ~Node() {}
        virtual bool addValueToColumn(IColumn &, const Iterator &) = 0;
    };

    template <typename NumberType>
    class NumericNode : public Node
    {
    public:
        bool addValueToColumn(IColumn & dest, const Iterator & it) override
        {
            return JSONExtractNumericImpl<JSONParser, NumberType, true>::addValueToColumn(dest, it);
        }
    };

    class StringNode : public Node
    {
    public:
        bool addValueToColumn(IColumn & dest, const Iterator & it) override
        {
            return JSONExtractStringImpl<JSONParser>::addValueToColumn(dest, it);
        }
    };

    class FixedStringNode : public Node
    {
    public:
        bool addValueToColumn(IColumn & dest, const Iterator & it) override
        {
            if (!JSONParser::isString(it))
                return false;
            auto & col_str = assert_cast<ColumnFixedString &>(dest);
            StringRef str = JSONParser::getString(it);
            if (str.size > col_str.getN())
                return false;
            col_str.insertData(str.data, str.size);
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

        bool addValueToColumn(IColumn & dest, const Iterator & it) override
        {
            auto & col_vec = assert_cast<ColumnVector<Type> &>(dest);

            if (JSONParser::isInt64(it))
            {
                Type value;
                if (!accurate::convertNumeric(JSONParser::getInt64(it), value) || !only_values.count(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (JSONParser::isUInt64(it))
            {
                Type value;
                if (!accurate::convertNumeric(JSONParser::getUInt64(it), value) || !only_values.count(value))
                    return false;
                col_vec.insertValue(value);
                return true;
            }

            if (JSONParser::isString(it))
            {
                auto value = name_to_value_map.find(JSONParser::getString(it));
                if (value == name_to_value_map.end())
                    return false;
                col_vec.insertValue(value->second);
                return true;
            }

            return false;
        }

    private:
        std::vector<std::pair<String, Type>> name_value_pairs;
        std::unordered_map<StringRef, Type> name_to_value_map;
        std::unordered_set<Type> only_values;
    };

    class NullableNode : public Node
    {
    public:
        NullableNode(std::unique_ptr<Node> nested_) : nested(std::move(nested_)) {}

        bool addValueToColumn(IColumn & dest, const Iterator & it) override
        {
            ColumnNullable & col_null = assert_cast<ColumnNullable &>(dest);
            if (!nested->addValueToColumn(col_null.getNestedColumn(), it))
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

        bool addValueToColumn(IColumn & dest, const Iterator & it) override
        {
            if (!JSONParser::isArray(it))
                return false;

            Iterator array_it = it;
            if (!JSONParser::firstArrayElement(array_it))
                return false;

            ColumnArray & col_arr = assert_cast<ColumnArray &>(dest);
            auto & data = col_arr.getData();
            size_t old_size = data.size();
            bool were_valid_elements = false;

            do
            {
                if (nested->addValueToColumn(data, array_it))
                    were_valid_elements = true;
                else
                    data.insertDefault();
            }
            while (JSONParser::nextArrayElement(array_it));

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

        bool addValueToColumn(IColumn & dest, const Iterator & it) override
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

            if (JSONParser::isArray(it))
            {
                Iterator array_it = it;
                if (!JSONParser::firstArrayElement(array_it))
                    return false;

                for (size_t index = 0; index != nested.size(); ++index)
                {
                    if (nested[index]->addValueToColumn(tuple.getColumn(index), array_it))
                        were_valid_elements = true;
                    else
                        tuple.getColumn(index).insertDefault();
                    if (!JSONParser::nextArrayElement(array_it))
                        break;
                }

                set_size(old_size + static_cast<size_t>(were_valid_elements));
                return were_valid_elements;
            }

            if (JSONParser::isObject(it))
            {
                if (name_to_index_map.empty())
                {
                    Iterator object_it = it;
                    if (!JSONParser::firstObjectMember(object_it))
                        return false;

                    for (size_t index = 0; index != nested.size(); ++index)
                    {
                        if (nested[index]->addValueToColumn(tuple.getColumn(index), object_it))
                            were_valid_elements = true;
                        else
                            tuple.getColumn(index).insertDefault();
                        if (!JSONParser::nextObjectMember(object_it))
                            break;
                    }
                }
                else
                {
                    Iterator object_it = it;
                    StringRef key;
                    if (!JSONParser::firstObjectMember(object_it, key))
                        return false;

                    do
                    {
                        auto index = name_to_index_map.find(key);
                        if (index != name_to_index_map.end())
                        {
                            if (nested[index->second]->addValueToColumn(tuple.getColumn(index->second), object_it))
                                were_valid_elements = true;
                        }
                    }
                    while (JSONParser::nextObjectMember(object_it, key));
                }

                set_size(old_size + static_cast<size_t>(were_valid_elements));
                return were_valid_elements;
            }

            return false;
        }

    private:
        std::vector<std::unique_ptr<Node>> nested;
        std::vector<String> explicit_names;
        std::unordered_map<StringRef, size_t> name_to_index_map;
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
    static constexpr size_t num_extra_arguments = 1;

    static DataTypePtr getType(const char * function_name, const ColumnsWithTypeAndName & arguments)
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

    void prepare(const char * function_name, const Block & block, const ColumnNumbers &, size_t result_pos)
    {
        extract_tree = JSONExtractTree<JSONParser>::build(function_name, block.getByPosition(result_pos).type);
    }

    using Iterator = typename JSONParser::Iterator;
    bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        return extract_tree->addValueToColumn(dest, it);
    }

protected:
    std::unique_ptr<typename JSONExtractTree<JSONParser>::Node> extract_tree;
};


template <typename JSONParser>
class JSONExtractKeysAndValuesImpl
{
public:
    static constexpr size_t num_extra_arguments = 1;

    static DataTypePtr getType(const char * function_name, const ColumnsWithTypeAndName & arguments)
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

    void prepare(const char * function_name, const Block & block, const ColumnNumbers &, size_t result_pos)
    {
        const auto & result_type = block.getByPosition(result_pos).type;
        const auto tuple_type = typeid_cast<const DataTypeArray *>(result_type.get())->getNestedType();
        const auto value_type = typeid_cast<const DataTypeTuple *>(tuple_type.get())->getElements()[1];
        extract_tree = JSONExtractTree<JSONParser>::build(function_name, value_type);
    }

    using Iterator = typename JSONParser::Iterator;
    bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        if (!JSONParser::isObject(it))
            return false;

        auto & col_arr = assert_cast<ColumnArray &>(dest);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        size_t old_size = col_tuple.size();
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_value = col_tuple.getColumn(1);

        StringRef key;
        Iterator object_it = it;
        if (!JSONParser::firstObjectMember(object_it, key))
            return false;

        do
        {
            if (extract_tree->addValueToColumn(col_value, object_it))
                col_key.insertData(key.data, key.size);
        }
        while (JSONParser::nextObjectMember(object_it, key));

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
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        auto & chars = col_str.getChars();
        WriteBufferFromVector<ColumnString::Chars> buf(chars, WriteBufferFromVector<ColumnString::Chars>::AppendModeTag());
        traverse(it, buf);
        buf.finalize();
        chars.push_back(0);
        col_str.getOffsets().push_back(chars.size());
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}

private:
    static void traverse(const Iterator & it, WriteBuffer & buf)
    {
        if (JSONParser::isInt64(it))
        {
            writeIntText(JSONParser::getInt64(it), buf);
            return;
        }
        if (JSONParser::isUInt64(it))
        {
            writeIntText(JSONParser::getUInt64(it), buf);
            return;
        }
        if (JSONParser::isDouble(it))
        {
            writeFloatText(JSONParser::getDouble(it), buf);
            return;
        }
        if (JSONParser::isBool(it))
        {
            if (JSONParser::getBool(it))
                writeCString("true", buf);
            else
                writeCString("false", buf);
            return;
        }
        if (JSONParser::isString(it))
        {
            writeJSONString(JSONParser::getString(it), buf, format_settings());
            return;
        }
        if (JSONParser::isArray(it))
        {
            writeChar('[', buf);
            Iterator array_it = it;
            if (JSONParser::firstArrayElement(array_it))
            {
                traverse(array_it, buf);
                while (JSONParser::nextArrayElement(array_it))
                {
                    writeChar(',', buf);
                    traverse(array_it, buf);
                }
            }
            writeChar(']', buf);
            return;
        }
        if (JSONParser::isObject(it))
        {
            writeChar('{', buf);
            Iterator object_it = it;
            StringRef key;
            if (JSONParser::firstObjectMember(object_it, key))
            {
                writeJSONString(key, buf, format_settings());
                writeChar(':', buf);
                traverse(object_it, buf);
                while (JSONParser::nextObjectMember(object_it, key))
                {
                    writeChar(',', buf);
                    writeJSONString(key, buf, format_settings());
                    writeChar(':', buf);
                    traverse(object_it, buf);
                }
            }
            writeChar('}', buf);
            return;
        }
        if (JSONParser::isNull(it))
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
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    using Iterator = typename JSONParser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        if (!JSONParser::isArray(it))
            return false;

        ColumnArray & col_res = assert_cast<ColumnArray &>(dest);
        Iterator array_it = it;
        size_t size = 0;
        if (JSONParser::firstArrayElement(array_it))
        {
            do
            {
                JSONExtractRawImpl<JSONParser>::addValueToColumn(col_res.getData(), array_it);
                ++size;
            } while (JSONParser::nextArrayElement(array_it));
        }

        col_res.getOffsets().push_back(col_res.getOffsets().back() + size);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};


template <typename JSONParser>
class JSONExtractKeysAndValuesRawImpl
{
public:

    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        DataTypePtr string_type = std::make_unique<DataTypeString>();
        DataTypePtr tuple_type = std::make_unique<DataTypeTuple>(DataTypes{string_type, string_type});
        return std::make_unique<DataTypeArray>(tuple_type);
    }

    using Iterator = typename JSONParser::Iterator;
    bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        if (!JSONParser::isObject(it))
            return false;

        auto & col_arr = assert_cast<ColumnArray &>(dest);
        auto & col_tuple = assert_cast<ColumnTuple &>(col_arr.getData());
        auto & col_key = assert_cast<ColumnString &>(col_tuple.getColumn(0));
        auto & col_value = assert_cast<ColumnString &>(col_tuple.getColumn(1));

        Iterator object_it = it;
        StringRef key;
        size_t size = 0;
        if (JSONParser::firstObjectMember(object_it, key))
        {
            do
            {
                col_key.insertData(key.data, key.size);
                JSONExtractRawImpl<JSONParser>::addValueToColumn(col_value, object_it);
                ++size;
            } while (JSONParser::nextObjectMember(object_it, key));
        }

        col_arr.getOffsets().push_back(col_arr.getOffsets().back() + size);
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
};

}
