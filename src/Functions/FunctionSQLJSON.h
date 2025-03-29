#pragma once

#include <sstream>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <base/range.h>

#include "config.h"


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_simdjson;
    extern const SettingsBool function_json_value_return_type_allow_complex;
    extern const SettingsBool function_json_value_return_type_allow_nullable;
    extern const SettingsUInt64 function_json_tuple_max_query_number;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
}

/// Have implemented the operator << for json elements. So we could use stringstream to serialize json elements.
/// But stingstream have bad performance, not recommend to use it.
template <typename Element>
class DefaultJSONStringSerializer
{
public:
    explicit DefaultJSONStringSerializer(ColumnString & col_str_) : col_str(col_str_) { }

    void addRawData(const char * ptr, size_t len)
    {
        out << std::string_view(ptr, len);
    }

    void addRawString(std::string_view str)
    {
        out << str;
    }

    /// serialize the json element into stringstream
    void addElement(const Element & element)
    {
        out << element.getElement();
    }
    void commit()
    {
        auto out_str = out.str();
        col_str.insertData(out_str.data(), out_str.size());
    }
    void rollback() {}
private:
    ColumnString & col_str;
    std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
};

/// A more efficient way to serialize json elements into destination column.
/// Formatter takes the chars buffer in the ColumnString and put data into it directly.
template<typename Element, typename Formatter>
class JSONStringSerializer
{
public:
    explicit JSONStringSerializer(ColumnString & col_str_)
        : col_str(col_str_), chars(col_str_.getChars()), offsets(col_str_.getOffsets()), formatter(col_str_.getChars())
    {
        prev_offset = offsets.empty() ? 0 : offsets.back();
    }
    /// Put the data into column's buffer directly.
    void addRawData(const char * ptr, size_t len)
    {
        chars.insert(ptr, ptr + len);
    }

    void addRawString(std::string_view str)
    {
        chars.insert(str.data(), str.data() + str.size());
    }

    /// serialize the json element into column's buffer directly
    void addElement(const Element & element)
    {
        formatter.append(element.getElement());
    }
    void commit()
    {
        chars.push_back(0);
        offsets.push_back(chars.size());
    }
    void rollback()
    {
        chars.resize(prev_offset);
    }
private:
    ColumnString & col_str;
    ColumnString::Chars & chars;
    IColumn::Offsets & offsets;
    Formatter formatter;
    size_t prev_offset;

};

template<typename JSONParser>
struct JSONPathInfo
{
    using GeneratorJSONPathPtr = std::shared_ptr<GeneratorJSONPath<JSONParser>>;
    GeneratorJSONPathPtr json_path;
    bool path_has_asterisk = false;
};

class FunctionSQLJSONHelpers
{
public:
    template <typename Name, typename Impl, class JSONParser>
    class Executor
    {
    public:
        using JSONPaths = std::vector<JSONPathInfo<JSONParser>>;

        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth, uint32_t parse_backtracks, bool function_json_value_return_type_allow_complex)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.size() < 2)
            {
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "JSONPath functions require at least 2 arguments");
            }

            const auto & json_column = arguments[0];

            if (!isString(json_column.type))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "JSONPath functions require first argument to be JSON of string, illegal type: {}",
                                json_column.type->getName());
            }
            JSONPaths json_paths;
            auto parse_json_path = [&](const ColumnWithTypeAndName & json_path_column, const size_t column_index) -> JSONPathInfo<JSONParser>
            {
                if (!isString(json_path_column.type))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "JSONPath functions require {}th argument "
                                    "to be JSONPath of type string, illegal type: {}", column_index+1, json_path_column.type->getName());
                }
                if (!isColumnConst(*json_path_column.column))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}th argument (JSONPath) must be constant string", column_index+1);
                }
                /// Prepare to parse argument (JSONPath)
                String query = typeid_cast<const ColumnConst &>(*json_path_column.column).getValue<String>();
                /// Tokenize the query
                Tokens tokens(query.data(), query.data() + query.size());
                /// Max depth 0 indicates that depth is not limited
                IParser::Pos token_iterator(tokens, parse_depth, parse_backtracks);

                /// Parse query and create AST tree
                Expected expected;
                ASTPtr res;
                ParserJSONPath parser;
                const bool parse_res = parser.parse(token_iterator, res, expected);
                if (!parse_res)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse JSONPath");
                }
                auto json_path = std::make_shared<GeneratorJSONPath<JSONParser>>(res);
                bool path_has_asterisk = query.find("[*]") != std::string::npos;
                return JSONPathInfo<JSONParser>{json_path, path_has_asterisk};
            };
            Impl impl;
            if (impl.hasMultipleJsonPaths())
            {
                for (size_t i = 1; i < arguments.size(); ++i)
                    json_paths.emplace_back(parse_json_path(arguments[i], i));
            }
            else
                json_paths.emplace_back(parse_json_path(arguments[1], 1));

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;
            /// Parse JSON for every row

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view json = json_column.column->getDataAt(i).toView();
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                if (document_ok)
                {
                    /// Instead of creating a new generator for each row, we can reuse the same one.
                    // generator_json_path.reinitialize();
                    added_to_column = impl.insertResultToColumn(*to, document, json_paths, function_json_value_return_type_allow_complex);
                }
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
            return to;
        }
    };
};

template <typename Name, template <typename, typename> typename Impl>
class FunctionSQLJSON : public IFunction
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionSQLJSON>(context_); }
    explicit FunctionSQLJSON(ContextPtr context_)
        : max_parser_depth(context_->getSettingsRef()[Setting::max_parser_depth]),
          max_parser_backtracks(context_->getSettingsRef()[Setting::max_parser_backtracks]),
          allow_simdjson(context_->getSettingsRef()[Setting::allow_simdjson]),
          function_json_value_return_type_allow_complex(context_->getSettingsRef()[Setting::function_json_value_return_type_allow_complex]),
          function_json_value_return_type_allow_nullable(context_->getSettingsRef()[Setting::function_json_value_return_type_allow_nullable]),
          function_json_tuple_max_query_number(context_->getSettingsRef()[Setting::function_json_tuple_max_query_number])
    {
    }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return Impl<DummyJSONParser, DefaultJSONStringSerializer<DummyJSONParser::Element>>::getArgumentsThatAreAlwaysConstant(function_json_tuple_max_query_number);
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return Impl<DummyJSONParser, DefaultJSONStringSerializer<DummyJSONParser::Element>>::getReturnType(
            Name::name, arguments, function_json_value_return_type_allow_nullable, function_json_tuple_max_query_number);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Choose JSONParser.
        /// 1. Lexer(path) -> Tokens
        /// 2. Create ASTPtr
        /// 3. Parser(Tokens, ASTPtr) -> complete AST
        /// 4. Execute functions: call getNextItem on generator and handle each item
        unsigned parse_depth = static_cast<unsigned>(max_parser_depth);
        unsigned parse_backtracks = static_cast<unsigned>(max_parser_backtracks);
#if USE_SIMDJSON
        if (allow_simdjson)
            return FunctionSQLJSONHelpers::Executor<
                Name,
                Impl<SimdJSONParser, JSONStringSerializer<SimdJSONParser::Element, SimdJSONElementFormatter>>,
                SimdJSONParser>::run(arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
#endif
        return FunctionSQLJSONHelpers::
            Executor<Name, Impl<DummyJSONParser, DefaultJSONStringSerializer<DummyJSONParser::Element>>, DummyJSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
    }
private:
    const size_t max_parser_depth;
    const size_t max_parser_backtracks;
    const bool allow_simdjson;
    const bool function_json_value_return_type_allow_complex;
    const bool function_json_value_return_type_allow_nullable;
    const size_t function_json_tuple_max_query_number;
};

struct NameJSONExists
{
    static constexpr auto name{"JSON_EXISTS"};
};

struct NameJSONValue
{
    static constexpr auto name{"JSON_VALUE"};
};

struct NameJSONQuery
{
    static constexpr auto name{"JSON_QUERY"};
};

struct NameJSONTuple
{
    static constexpr auto name{"JSON_TUPLE"};
};

template <typename JSONParser, typename JSONStringSerializer>
class JSONExistsImpl
{
public:
    using Element = typename JSONParser::Element;
    using JSONPaths = std::vector<JSONPathInfo<JSONParser>>;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, bool, size_t) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }
    static ColumnNumbers getArgumentsThatAreAlwaysConstant(const size_t) { return {1}; }
    static bool hasMultipleJsonPaths() { return false; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, JSONPaths & json_paths, bool)
    {
        Element current_element = root;
        VisitorStatus status;
        auto & generator_json_path = json_paths[0].json_path;
        generator_json_path->reinitialize();
        while ((status = generator_json_path->getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                break;
            }
            current_element = root;
        }

        /// insert result, status can be either Ok (if we found the item)
        /// or Exhausted (if we never found the item)
        ColumnUInt8 & col_bool = assert_cast<ColumnUInt8 &>(dest);
        if (status == VisitorStatus::Ok)
        {
            col_bool.insert(1);
        }
        else
        {
            col_bool.insert(0);
        }
        return true;
    }
};

template <typename JSONParser, typename JSONStringSerializer>
class JSONValueImpl
{
public:
    using Element = typename JSONParser::Element;
    using JSONPaths = std::vector<JSONPathInfo<JSONParser>>;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, bool function_json_value_return_type_allow_nullable, size_t)
    {
        if (function_json_value_return_type_allow_nullable)
        {
            DataTypePtr string_type = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeNullable>(string_type);
        }

        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }
    static ColumnNumbers getArgumentsThatAreAlwaysConstant(const size_t) { return {1}; }
    static bool hasMultipleJsonPaths() { return false; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, JSONPaths & json_paths, bool function_json_value_return_type_allow_complex)
    {
        Element current_element = root;
        VisitorStatus status;
        auto & generator_json_path = json_paths[0].json_path;
        generator_json_path->reinitialize();
        while ((status = generator_json_path->getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                if (function_json_value_return_type_allow_complex)
                {
                    break;
                }
                if (!(current_element.isArray() || current_element.isObject()))
                {
                    break;
                }
            }
            else if (status == VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = root;
        }

        if (status == VisitorStatus::Exhausted)
            return false;
        ColumnString * col_str = nullptr;
        if (isColumnNullable(dest))
        {
            ColumnNullable & col_null = assert_cast<ColumnNullable &>(dest);
            col_null.getNullMapData().push_back(0);
            col_str = assert_cast<ColumnString *>(&col_null.getNestedColumn());
        }
        else
        {
            col_str = assert_cast<ColumnString *>(&dest);
        }
        JSONStringSerializer json_serializer(*col_str);
        if (current_element.isString())
        {
            auto str = current_element.getString();
            json_serializer.addRawString(str);
        }
        else
            json_serializer.addElement(current_element);
        json_serializer.commit();
        return true;
    }
};

/**
 * Function to test jsonpath member access, will be removed in final PR
 * @tparam JSONParser parser
 */
template <typename JSONParser, typename JSONStringSerializer>
class JSONQueryImpl
{
public:
    using Element = typename JSONParser::Element;
    using GeneratorJSONPathPtr = std::shared_ptr<GeneratorJSONPath<JSONParser>>;
    using JSONPaths = std::vector<JSONPathInfo<JSONParser>>;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, bool, size_t) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }
    static ColumnNumbers getArgumentsThatAreAlwaysConstant(const size_t) { return {1}; }
    static bool hasMultipleJsonPaths() { return false; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, JSONPaths & json_paths, bool)
    {
        ColumnString & col_str = assert_cast<ColumnString &>(dest);

        Element current_element = root;
        VisitorStatus status;
        bool success = false;
        const char * array_begin = "[";
        const char * array_end = "]";
        const char * comma = ", ";
        JSONStringSerializer json_serializer(col_str);
        json_serializer.addRawData(array_begin, 1);
        auto & generator_json_path = json_paths[0].json_path;
        generator_json_path->reinitialize();
        while ((status = generator_json_path->getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                if (success)
                {
                    json_serializer.addRawData(comma, 2);
                }
                success = true;
                json_serializer.addElement(current_element);
            }
            else if (status == VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = root;
        }
        if (!success)
        {
            json_serializer.rollback();
            return false;
        }
        json_serializer.addRawData(array_end, 1);
        json_serializer.commit();
        return true;
    }
};

template <typename JSONParser, typename JSONStringSerializer>
class JSONTupleImpl
{
public:
    using Element = typename JSONParser::Element;
    using JSONPaths = std::vector<JSONPathInfo<JSONParser>>;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName & arguments, bool, size_t max_query_number)
    {
        if (arguments.size() - 1 > max_query_number)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function json_tuple query path arguments number exceeds the maximum limit {}, you can adjust the limit by `function_json_tuple_max_tuple_element_number`.",
                 max_query_number);
        }
        DataTypes dataTypes;
        for (size_t i = 0; i < arguments.size() - 1; ++i)
        {
            const auto element_type = std::make_shared<DataTypeString>();
            dataTypes.emplace_back(makeNullable(element_type));
        }
        return std::make_shared<DataTypeTuple>(dataTypes);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }
    static bool hasMultipleJsonPaths() { return true; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant(const size_t max_query_number)
    {
        ColumnNumbers numbers;
        for (size_t i = 1; i <= max_query_number; ++i)
            numbers.emplace_back(i);
        return numbers;
    }

    static bool insertResultToColumn(IColumn & dest, const Element & root, JSONPaths & json_paths, bool)
    {
        ColumnTuple & col_tuple = assert_cast<ColumnTuple &>(dest);
        for (size_t i = 0; i < json_paths.size(); ++i)
        {
            auto & col_element = col_tuple.getColumn(i);
            ColumnNullable & col_null = assert_cast<ColumnNullable &>(col_element);
            ColumnString & col_str = assert_cast<ColumnString &>(col_null.getNestedColumn());
            ColumnUInt8 & col_null_map = col_null.getNullMapColumn();
            Element current_element = root;
            VisitorStatus status;
            bool success = false;
            auto & generator_json_path = json_paths[i].json_path;
            bool path_has_asterisk = json_paths[i].path_has_asterisk;
            generator_json_path->reinitialize();
            JSONStringSerializer json_serializer(col_str);
            std::vector<Element> elements_to_serialize;
            while ((status = generator_json_path->getNextItem(current_element)) != VisitorStatus::Exhausted)
            {
                if (status == VisitorStatus::Ok)
                {
                    success = true;
                    if (path_has_asterisk)
                    {
                        elements_to_serialize.emplace_back(current_element);
                    }
                    else if (current_element.isString())
                    {
                        auto str = current_element.getString();
                        json_serializer.addRawString(str);
                        col_null_map.insert(0);
                    }
                    else
                    {
                        json_serializer.addElement(current_element);
                        col_null_map.insert(0);
                    }
                }
                else if (status == VisitorStatus::Error)
                {
                    /// ON ERROR
                    /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                    ///  however this functionality is not implemented yet
                }
                current_element = root;
            }
            if (!success)
            {
                json_serializer.rollback();
                col_null.insertDefault();
                continue;
            }
            /// If path has '[*]', e.g. `$.a[*].b`, then we should seriliaze multiple elements into a string.
            /// E.g. Select('{"a":[{"b":"d", "b":"c"}]}', '$.a[*].b'), the result should be '["d", "c"]'.
            if (path_has_asterisk)
            {
                if (elements_to_serialize.size() == 1)
                {
                    json_serializer.addElement(elements_to_serialize[0]);
                }
                else if (elements_to_serialize.size() > 1)
                {
                    json_serializer.addRawData("[", 1);
                    size_t j = 0;
                    while (j < elements_to_serialize.size() - 1)
                    {
                        json_serializer.addElement(elements_to_serialize[j]);
                        json_serializer.addRawData(",", 1);
                        ++j;
                    }
                    json_serializer.addElement(elements_to_serialize[j]);
                    json_serializer.addRawData("]", 1);
                }
                col_null_map.insert(0);
            }
            json_serializer.commit();
        }
        return true;
    }
};

}
