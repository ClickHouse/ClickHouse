#pragma once

#include <sstream>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Functions/RapidJSONParser.h>
#include <Functions/SimdJSONParser.h>
#include <Interpreters/Context.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <base/range.h>

#include "config_functions.h"

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int BAD_ARGUMENTS;
}

class FunctionSQLJSONHelpers
{
public:
    template <typename Name, template <typename> typename Impl, class JSONParser>
    class Executor
    {
    public:
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.size() < 2)
            {
                throw Exception{"JSONPath functions require at least 2 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
            }

            const auto & json_column = arguments[0];

            if (!isString(json_column.type))
            {
                throw Exception(
                    "JSONPath functions require first argument to be JSON of string, illegal type: " + json_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const auto & json_path_column = arguments[1];

            if (!isString(json_path_column.type))
            {
                throw Exception(
                    "JSONPath functions require second argument to be JSONPath of type string, illegal type: " + json_path_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            if (!isColumnConst(*json_path_column.column))
            {
                throw Exception("Second argument (JSONPath) must be constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const ColumnPtr & arg_jsonpath = json_path_column.column;
            const auto * arg_jsonpath_const = typeid_cast<const ColumnConst *>(arg_jsonpath.get());
            const auto * arg_jsonpath_string = typeid_cast<const ColumnString *>(arg_jsonpath_const->getDataColumnPtr().get());

            const ColumnPtr & arg_json = json_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            /// Get data and offsets for 1 argument (JSONPath)
            const ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Prepare to parse 1 argument (JSONPath)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
            }

            /// Get data and offsets for 2 argument (JSON)
            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl<JSONParser> impl;

            for (const auto i : collections::range(0, input_rows_count))
            {
                std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                if (document_ok)
                {
                    added_to_column = impl.insertResultToColumn(*to, document, res);
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

template <typename Name, template <typename> typename Impl>
class FunctionSQLJSON : public IFunction, WithConstContext
{
public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionSQLJSON>(context_); }
    explicit FunctionSQLJSON(ContextPtr context_) : WithConstContext(context_) { }

    static constexpr auto name = Name::name;
    String getName() const override { return Name::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return Impl<DummyJSONParser>::getReturnType(Name::name, arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Choose JSONParser.
        /// 1. Lexer(path) -> Tokens
        /// 2. Create ASTPtr
        /// 3. Parser(Tokens, ASTPtr) -> complete AST
        /// 4. Execute functions: call getNextItem on generator and handle each item
        uint32_t parse_depth = getContext()->getSettingsRef().max_parser_depth;
#if USE_SIMDJSON
        if (getContext()->getSettingsRef().allow_simdjson)
            return FunctionSQLJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count, parse_depth);
#endif
        return FunctionSQLJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count, parse_depth);
    }
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

template <typename JSONParser>
class JSONExistsImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        VisitorStatus status;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
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

template <typename JSONParser>
class JSONValueImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        VisitorStatus status;
        Element res;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
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
        {
            return false;
        }

        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        out << current_element.getElement();
        auto output_str = out.str();
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(output_str.data(), output_str.size());
        return true;
    }
};

/**
 * Function to test jsonpath member access, will be removed in final PR
 * @tparam JSONParser parser
 */
template <typename JSONParser>
class JSONQueryImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        VisitorStatus status;
        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        /// Create json array of results: [res1, res2, ...]
        out << "[";
        bool success = false;
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                if (success)
                {
                    out << ", ";
                }
                success = true;
                out << current_element.getElement();
            }
            else if (status == VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = root;
        }
        out << "]";
        if (!success)
        {
            return false;
        }
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        auto output_str = out.str();
        col_str.insertData(output_str.data(), output_str.size());
        return true;
    }
};

}
