#pragma once

#include <type_traits>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Core/AccurateComparison.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/DummyJSONParser.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/Generators/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Functions/RapidJSONParser.h>
#include <Functions/SimdJSONParser.h>
#include <Interpreters/Context.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <boost/tti/has_member_function.hpp>
#include <Common/CpuId.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <ext/range.h>
//#include <IO/Operators.h>
#include <sstream>

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
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
        {
            MutableColumnPtr to{result_type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.size() < 2)
            {
                throw Exception{"JSONPath functions require at least 2 arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
            }

            /// Check 1 argument: must be of type String (JSONPath)
            const auto & first_column = arguments[0];
            if (!isString(first_column.type))
            {
                throw Exception{
                    "JSONPath functions require 1 argument to be JSONPath of type string, illegal type: " + first_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }

            /// Check 2 argument: must be of type String (JSON)
            const auto & second_column = arguments[1];
            if (!isString(second_column.type))
            {
                throw Exception{
                    "JSONPath functions require 2 argument to be JSON of string, illegal type: " + second_column.type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }

            /// If argument is successfully cast to (ColumnConst *) then it is quoted string
            /// Example:
            ///     SomeFunction('some string argument')
            ///
            /// Otherwise it is a column
            /// Example:
            ///     SomeFunction(database.table.column)

            /// Check 1 argument: must be const String (JSONPath)
            const ColumnPtr & arg_jsonpath = first_column.column;
            const auto * arg_jsonpath_const = typeid_cast<const ColumnConst *>(arg_jsonpath.get());
            if (!arg_jsonpath_const)
            {
                throw Exception{"JSONPath argument must be of type const String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
            }
            /// Retrieve data from 1 argument
            const auto * arg_jsonpath_string = typeid_cast<const ColumnString *>(arg_jsonpath_const->getDataColumnPtr().get());
            if (!arg_jsonpath_string)
            {
                throw Exception{"Illegal column " + arg_jsonpath->getName(), ErrorCodes::ILLEGAL_COLUMN};
            }

            /// Check 2 argument: must be const or non-const String (JSON)
            const ColumnPtr & arg_json = second_column.column;
            const auto * col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            const auto * col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            /// Get data and offsets for 1 argument (JSONPath)
            const ColumnString::Chars & chars_path = arg_jsonpath_string->getChars();
            const ColumnString::Offsets & offsets_path = arg_jsonpath_string->getOffsets();

            /// Get data and offsets for 1 argument (JSON)
            const char * query_begin = reinterpret_cast<const char *>(&chars_path[0]);
            const char * query_end = query_begin + offsets_path[0] - 1;

            /// Tokenize query
            Tokens tokens(query_begin, query_end);
            /// Max depth 0 indicates that depth is not limited
            IParser::Pos token_iterator(tokens, 0);

            /// Parse query and create AST tree
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                throw Exception{"Unable to parse JSONPath", ErrorCodes::BAD_ARGUMENTS};
            }

            /// Get data and offsets for 1 argument (JSON)
            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl<JSONParser> impl;
            for (const auto i : ext::range(0, input_rows_count))
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

private:
};

template <typename Name, template <typename> typename Impl>
class FunctionSQLJSON : public IFunction
{
public:
    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionSQLJSON>(context_); }
    FunctionSQLJSON(const Context & context_) : context(context_) { }

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
        /// 1. Lexer(path) -> Tokens
        /// 2. Create ASTPtr
        /// 3. Parser(Tokens, ASTPtr) -> complete AST
        /// 4. Execute functions, call interpreter for each json (in function)
#if USE_SIMDJSON
        if (context.getSettingsRef().allow_simdjson)
            return FunctionSQLJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count);
#endif

#if USE_RAPIDJSON
        throw Exception{"RapidJSON is not supported :(", ErrorCodes::BAD_ARGUMENTS};
#else
        return FunctionSQLJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count);
#endif
    }

private:
    const Context & context;
};

struct NameSQLJSONTest
{
    static constexpr auto name{"SQLJSONTest"};
};

struct NameSQLJSONMemberAccess
{
    static constexpr auto name{"SQLJSONMemberAccess"};
};

/**
 * Function to test logic before function calling, will be removed in final PR
 * @tparam JSONParser parser
 */
template <typename JSONParser>
class SQLJSONTestImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element &, ASTPtr &)
    {
        String str = "I am working:-)";
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        col_str.insertData(str.data(), str.size());
        return true;
    }
};

/**
 * Function to test jsonpath member access, will be removed in final PR
 * @tparam JSONParser parser
 */
template <typename JSONParser>
class SQLJSONMemberAccessImpl
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
        while ((status = generator_json_path.getNextItem(current_element)) == VisitorStatus::Ok)
        {
            /// No-op
        }
        if (status == VisitorStatus::Error)
        {
            return false;
        }
        ColumnString & col_str = assert_cast<ColumnString &>(dest);
        std::stringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ostr << current_element.getElement();
        auto output_str = ostr.str();
        col_str.insertData(output_str.data(), output_str.size());
        return true;
    }
};

}
