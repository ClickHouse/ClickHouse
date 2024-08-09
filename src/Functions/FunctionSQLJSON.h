#pragma once

#include <sstream>
#include <type_traits>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Interpreters/Context.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/range.h>

#include "config.h"


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
        static ColumnPtr run(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, uint32_t parse_depth, const ContextPtr & context)
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

            const auto & json_path_column = arguments[1];

            if (!isString(json_path_column.type))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "JSONPath functions require second argument "
                                "to be JSONPath of type string, illegal type: {}", json_path_column.type->getName());
            }
            if (!isColumnConst(*json_path_column.column))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument (JSONPath) must be constant string");
            }

            /// Prepare to parse 1 argument (JSONPath)
            String query = typeid_cast<const ColumnConst &>(*json_path_column.column).getValue<String>();

            /// Tokenize the query
            Tokens tokens(query.data(), query.data() + query.size());
            /// Max depth 0 indicates that depth is not limited
            IParser::Pos token_iterator(tokens, parse_depth);

            /// Parse query and create AST tree
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            const bool parse_res = parser.parse(token_iterator, res, expected);
            if (!parse_res)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse JSONPath");
            }

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl<JSONParser> impl;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view json = json_column.column->getDataAt(i).toView();
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                if (document_ok)
                {
                    added_to_column = impl.insertResultToColumn(*to, document, res, context);
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
        return Impl<DummyJSONParser>::getReturnType(Name::name, arguments, getContext());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Choose JSONParser.
        /// 1. Lexer(path) -> Tokens
        /// 2. Create ASTPtr
        /// 3. Parser(Tokens, ASTPtr) -> complete AST
        /// 4. Execute functions: call getNextItem on generator and handle each item
        unsigned parse_depth = static_cast<unsigned>(getContext()->getSettingsRef().max_parser_depth);
#if USE_SIMDJSON
        if (getContext()->getSettingsRef().allow_simdjson)
            return FunctionSQLJSONHelpers::Executor<Name, Impl, SimdJSONParser>::run(arguments, result_type, input_rows_count, parse_depth, getContext());
#endif
        return FunctionSQLJSONHelpers::Executor<Name, Impl, DummyJSONParser>::run(arguments, result_type, input_rows_count, parse_depth, getContext());
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

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, const ContextPtr &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr, const ContextPtr &)
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

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, const ContextPtr & context)
    {
        if (context->getSettingsRef().function_json_value_return_type_allow_nullable)
        {
            DataTypePtr string_type = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeNullable>(string_type);
        }
        else
        {
            return std::make_shared<DataTypeString>();
        }
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr, const ContextPtr & context)
    {
        GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        VisitorStatus status;

        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
        {
            if (status == VisitorStatus::Ok)
            {
                if (context->getSettingsRef().function_json_value_return_type_allow_complex)
                {
                    break;
                }
                else if (!(current_element.isArray() || current_element.isObject()))
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

        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        out << current_element.getElement();
        auto output_str = out.str();
        ColumnString * col_str;
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
        ColumnString::Chars & data = col_str->getChars();
        ColumnString::Offsets & offsets = col_str->getOffsets();

        if (current_element.isString())
        {
            ReadBufferFromString buf(output_str);
            readJSONStringInto(data, buf);
            data.push_back(0);
            offsets.push_back(data.size());
        }
        else
        {
            col_str->insertData(output_str.data(), output_str.size());
        }
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

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, const ContextPtr &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, ASTPtr & query_ptr, const ContextPtr &)
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
