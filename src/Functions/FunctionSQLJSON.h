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

/// Have implemented the operator << for json elements. So we could use stringstream to serialize json elements.
/// But stingstream have bad performance, not recommend to use it.
template <typename Element>
class DefaultJSONStringSerializer
{
public:
    explicit DefaultJSONStringSerializer(ColumnString & col_str_) : col_str(col_str_) { }

    inline void addRawData(const char * ptr, size_t len)
    {
        out << std::string_view(ptr, len);
    }

    inline void addRawString(std::string_view str)
    {
        out << str;
    }

    /// serialize the json element into stringstream
    inline void addElement(const Element & element)
    {
        out << element.getElement();
    }
    inline void commit()
    {
        auto out_str = out.str();
        col_str.insertData(out_str.data(), out_str.size());
    }
    inline void rollback() {}
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
    inline void addRawData(const char * ptr, size_t len)
    {
        chars.insert(ptr, ptr + len);
    }

    inline void addRawString(std::string_view str)
    {
        chars.insert(str.data(), str.data() + str.size());
    }

    /// serialize the json element into column's buffer directly
    inline void addElement(const Element & element)
    {
        formatter.append(element.getElement());
    }
    inline void commit()
    {
        chars.push_back(0);
        offsets.push_back(chars.size());
    }
    inline void rollback()
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

class EmptyJSONStringSerializer{};


class FunctionSQLJSONHelpers
{
public:
    template <typename Name, typename Impl, class JSONParser>
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse JSONPath");
            }

            /// Get data and offsets for 2 argument (JSON)
            const ColumnString::Chars & chars_json = col_json_string->getChars();
            const ColumnString::Offsets & offsets_json = col_json_string->getOffsets();

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl impl;
            GeneratorJSONPath<JSONParser> generator_json_path(res);
            for (const auto i : collections::range(0, input_rows_count))
            {
                std::string_view json{
                    reinterpret_cast<const char *>(&chars_json[offsets_json[i - 1]]), offsets_json[i] - offsets_json[i - 1] - 1};
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                if (document_ok)
                {
                    /// Instead of creating a new generator for each row, we can reuse the same one.
                    generator_json_path.reinitialize();
                    added_to_column = impl.insertResultToColumn(*to, document, generator_json_path, context);
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
        return Impl<DummyJSONParser, DefaultJSONStringSerializer<DummyJSONParser::Element>>::getReturnType(
            Name::name, arguments, getContext());
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
            return FunctionSQLJSONHelpers::Executor<
                Name,
                Impl<SimdJSONParser, JSONStringSerializer<SimdJSONParser::Element, SimdJSONElementFormatter>>,
                SimdJSONParser>::run(arguments, result_type, input_rows_count, parse_depth, getContext());
#endif
        return FunctionSQLJSONHelpers::
            Executor<Name, Impl<DummyJSONParser, DefaultJSONStringSerializer<DummyJSONParser::Element>>, DummyJSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, getContext());
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

template <typename JSONParser, typename JSONStringSerializer>
class JSONExistsImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, const ContextPtr &) { return std::make_shared<DataTypeUInt8>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, GeneratorJSONPath<JSONParser> & generator_json_path, const ContextPtr &)
    {
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

template <typename JSONParser, typename JSONStringSerializer>
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

    static bool insertResultToColumn(IColumn & dest, const Element & root, GeneratorJSONPath<JSONParser> & generator_json_path, const ContextPtr & context)
    {
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

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &, const ContextPtr &) { return std::make_shared<DataTypeString>(); }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, GeneratorJSONPath<JSONParser> & generator_json_path, const ContextPtr &)
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
        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
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

}
