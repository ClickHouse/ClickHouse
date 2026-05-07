#pragma once

#include <sstream>
#include <type_traits>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/JSONParsers/DummyJSONParser.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Functions/JSONPath/Parsers/ParserJSONPath.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Interpreters/Context.h>
#include <base/range.h>

#include "config.h"


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_simdjson;
    extern const SettingsBool function_json_value_return_type_allow_complex;
    extern const SettingsBool function_json_value_return_type_allow_nullable;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

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

class FunctionSQLJSONHelpers
{
public:
    /// Single-path executor: parses one JSONPath, applies Impl per row.
    template <typename Name, typename Impl, class JSONParser>
    class Executor
    {
    public:
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

            const auto & json_path_column = arguments[1];

            if (!isString(json_path_column.type))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Second argument of JSONPath functions must be a String JSONPath, "
                                "a Tuple(String) of JSONPaths, or an Array(String) of JSONPaths, illegal type: {}",
                                json_path_column.type->getName());
            }
            if (!isColumnConst(*json_path_column.column))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Second argument of JSONPath functions must be a constant String, "
                                "a Tuple(String) of JSONPaths, or an Array(String) of JSONPaths");
            }

            /// Prepare to parse 1 argument (JSONPath)
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

            JSONParser json_parser;
            using Element = typename JSONParser::Element;
            Element document;
            bool document_ok = false;

            /// Parse JSON for every row
            Impl impl;
            GeneratorJSONPath<JSONParser> generator_json_path(res);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view json = json_column.column->getDataAt(i);
                document_ok = json_parser.parse(json, document);

                bool added_to_column = false;
                if (document_ok)
                {
                    /// Instead of creating a new generator for each row, we can reuse the same one.
                    generator_json_path.reinitialize();
                    added_to_column = impl.insertResultToColumn(*to, document, generator_json_path, function_json_value_return_type_allow_complex);
                }
                if (!added_to_column)
                {
                    to->insertDefault();
                }
            }
            return to;
        }
    };

    /// Multi-path executor: the second argument is a nested Tuple/Array/String structure.
    /// Each string leaf is a JSONPath. The result mirrors the structure shape, with each leaf
    /// replaced by the result of applying the Impl to that path.
    ///
    /// The shape of the nested structure is analyzed once before the row loop.
    /// A "plan" is built: a tree of nodes, each knowing its kind (Leaf/Tuple/Array),
    /// its children, and pointers to the destination columns. The per-row loop
    /// follows this plan without any type checks.
    template <typename Impl, class JSONParser>
    class MultiPathExecutor
    {
    public:
        using Element = typename JSONParser::Element;

        static ColumnPtr run(
            const ColumnsWithTypeAndName & arguments,
            const DataTypePtr & result_type,
            size_t input_rows_count,
            uint32_t parse_depth,
            uint32_t parse_backtracks,
            bool function_json_value_return_type_allow_complex)
        {
            const auto & json_column = arguments[0];
            const auto & path_column = arguments[1];

            if (!isString(json_column.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument of JSONPath functions must be a String, got: {}", json_column.type->getName());

            if (!isColumnConst(*path_column.column))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument of JSONPath functions must be a constant String, "
                    "a Tuple(String) of JSONPaths, or an Array(String) of JSONPaths");

            const auto & const_col = assert_cast<const ColumnConst &>(*path_column.column);
            const auto & const_data = const_col.getDataColumn();

            MutableColumnPtr to = result_type->createColumn();
            to->reserve(input_rows_count);

            /// Build the execution plan: parse the structure once, collect generators and column pointers.
            std::vector<std::shared_ptr<GeneratorJSONPath<JSONParser>>> generators;
            PlanNode plan = buildPlan(*to, const_data, path_column.type, 0, parse_depth, parse_backtracks, generators);

            JSONParser json_parser;
            Element document;
            Impl impl;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view json = json_column.column->getDataAt(i);
                bool document_ok = json_parser.parse(json, document);

                size_t gen_idx = 0;
                executePlan(plan, document, generators, gen_idx, document_ok, impl, function_json_value_return_type_allow_complex);
            }
            return to;
        }

    private:
        enum class NodeKind : uint8_t { Leaf, Tuple, Array };

        struct PlanNode
        {
            NodeKind kind;
            IColumn * dest = nullptr;           /// destination column for this node
            size_t array_size = 0;              /// for Array nodes: constant number of elements
            IColumn::Offsets * array_offsets = nullptr; /// for Array nodes: offsets column
            std::vector<PlanNode> children;     /// for Tuple: one child per element; for Array: single child (the element plan)
        };

        static std::shared_ptr<GeneratorJSONPath<JSONParser>> parseJSONPath(
            std::string_view query, uint32_t parse_depth, uint32_t parse_backtracks)
        {
            Tokens tokens(query.data(), query.data() + query.size());
            IParser::Pos token_iterator(tokens, parse_depth, parse_backtracks);
            Expected expected;
            ASTPtr res;
            ParserJSONPath parser;
            if (!parser.parse(token_iterator, res, expected))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse JSONPath");
            return std::make_shared<GeneratorJSONPath<JSONParser>>(res);
        }

        /// Build the plan tree: analyze the structure once, collecting generators and column pointers.
        /// `index` is the row index within `path_data` to read structure from.
        static PlanNode buildPlan(
            IColumn & dest,
            const IColumn & path_data,
            const DataTypePtr & path_type,
            size_t index,
            uint32_t parse_depth,
            uint32_t parse_backtracks,
            std::vector<std::shared_ptr<GeneratorJSONPath<JSONParser>>> & generators)
        {
            if (isString(path_type))
            {
                PlanNode node;
                node.kind = NodeKind::Leaf;
                node.dest = &dest;
                generators.push_back(parseJSONPath(path_data.getDataAt(index), parse_depth, parse_backtracks));
                return node;
            }

            if (checkAndGetDataType<DataTypeTuple>(path_type.get()))
            {
                const auto & tuple_col = assert_cast<const ColumnTuple &>(path_data);
                const auto & tuple_type = assert_cast<const DataTypeTuple &>(*path_type);
                auto & dest_tuple = assert_cast<ColumnTuple &>(dest);

                PlanNode node;
                node.kind = NodeKind::Tuple;
                node.dest = &dest;
                node.children.reserve(tuple_col.tupleSize());
                for (size_t i = 0; i < tuple_col.tupleSize(); ++i)
                    node.children.push_back(buildPlan(
                        dest_tuple.getColumn(i), tuple_col.getColumn(i),
                        tuple_type.getElement(i), index, parse_depth, parse_backtracks, generators));
                return node;
            }

            if (const auto * array_type = checkAndGetDataType<DataTypeArray>(path_type.get()))
            {
                const auto & array_path = assert_cast<const ColumnArray &>(path_data);
                auto & dest_array = assert_cast<ColumnArray &>(dest);

                size_t offset = index == 0 ? 0 : array_path.getOffsets()[index - 1];
                size_t array_size = array_path.getOffsets()[index] - offset;

                PlanNode node;
                node.kind = NodeKind::Array;
                node.dest = &dest;
                node.array_size = array_size;
                node.array_offsets = &dest_array.getOffsets();

                /// Each array element gets its own child plan node (they may reference different generators).
                node.children.reserve(array_size);
                for (size_t i = 0; i < array_size; ++i)
                    node.children.push_back(buildPlan(
                        dest_array.getData(), array_path.getData(),
                        array_type->getNestedType(), offset + i,
                        parse_depth, parse_backtracks, generators));
                return node;
            }

            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "JSONPath structure must contain only String, Tuple, or Array elements, got: {}",
                path_type->getName());
        }

        /// Execute the plan for one row. No type checks — just follows the pre-built tree.
        static void executePlan(
            const PlanNode & node,
            const Element & document,
            const std::vector<std::shared_ptr<GeneratorJSONPath<JSONParser>>> & generators,
            size_t & gen_idx,
            bool document_ok,
            Impl & impl,
            bool function_json_value_return_type_allow_complex)
        {
            switch (node.kind)
            {
                case NodeKind::Leaf:
                {
                    if (!document_ok)
                    {
                        node.dest->insertDefault();
                        ++gen_idx;
                        return;
                    }
                    auto & generator = *generators[gen_idx];
                    generator.reinitialize();
                    bool added = impl.insertResultToColumn(*node.dest, document, generator, function_json_value_return_type_allow_complex);
                    if (!added)
                        node.dest->insertDefault();
                    ++gen_idx;
                    return;
                }

                case NodeKind::Tuple:
                {
                    for (const auto & child : node.children)
                        executePlan(child, document, generators, gen_idx, document_ok, impl, function_json_value_return_type_allow_complex);
                    return;
                }

                case NodeKind::Array:
                {
                    for (const auto & child : node.children)
                        executePlan(child, document, generators, gen_idx, document_ok, impl, function_json_value_return_type_allow_complex);

                    size_t base = node.array_offsets->empty() ? 0 : node.array_offsets->back();
                    node.array_offsets->push_back(base + node.array_size);
                    return;
                }
            }
        }
    };

    /// Build a return type that mirrors the path argument structure,
    /// replacing each String leaf with `leaf_type`.
    static DataTypePtr buildReturnType(const DataTypePtr & path_type, const DataTypePtr & leaf_type)
    {
        if (isString(path_type))
            return leaf_type;

        if (const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(path_type.get()))
        {
            DataTypes element_types;
            element_types.reserve(tuple_type->getElements().size());
            for (const auto & elem : tuple_type->getElements())
                element_types.push_back(buildReturnType(elem, leaf_type));
            if (tuple_type->hasExplicitNames())
                return std::make_shared<DataTypeTuple>(element_types, tuple_type->getElementNames());
            return std::make_shared<DataTypeTuple>(element_types);
        }

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(path_type.get()))
            return std::make_shared<DataTypeArray>(buildReturnType(array_type->getNestedType(), leaf_type));

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "JSONPath structure must contain only String, Tuple, or Array elements, got {}",
            path_type->getName());
    }

    /// Check whether a type is a nested structure (Tuple/Array) with String leaves,
    /// i.e., suitable as a multi-path argument.
    static bool isMultiPathType(const DataTypePtr & type)
    {
        if (isString(type))
            return false; /// A plain String is not multi-path, it's the normal single-path case.

        if (const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(type.get()))
        {
            if (tuple_type->getElements().empty())
                return false; /// Empty Tuple() is not a valid multi-path structure.
            for (const auto & elem : tuple_type->getElements())
                if (!isString(elem) && !isMultiPathType(elem))
                    return false;
            return true;
        }

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(type.get()))
            return isString(array_type->getNestedType()) || isMultiPathType(array_type->getNestedType());

        return false;
    }
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
          function_json_value_return_type_allow_nullable(context_->getSettingsRef()[Setting::function_json_value_return_type_allow_nullable])
    {
    }

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
            Name::name, arguments, function_json_value_return_type_allow_nullable);
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
            return Impl<SimdJSONParser, JSONStringSerializer<SimdJSONParser::Element, SimdJSONElementFormatter>>::execute(
                arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
#endif
        return Impl<DummyJSONParser, DefaultJSONStringSerializer<DummyJSONParser::Element>>::execute(
            arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
    }
private:
    const size_t max_parser_depth;
    const size_t max_parser_backtracks;
    const bool allow_simdjson;
    const bool function_json_value_return_type_allow_complex;
    const bool function_json_value_return_type_allow_nullable;
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

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName & arguments, bool)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function JSON_EXISTS requires at least 2 arguments");

        if (FunctionSQLJSONHelpers::isMultiPathType(arguments[1].type))
            return FunctionSQLJSONHelpers::buildReturnType(arguments[1].type, std::make_shared<DataTypeUInt8>());

        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, GeneratorJSONPath<JSONParser> & generator_json_path, bool)
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

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count,
                             uint32_t parse_depth, uint32_t parse_backtracks, bool function_json_value_return_type_allow_complex)
    {
        if (FunctionSQLJSONHelpers::isMultiPathType(arguments[1].type))
            return FunctionSQLJSONHelpers::MultiPathExecutor<JSONExistsImpl, JSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);

        return FunctionSQLJSONHelpers::Executor<NameJSONExists, JSONExistsImpl, JSONParser>::run(
            arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
    }
};

template <typename JSONParser, typename JSONStringSerializer>
class JSONValueImpl
{
public:
    using Element = typename JSONParser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName & arguments, bool function_json_value_return_type_allow_nullable)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function JSON_VALUE requires at least 2 arguments");

        if (FunctionSQLJSONHelpers::isMultiPathType(arguments[1].type))
        {
            DataTypePtr leaf_type = std::make_shared<DataTypeString>();
            if (function_json_value_return_type_allow_nullable)
                leaf_type = makeNullable(leaf_type);
            return FunctionSQLJSONHelpers::buildReturnType(arguments[1].type, leaf_type);
        }

        if (function_json_value_return_type_allow_nullable)
        {
            DataTypePtr string_type = std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeNullable>(string_type);
        }

        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, GeneratorJSONPath<JSONParser> & generator_json_path, bool function_json_value_return_type_allow_complex)
    {
        Element current_element = root;
        VisitorStatus status;

        while ((status = generator_json_path.getNextItem(current_element)) != VisitorStatus::Exhausted)
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
            col_null.getNullMapData().push_back(false);
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

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count,
                             uint32_t parse_depth, uint32_t parse_backtracks, bool function_json_value_return_type_allow_complex)
    {
        if (FunctionSQLJSONHelpers::isMultiPathType(arguments[1].type))
            return FunctionSQLJSONHelpers::MultiPathExecutor<JSONValueImpl, JSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);

        return FunctionSQLJSONHelpers::Executor<NameJSONValue, JSONValueImpl, JSONParser>::run(
            arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
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

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName & arguments, bool)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function JSON_QUERY requires at least 2 arguments");

        if (FunctionSQLJSONHelpers::isMultiPathType(arguments[1].type))
            return FunctionSQLJSONHelpers::buildReturnType(arguments[1].type, std::make_shared<DataTypeString>());

        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & root, GeneratorJSONPath<JSONParser> & generator_json_path, bool)
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

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count,
                             uint32_t parse_depth, uint32_t parse_backtracks, bool function_json_value_return_type_allow_complex)
    {
        if (FunctionSQLJSONHelpers::isMultiPathType(arguments[1].type))
            return FunctionSQLJSONHelpers::MultiPathExecutor<JSONQueryImpl, JSONParser>::run(
                arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);

        return FunctionSQLJSONHelpers::Executor<NameJSONQuery, JSONQueryImpl, JSONParser>::run(
            arguments, result_type, input_rows_count, parse_depth, parse_backtracks, function_json_value_return_type_allow_complex);
    }
};

}
