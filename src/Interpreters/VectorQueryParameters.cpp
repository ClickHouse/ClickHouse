#include <Interpreters/VectorQueryParameters.h>

#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/Lexer.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Common/StringUtils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/FieldToDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/castColumn.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/convertFieldToType.h>
#include <Columns/ColumnConst.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Poco/String.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_set>

namespace DB
{

namespace
{

auto logger = getLogger("VectorQueryParameters");
using NodePath = std::vector<UInt32>;

/// Canonical lowercase names for the recognized vector/search functions.
constexpr std::string_view COSINEDISTANCE_FUNCTION_NAME = "cosinedistance";
constexpr std::string_view L2DISTANCE_FUNCTION_NAME = "l2distance";
constexpr std::string_view HASTOKEN_FUNCTION_NAME = "hastoken";
constexpr std::string_view CAST_FUNCTION_NAME = "cast";

/// Convert a DataTypePtr to its string representation for logging/debugging.
String dataTypePtrToString(const DataTypePtr & type)
{
    if (!type)
        return "nullptr";
    return type->getName();
}

/// Check whether a bareword preceding the '-' symbol is a SQL keyword.
/// If it is, the '-' should be treated as a minus operator rather than part of a negative number literal.
bool tokenIsKeyWord(const String & token_name)
{
    String name = Poco::toLower(token_name);
    static const std::unordered_set<String> keywords = {
        "select", "from", "where", "and", "or", "not", "in", "like", "ilike",
        "is", "null", "between", "case", "when", "then", "else", "end",
        "order", "by", "group", "having", "limit", "offset", "as", "on", "join",
        "left", "right", "inner", "outer", "full", "cross", "natural", "using",
        "union", "intersect", "except", "all", "any", "some",
        "with", "recursive", "array", "map", "tuple", "set", "values",
        "insert", "update", "delete", "create", "alter", "drop", "truncate",
        "over", "partition", "range", "rows", "groups", "unbounded", "preceding",
        "following", "current", "row", "nulls", "first", "last",
        "distinct", "exists", "having", "returning", "format", "type", "settings",
        "sample", "if", "window", "frame", "exclude", "ties",
        "grant", "revoke", "privileges", "role", "user", "show", "describe", "explain",
        "primary", "key", "foreign", "references", "constraint", "unique", "index",
        "default", "check", "cascade", "restrict", "no", "action",
        "asc", "desc", "nulls", "unique", "using", "tablesample",
        "materialized", "temporary", "temporary", "replace", "ignore", "strict",
        "lazy", "volatile", "immutable", "stable"
    };
    return keywords.contains(name);
}
/// Check whether the token preceding a '-' is a value (not a keyword/operator/comma).
/// When the preceding token is a value, the '-' is an arithmetic operator (e.g. "a - 1"),
/// not the start of a negative number literal (e.g. "WHERE x = -1").
bool tokenIsValue(Token token)
{
    if (token.type == TokenType::Comma || token.type == TokenType::OpeningRoundBracket || token.type == TokenType::OpeningSquareBracket ||
        token.type == TokenType::Equals || token.type == TokenType::NotEquals || token.type == TokenType::Less || token.type == TokenType::Greater ||
        token.type == TokenType::LessOrEquals || token.type == TokenType::GreaterOrEquals ||
        (token.type == TokenType::BareWord && tokenIsKeyWord(std::string(token.begin, token.size())))
        )
        return false;
    return true;
}

/// Determine whether a function's arguments can be safely normalized (replaced with '?')
/// for caching purposes.  Functions that are non-deterministic, timezone-sensitive, or
/// whose string arguments are structural (type names, format patterns) would cause
/// incorrect cache key collisions if normalized, so they are excluded.
bool functionCanCache(const String & function_name)
{
    String fun_name = Poco::toLower(function_name);
    // Functions that take type name as string argument - the string changes query structure
    // and cannot be normalized to a placeholder without causing cache key collisions
    if (fun_name == "variantelement" ||
        fun_name == "tupleelement" ||
        fun_name == "dynamicelement" ||
        fun_name == "reinterpretasstring" ||
        fun_name == "reinterpretasint8" ||
        fun_name == "reinterpretasint16" ||
        fun_name == "reinterpretasint32" ||
        fun_name == "reinterpretasint64" ||
        fun_name == "reinterpretasuint8" ||
        fun_name == "reinterpretasuint16" ||
        fun_name == "reinterpretasuint32" ||
        fun_name == "reinterpretasuint64" ||
        fun_name == "reinterpretasdate" ||
        fun_name == "reinterpretasdatetime" ||
        fun_name == "reinterpretasdatetime64" ||
        fun_name == "reinterpretastimestamp" ||
        fun_name == "substringindex" ||
        fun_name == "bech32" ||
        fun_name == "tochar" ||
        fun_name == "format" ||
        fun_name == "extract" ||
        fun_name == "dateformat" ||
        fun_name == "frombase64" ||
        fun_name == "tobase64" ||
        fun_name == "hex" ||
        fun_name == "unhex" ||
        fun_name == "bitmasktoarray" ||
        fun_name == "polygon" ||
        fun_name == "polygonconvexhull" ||
        fun_name == "polygonarea" ||
        fun_name == "polygonperimeter" ||
        fun_name == "polygonpoint" ||
        fun_name == "polygoncontains" ||
        fun_name == "geohashencode" ||
        fun_name == "geohashdecode" ||
        fun_name == "h3tochildren" ||
        fun_name == "h3fromchildren" ||
        fun_name == "geodesyazimuth" ||
        fun_name == "printf" ||
        fun_name == "xxhash64" ||
        fun_name == "xxhash32" ||
        fun_name == "cityhash64" ||
        fun_name == "cityhash32" ||
        fun_name == "cityhash32" ||
        fun_name == "hasanytokens" ||
        fun_name == "hasalltokens" ||
        fun_name == "hasphrase" ||
        fun_name == "interval" ||
        fun_name == "profileevents" ||
        fun_name == "tonullable" ||
        // rand functions
        fun_name == "rand" ||
        fun_name == "rand64" ||
        fun_name == "randcanonical" ||
        fun_name == "randconstant" ||
        fun_name == "randuniform" ||
        fun_name == "randnormal" ||
        fun_name == "randomstring" ||
        fun_name == "randomstringutf8" ||
        fun_name == "randomprintableascii" ||
        fun_name == "randomfixedstring" ||
        // Timezone-related functions - timezone affects query result
        fun_name == "datetrunc" ||
        fun_name == "todate" ||
        fun_name == "todate32" ||
        fun_name == "todatetime" ||
        fun_name == "todatetime64" ||
        fun_name == "toyyyymm" ||
        fun_name == "toyyyymmdd" ||
        fun_name == "toyyyymmddhhmmss" ||
        fun_name == "tostartofday" ||
        fun_name == "tostartofweek" ||
        fun_name == "tostartofmonth" ||
        fun_name == "tostartofquarter" ||
        fun_name == "tostartofyear" ||
        fun_name == "tostartofminute" ||
        fun_name == "tostartofhour" ||
        fun_name == "tostartoffiveminute" ||
        fun_name == "tostartoftenminutes" ||
        fun_name == "tounixtimestamp" ||
        fun_name == "timezonehour" ||
        fun_name == "timezoneminute" ||
        fun_name == "fromunixtime" ||
        fun_name == "formatdatetime")
    {
        LOG_DEBUG(logger, "fun_name={}", fun_name);
        return false;
    }
    return true;
}

/// Check whether a lexer token (if it looks like a function name) can be cached.
/// Tokens shorter than 3 characters are never function names, so they pass.
bool tokenCanCache(Token token)
{
    if (token.size() >= 3)
        return functionCanCache(std::string(token.begin, token.size()));
    return true;
}

/// Check whether a function name should be processed by the parameterizer.
/// When `only_vector` is true, only vector search functions (cosinedistance,
/// l2distance, hastoken) are recognized.  Otherwise, the general functionCanCache
/// check is used.
bool checkFunctionName(const String function_name, bool only_vector)
{
    if (only_vector)
    {
        if (function_name == COSINEDISTANCE_FUNCTION_NAME ||
            function_name == L2DISTANCE_FUNCTION_NAME ||
            function_name == HASTOKEN_FUNCTION_NAME)
            return true;
        return false;
    }
    return functionCanCache(function_name);
}

/// Assign a numeric rank to each numeric TypeIndex for type promotion decisions.
/// Higher rank means wider type; used by getType() to pick the wider of two numeric types.
constexpr int getTypeRank(TypeIndex idx)
{
    switch (idx)
    {
        case TypeIndex::UInt8: return 1;
        case TypeIndex::UInt16: return 2;
        case TypeIndex::UInt32: return 3;
        case TypeIndex::UInt64: return 4;
        case TypeIndex::UInt128: return 5;
        case TypeIndex::UInt256: return 6;
        case TypeIndex::Int8: return 11;
        case TypeIndex::Int16: return 12;
        case TypeIndex::Int32: return 13;
        case TypeIndex::Int64: return 14;
        case TypeIndex::Int128: return 15;
        case TypeIndex::Int256: return 16;
        case TypeIndex::BFloat16: return 20;
        case TypeIndex::Float32: return 21;
        case TypeIndex::Float64: return 22;
        default: return 0;
    }
}

/// Pick the wider of two numeric types for constant replacement.
/// When the runtime parameter type is wider than the plan's stored type (e.g. Int64 vs Int32),
/// the wider type is used to avoid truncation.  Array types always use the result_type.
DataTypePtr getType(DataTypePtr data_type_ptr, DataTypePtr result_type)
{
    if (isArray(result_type) || !data_type_ptr)
        return result_type;

    WhichDataType which_data(data_type_ptr);
    WhichDataType which_result(result_type);
    bool data_is_numeric = which_data.isInteger() || which_data.isFloat();
    bool result_is_numeric = which_result.isInteger() || which_result.isFloat();

    if (!data_is_numeric || !result_is_numeric)
        return result_type;

    int data_rank = getTypeRank(data_type_ptr->getColumnType());
    int result_rank = getTypeRank(result_type->getColumnType());

    if (data_rank > result_rank)
        return data_type_ptr;
    return result_type;
}

/// Check whether a DAG scope string indicates a VectorScan step.
/// VectorScan bindings use special matching rules for array-typed constants.
bool isVectorScanBindingScope(const String & dag_scope)
{
    return dag_scope.find("VectorScan") != String::npos;
}

/// Navigate an AST tree by following a sequence of child indices.
/// Returns the node at the given path, or nullptr if any index is out of bounds.
ASTPtr getASTNodeByPath(ASTPtr root, const std::vector<UInt32> & path)
{
    ASTPtr current = std::move(root);
    for (const auto index : path)
    {
        if (!current || index >= current->children.size())
            return {};
        current = current->children[index];
    }
    return current;
}

// std::string_view stripOuterQuotes(std::string_view value)
// {
//     if (value.size() >= 2 && ((value.front() == '\'' && value.back() == '\'') || (value.front() == '"' && value.back() == '"')))
//         return value.substr(1, value.size() - 2);
//     return value;
// }


/// Parse a SQL single-quoted string literal (e.g. 'hello world') into a Field.
/// Uses ClickHouse's SQL-style quoting rules (backslash escapes, doubled quotes).
bool parseStringLiteral(std::string_view literal, Field & result)
{
    ReadBufferFromMemory buf(literal.data(), literal.size());
    String value;
    readQuotedStringWithSQLStyle(value, buf);
    if (!buf.eof())
        return false;
    result = std::move(value);
    return true;
}

/// Fast path for parsing numeric literals (integers and floats) from raw token text.
/// Handles optional sign prefixes, underscore separators, decimal points, and exponents.
/// Returns true and sets `result` on success; returns false on malformed input.
bool parseNumberLiteralFast(std::string_view literal, Field & result)
{
    if (literal.empty())
        return false;

    bool negative = false;
    if (literal.front() == '-' || literal.front() == '+')
    {
        negative = (literal.front() == '-');
        literal.remove_prefix(1);
    }

    if (literal.empty())
        return false;

    // Check if this is likely a float by looking for decimal point or exponent
    bool is_float = false;
    for (char ch : literal)
    {
        if (ch == '.' || ch == 'e' || ch == 'E')
        {
            is_float = true;
            break;
        }
    }

    // Remove underscores for both integer and float cases
    std::string clean_literal;
    clean_literal.reserve(literal.size());
    for (char ch : literal)
    {
        if (ch != '_')
            clean_literal.push_back(ch);
    }

    if (clean_literal.empty())
        return false;

    if (is_float)
    {
        // Handle float parsing with sign
        std::string float_str;
        if (negative)
            float_str = "-" + clean_literal;
        else
            float_str = clean_literal;

        ReadBufferFromMemory buf(float_str.data(), float_str.size());
        Float64 float_value{};
        if (tryReadFloatTextPrecise(float_value, buf) && buf.eof())
        {
            result = float_value;
            return true;
        }
        return false;
    }
    else
    {
        // Handle integer parsing
        ReadBufferFromMemory buf(clean_literal.data(), clean_literal.size());
        Int64 int_value{};
        UInt64 uint_value{};

        // Try signed integer first
        if (negative)
        {
            if (tryReadIntText(int_value, buf) && buf.eof())
            {
                result = -int_value;
                return true;
            }
        }
        else
        {
            // Try unsigned integer
            if (tryReadIntText(uint_value, buf) && buf.eof())
            {
                result = uint_value;
                return true;
            }
            return false;
        }
        return false;
    }
}

/// Parse a string representation of a numeric array (e.g. "[1.0, 2.0, 3.0]") into a typed Array Field.
/// Uses the target DataTypeArray's serialization to deserialize the text directly.
/// Returns false on parse failure or if the target type is not an array type.
bool stringToNumericArrayField(std::string_view literal, const DataTypePtr & target_type, Field & result)
{
    if (!target_type)
        return false;

    // Check if target type is Array type
    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(target_type.get());
    if (!array_type)
        return false;

    try
    {
        // Get the serialization for the target array type
        auto array_serialization = target_type->getDefaultSerialization();

        // Create a column to hold the result
        auto result_column = target_type->createColumn();

        // Create read buffer from the literal string
        ReadBufferFromMemory read_buffer(literal.data(), literal.size());

        // Use format settings (can be customized if needed)
        FormatSettings format_settings;

        // Directly deserialize the whole text into the column
        // This bypasses the castColumn overhead and goes straight to serialization
        array_serialization->deserializeWholeText(*result_column, read_buffer, format_settings);

        // Verify that the entire input was consumed
        if (!read_buffer.eof())
        {
            // There's unexpected data after parsing, treat as failure
            return false;
        }

        // Extract the Field from the column
        result_column->get(0, result);
        return true;
    }
    catch (...)
    {
        LOG_TRACE(logger, "stringToNumericArrayField error: {}", getCurrentExceptionMessage(false));
        // Any exception during parsing should be caught and return false
        return false;
    }
}


/// Check if a Field type is one of the three numeric types (UInt64, Int64, Float64).
bool isNumericFieldType(Field::Types::Which type)
{
    return type == Field::Types::UInt64
        || type == Field::Types::Int64
        || type == Field::Types::Float64;
}

/// Convert any numeric Field value to Float64 for cross-type comparison.
/// Returns 0.0 for non-numeric types.
Float64 toFloat64(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::UInt64:
            return static_cast<Float64>(field.safeGet<UInt64>());
        case Field::Types::Int64:
            return static_cast<Float64>(field.safeGet<Int64>());
        case Field::Types::Float64:
            return field.safeGet<Float64>();
        default:
            return 0.0;
    }
}

/// Compare two Fields for value equality, handling cross-type numeric comparison.
/// Array fields are compared element-wise recursively.  When types differ but both
/// are numeric, the values are promoted to Float64 before comparison.
bool fieldsEquivalent(const Field & lhs, const Field & rhs)
{
    const auto lhs_type = lhs.getType();
    const auto rhs_type = rhs.getType();
    if (lhs_type == rhs_type)
    {
        if (lhs_type == Field::Types::Array)
        {
            const auto & lhs_array = lhs.safeGet<Array>();
            const auto & rhs_array = rhs.safeGet<Array>();
            if (lhs_array.size() != rhs_array.size())
                return false;
            for (size_t i = 0; i < lhs_array.size(); ++i)
            {
                if (!fieldsEquivalent(lhs_array[i], rhs_array[i]))
                    return false;
            }
            return true;
        }
        return lhs == rhs;
    }

    if (isNumericFieldType(lhs_type) && isNumericFieldType(rhs_type))
        return toFloat64(lhs) == toFloat64(rhs);

    return false;
}

/// Map a FunctionNames enum value to its canonical lowercase string_view.
std::string_view getFunctionName(FunctionNames fn_enum)
{
    switch (fn_enum)
    {
        case FunctionNames::COSINEDISTANCE:
            return COSINEDISTANCE_FUNCTION_NAME;
        case FunctionNames::L2DISTANCE:
            return L2DISTANCE_FUNCTION_NAME;
        case FunctionNames::HASTOKEN:
            return HASTOKEN_FUNCTION_NAME;
        case FunctionNames::CAST:
            return CAST_FUNCTION_NAME;
    }
    UNREACHABLE();
}

/// Append the canonical function name string to the output buffer.
void appendFunctionName(String & out, FunctionNames fn_enum)
{
    const auto name = getFunctionName(fn_enum);
    out.append(name.data(), name.size());
}

/// Extract the field name portion after the last '.' in a qualified name.
/// For example, "table.column" returns "column".  Returns empty string on failure.
String getFieldName(String input_name)
{
    if (input_name.empty())
        return "";
    size_t index = input_name.find_last_of('.');
    if (input_name.size() > index + 1)
        return input_name.substr(index + 1);
    return "";
}


/// Check whether a plan step's scope string matches the expected step type.
/// step_type 1/4 → "ExpressionStep", step_type 2 → "FilterStep".
bool scopeMatchesStepType(Int32 step_type, const String & scope)
{
    switch (step_type)
    {
        case 1:
        case 4:
            return scope == "ExpressionStep";
        case 2:
            return scope == "FilterStep";
        default:
            return false;
    }
}

/// Determine whether a plan-side constant candidate matches an AST-side literal position.
/// Matching criteria (all must pass):
///   1. The plan step scope must match the AST step type (Expression/Filter).
///   2. The identifier (column) name must match when the AST has one.
///   3. The enclosing function chain must be compatible.
///   4. The runtime parameter value must be equivalent to the candidate's current value.
/// Returns true if the candidate is a valid match for the given AST literal.
bool candidateMatchesAstLiteral(
    const PlanConstantCandidate & candidate,
    size_t ast_index,
    const VectorQueryPlanCache::ASTLiteralPosition & position,
    const VectorQueryParameters::NormalizedQueryResult & parameters)
{
    if (!scopeMatchesStepType(position.step_type, candidate.binding.dag_scope))
        return false;
    if (!position.identifier_name.empty() && candidate.identifier_names != position.identifier_name)
        return false;
    if (!candidate.function_names.empty())
    {
        size_t number = candidate.function_names.size();
        String function_names_str;
        for (const auto & fname : candidate.function_names)
        {
            if (!function_names_str.empty())
                function_names_str += "-";
            function_names_str += fname;
        }
        String function_names;
        for (const auto & function_name : position.function_list)
        {
            if (!function_names.empty())
                function_names += "-";
            function_names += function_name;
        }
        if (function_names_str != function_names)
        {
            if (number >= position.function_list.size())
                return false;
            for (size_t i = number; i > 0; i--)
            {
                if (position.function_list[i - 1] != candidate.function_names[i - 1])
                    return false;
            }
        }
    }
    if (ast_index < parameters.parsed_params.size())
    {
        if (parameters.parsed_params[ast_index].getType() == Field::Types::String &&
            position.field_type == Field::Types::Array)
        {
            if (candidate.value.getType() != Field::Types::String && candidate.binding.target_type)
            {
                Field converted;
                const auto & raw_text = parameters.parsed_params[ast_index].safeGet<String>();
                if (stringToNumericArrayField(raw_text, candidate.binding.target_type, converted))
                    return fieldsEquivalent(converted, candidate.value);
                return false;
            }
            return false;
        }
        if (candidate.value.getType() == Field::Types::Tuple)
        {
            LOG_DEBUG(logger, "not support tuple type");
            return false;
        }
        return fieldsEquivalent(parameters.parsed_params[ast_index], candidate.value);
    }
    return false;
}

/// Walk an ActionsDAG from its output nodes and collect all COLUMN (constant) nodes
/// that are children of recognized cacheable functions.  Each constant becomes a
/// PlanConstantCandidate with its DAG node index, parent function chain, identifier name,
/// and current value.  When `only_vector` is true, only constants inside vector search
/// functions are collected.  Shared COLUMN nodes (used by multiple parent functions) are
/// automatically split so each parent gets its own independent constant node.
void findActionsDAGAndCollectConstants(
    ActionsDAG & dag,
    const std::vector<UInt32> & plan_node_path,
    const String & dag_scope,
    Int32 step_type,
    std::vector<PlanConstantCandidate> & out_candidates,
    bool only_vector)
{
    if (dag.getOutputs().empty())
        return;
    // Helper to check if a function is a comparison operator
    auto is_comparison_function = [](const String & func_name) -> bool
    {
        static const std::unordered_set<String> comparison_ops = {
            "equals", "notEquals", "less", "greater",
            "lessOrEquals", "greaterOrEquals",
            "like", "notLike", "in", "notIn"
        };
        return comparison_ops.contains(func_name);
    };

    String current_field_name;
    bool should_clear_and_return = false;
    std::unordered_map<const ActionsDAG::Node *, size_t> map;
    for (const auto & node : dag.getNodes())
    {
        size_t idx = map.size();
        map[&node] = idx;
    }
    // Recursive traversal function starting from a node
    std::function<void(const ActionsDAG::Node *, const ActionsDAG::Node *, std::vector<String>&)> traverse_node;
    traverse_node = [&](const ActionsDAG::Node * node, const ActionsDAG::Node * parent_node, std::vector<String>& function_names)
    {
        if (!node || should_clear_and_return)
            return;
        auto map_it = map.find(node);
        if (map_it == map.end())
            return;
        // Process based on node type
        switch (node->type)
        {
            case ActionsDAG::ActionType::FUNCTION:
            {
                // Push function name to the list
                if (node->function_base)
                {
                    String func_name = Poco::toLower(node->function_base->getName());
                    if (checkFunctionName(func_name, only_vector))
                    {
                        function_names.push_back(func_name);
                        // Recursively traverse children
                        for (const auto * child : node->children)
                            traverse_node(child, node, function_names);
                        function_names.pop_back();
                    }
                }
                break;
            }

            case ActionsDAG::ActionType::INPUT:
            {
                // Save current field name
                current_field_name = node->result_name;
                break;
            }

            case ActionsDAG::ActionType::COLUMN:
            {
                // Check if this is a constant column
                if (node->column && isColumnConst(*node->column))
                {
                    const auto * column_const = typeid_cast<const ColumnConst *>(node->column.get());
                    if (!column_const)
                        break;

                    const Field value = column_const->getField();
                    int column_const_number = 0;
                    int input_number = 0;
                    size_t function_size = function_names.size();
                    String last_function_name;
                    if (function_size >= 1)
                        last_function_name = function_names[function_size - 1];
                    current_field_name = " ";
                    DataTypePtr result_type;
                    if (parent_node)
                    {
                        for (const auto * child : parent_node->children)
                        {
                            switch (child->type)
                            {
                                case ActionsDAG::ActionType::INPUT:
                                    input_number++;
                                    current_field_name = getFieldName(child->result_name);
                                    result_type = child->result_type;
                                    break;
                                case ActionsDAG::ActionType::COLUMN:
                                    column_const_number++;
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (input_number > 1 || (column_const_number > 1 && is_comparison_function(last_function_name)))
                        {
                            // Set clear flag and return immediately
                            should_clear_and_return = true;
                            return;
                        }
                        PlanConstantCandidate candidate;
                        candidate.binding.plan_node_path = plan_node_path;
                        candidate.binding.parameter_index = 0;
                        candidate.binding.dag_scope = dag_scope;
                        candidate.binding.dag_node_index = static_cast<UInt32>(map[node]);
                        candidate.binding.parent_function_node_index = parent_node ? static_cast<UInt32>(map[parent_node]) : std::numeric_limits<UInt32>::max();
                        candidate.binding.value_text = applyVisitor(FieldVisitorToString(), value);
                        candidate.binding.field_type = static_cast<Int32>(value.getType());
                        candidate.binding.target_type = result_type;
                        candidate.value = value;
                        candidate.step_type = step_type;
                        candidate.function_names = function_names;
                        candidate.identifier_names = current_field_name;
                        out_candidates.push_back(candidate);
                    }
                }
                break;
            }
            default:
                break;
        }
    };
    if (dag_scope == "ExpressionStep" || dag_scope == "FilterStep")
    {
        for (const auto * node : dag.getOutputs())
        {
            std::vector<String> function_names;
            traverse_node(node, nullptr, function_names);

            if (should_clear_and_return)
                out_candidates.clear();
        }
    }

    std::unordered_map<UInt32, std::vector<size_t>> node_index_to_candidate_indices;
    for (size_t i = 0; i < out_candidates.size(); ++i)
        node_index_to_candidate_indices[out_candidates[i].binding.dag_node_index].push_back(i);

    for (auto & [dag_node_index, candidate_indices] : node_index_to_candidate_indices)
    {
        if (candidate_indices.size() <= 1)
            continue;

        LOG_DEBUG(logger, "Detected shared COLUMN node {} with {} candidates, splitting", dag_node_index, candidate_indices.size());

        auto node_it = dag.getNodes().begin();
        std::advance(node_it, std::min<size_t>(dag_node_index, dag.getNodes().size()));
        if (node_it == dag.getNodes().end())
            continue;

        const ActionsDAG::Node & original_node = *node_it;

        std::unordered_map<UInt32, ActionsDAG::Node *> func_index_to_node;
        {
            size_t idx = 0;
            for (auto it = dag.getNodes().begin(); it != dag.getNodes().end(); ++it, ++idx)
            {
                auto & dag_node = const_cast<ActionsDAG::Node &>(*it);
                if (dag_node.type != ActionsDAG::ActionType::FUNCTION)
                    continue;
                for (const auto * child : dag_node.children)
                {
                    if (child == &original_node)
                    {
                        func_index_to_node[static_cast<UInt32>(idx)] = &dag_node;
                        break;
                    }
                }
            }
        }

        for (size_t ci = 1; ci < candidate_indices.size(); ++ci)
        {
            size_t candidate_idx = candidate_indices[ci];
            auto & candidate = out_candidates[candidate_idx];
            UInt32 parent_func_idx = candidate.binding.parent_function_node_index;

            auto func_it = func_index_to_node.find(parent_func_idx);
            if (func_it == func_index_to_node.end())
            {
                LOG_DEBUG(logger, "Cannot find parent FUNCTION node {} for candidate[{}], skipping", parent_func_idx, candidate_idx);
                continue;
            }

            ActionsDAG::Node * parent_func = func_it->second;

            const auto * col_const = typeid_cast<const ColumnConst *>(original_node.column.get());
            if (!col_const)
                continue;

            const ActionsDAG::Node & added_node = dag.addColumn(
                original_node.column, original_node.result_type,
                original_node.result_name + "_" + std::to_string(ci),
                original_node.is_deterministic_constant);

            auto & mutable_children = parent_func->children;
            for (auto & child : mutable_children)
            {
                if (child == &original_node)
                {
                    child = &added_node;
                    break;
                }
            }

            size_t new_node_index = dag.getNodes().size() - 1;
            candidate.binding.dag_node_index = static_cast<UInt32>(new_node_index);

            LOG_DEBUG(logger, "Split shared COLUMN node: candidate[{}] now points to new node {} (value='{}'), parent FUNCTION node {} updated",
                candidate_idx, new_node_index, candidate.binding.value_text, parent_func_idx);
        }
    }
}

namespace
{
/// Check if the SQL text starts with the SELECT keyword.
/// Skips non-significant tokens (comments, whitespace) before checking.
/// Non-SELECT queries (INSERT, CREATE, etc.) are not eligible for vector plan caching.
bool isSelectStatement(Lexer pre_lexer)
{
    Token first = pre_lexer.nextToken();
    // If there are not Significant characters before SELECT, they need to be filtered out before judgment.
    while (!first.isSignificant())
        first = pre_lexer.nextToken();
    if (first.isEnd() || first.isError())
        return false;
    if (first.type != TokenType::BareWord || first.size() != 6)
        return false;

    const char * word = first.begin;
    return equalsCaseInsensitive(word[0], 's')
        && equalsCaseInsensitive(word[1], 'e')
        && equalsCaseInsensitive(word[2], 'l')
        && equalsCaseInsensitive(word[3], 'e')
        && equalsCaseInsensitive(word[4], 'c')
        && equalsCaseInsensitive(word[5], 't');
}

/// Check if a lexer token matches a given bare word name (case-insensitive).
/// The token and bare_word_name must have the same length for an exact match.
/// Used to recognize SQL keywords (SELECT, FROM, WHERE) and function names
/// (l2distance, cosinedistance, hastoken, cast) during tokenization.
bool tokenMatchesBareWord(Token token, std::string_view bare_word_name)
{
    if (token.size() != bare_word_name.size())
        return false;
    const char * word = token.begin;
    const char * name = bare_word_name.data();
    for (size_t i = 0; i < token.size(); i++)
    {
        if (!equalsCaseInsensitive(word[i], name[i]))
            return false;
    }
    return true;
}

std::optional<Field> tokenToSettingField(Token token)
{
    if (token.type == TokenType::StringLiteral)
    {
        Field value;
        parseStringLiteral(std::string_view(token.begin, token.size()), value);
        return value;
    }
    if (token.type == TokenType::Number || token.type == TokenType::BareWord)
        return Field(String(token.begin, token.size()));
    return std::nullopt;
}

/// Core parsing routine shared by the AST and QueryPlan paths.
/// Iterates over `parameters.params`, converts each raw string token into a typed Field
/// using the provided type hints (`target_types`, `literal_types`), and populates
/// `parameters.parsed_params`.  String literals are parsed via parseStringLiteral(),
/// numeric arrays via stringToNumericArrayField(), and plain numbers via parseNumberLiteralFast().
/// Returns true if at least one parameter was successfully parsed.
bool parseNormalizedParams(
    VectorQueryParameters::NormalizedQueryResult & parameters,
    const std::vector<DataTypePtr> & target_types,
    const std::vector<Int32> & literal_types,
    bool only_vector)
{
    parameters.parsed_params.clear();
    parameters.parsed_params.reserve(parameters.params.size());
    for (size_t i = 0; i < parameters.params.size(); ++i)
    {
        const auto & raw = parameters.params[i].original_string;
        const auto type = literal_types[i] >= 0
            ? static_cast<Field::Types::Which>(literal_types[i])
            : Field::Types::String;
        Field converted;
        bool parsed = false;

        if (type == Field::Types::String)
        {
            try
            {
                parsed = parseStringLiteral(raw, converted);
            }
            catch (...)
            {
                parsed = false;
                LOG_TRACE(logger, "parse string error:{},raw={},size={}", getCurrentExceptionMessage(false), raw, raw.size());
            }
        }
        else if (type == Field::Types::Array)
        {
            if (raw.size() > 2 && ((raw.front() == '\'' && raw.back() == '\'') || (raw.front() == '"' && raw.back() == '"')))
            {
                try
                {
                    parsed = parseStringLiteral(raw, converted);
                }
                catch (...)
                {
                    parsed = false;
                    LOG_TRACE(logger, "parse string error:{},raw={},size={}", getCurrentExceptionMessage(false), raw, raw.size());
                }
            }
            else
                parsed = stringToNumericArrayField(raw, target_types[i], converted);
        }
        else if (!only_vector)
            parsed = parseNumberLiteralFast(raw, converted);

        if (!parsed)
            converted = raw;

        parameters.parsed_params.push_back(std::move(converted));
    }

    return !parameters.parsed_params.empty();
}
}

}

VectorQueryParameters::LightParseResult VectorQueryParameters::parseVectorSettingsFromQuery(
    const char * begin,
    const char * end) const
{
    VectorQueryParameters::LightParseResult result;
    if (!begin || !end || begin >= end)
        return result;

    Lexer lexer(begin, end);
    if (!isSelectStatement(lexer))
        return result;
    result.is_select = true;
    bool is_from = false;
    bool start_system_table_check = false;
    bool is_settings = false;
    std::string_view setting_name;
    bool setting_has_name = false;
    bool setting_expect_value = false;

    while (true)
    {
        Token token = lexer.nextToken();
        if (token.isEnd() || token.isError() || token.type == TokenType::Semicolon)
            break;
        if (!token.isSignificant())
            continue;

        if (start_system_table_check)
        {
            start_system_table_check = false;
            if (token.type == TokenType::BareWord && token.size() == 6 && tokenMatchesBareWord(token, "system"))
            {
                result.is_select = false;
                LOG_DEBUG(logger, "not support system table, sql({})", std::string(begin, end));
                return result;
            }
        }

        if (token.type == TokenType::BareWord && token.size() == 4 && tokenMatchesBareWord(token, "from"))
        {
            is_from = true;
            start_system_table_check = true;
        }
        if (is_from && token.type == TokenType::BareWord && token.size() == 6 && tokenMatchesBareWord(token, "select"))
        {
            result.is_select = false;
            LOG_DEBUG(logger, "not support subquery, sql({})", std::string(begin, end));
            return result;
        }
        if (is_settings)
        {
            if (!setting_expect_value && token.type == TokenType::BareWord && token.size() == 6 && tokenMatchesBareWord(token, "format"))
            {
                if (setting_has_name && !setting_expect_value)
                    result.changes.setSetting(setting_name, Settings::castValueUtil(setting_name, Field(true)));

                setting_name = {};
                setting_has_name = false;
                setting_expect_value = false;
                is_settings = false;
                continue;
            }

            if (token.type == TokenType::Comma)
            {
                if (setting_has_name && !setting_expect_value)
                    result.changes.setSetting(setting_name, Settings::castValueUtil(setting_name, Field(true)));

                setting_name = {};
                setting_has_name = false;
                setting_expect_value = false;
                continue;
            }

            if (!setting_has_name)
            {
                if (token.type == TokenType::BareWord)
                {
                    setting_name = std::string_view(token.begin, token.size());
                    setting_has_name = true;
                    setting_expect_value = false;
                }
                continue;
            }

            if (!setting_expect_value && token.type == TokenType::Equals)
            {
                setting_expect_value = true;
                continue;
            }

            if (setting_expect_value)
            {
                if (token.type == TokenType::BareWord && token.size() == 7 && tokenMatchesBareWord(token, "default"))
                {
                    Settings default_settings;
                    result.changes.setSetting(setting_name, default_settings.get(setting_name));
                }
                else if (auto value = tokenToSettingField(token))
                    result.changes.setSetting(setting_name, Settings::castValueUtil(setting_name, *value));

                setting_name = {};
                setting_has_name = false;
                setting_expect_value = false;
                continue;
            }

            setting_name = {};
            setting_has_name = false;
            setting_expect_value = false;
            continue;
        }
        if (is_from && token.type == TokenType::BareWord && token.size() == 8 && tokenMatchesBareWord(token, "settings"))
            is_settings = true;
    }

    if (is_settings && setting_has_name && !setting_expect_value)
        result.changes.setSetting(setting_name, Settings::castValueUtil(setting_name, Field(true)));

    return result;
}

/// Tokenize the raw SQL text and produce a normalized cache key plus extracted parameters.
///
/// This function does two jobs at once:
/// 1. Build a cache-key-friendly SQL template where replaceable literals collapse to '?'.
/// 2. Preserve the original literal text in `params` so cache hits can rebuild
///    AST / QueryPlan snapshots with the current runtime values.
///
/// The lexer recognizes vector search function boundaries (l2distance, cosinedistance,
/// hastoken) and handles special cases:
///   - Vector array literals are kept as-is (not normalized) unless `use_cast` is set.
///   - The LIMIT keyword stops parameter collection (LIMIT values are plan-step bindings).
///   - POSITION(x IN y) parameters are reordered to match the canonical AST order.
///   - DATE_PART's first string argument (field name) is skipped.
///   - Negative number literals (preceded by '-') are collected as single tokens.
///   - SYSTEM table queries are rejected (not cacheable).
///
/// Returns a NormalizedQueryResult with hash=0 if the query is not eligible for caching.
VectorQueryParameters::NormalizedQueryResult VectorQueryParameters::normalizeQueryAndExtractParams(
    const char * begin,
    const char * end,
    bool only_vector,
    bool use_cast,
    bool enable_vector_performance_test)
{
    NormalizedQueryResult result;
    SipHash hash;
    Lexer lexer(begin, end);
    if (!enable_vector_performance_test)
    {
        if (!isSelectStatement(lexer))
        {
            result.hash = 0;
            result.normalized_sql = "";
            LOG_DEBUG(logger, "sql({}) has not begin with select", std::string(begin, end));
            return result;
        }
    }

    size_t num_literals_in_sequence = 0;
    bool parse_params = true;
    bool is_cast = false;
    UInt32 vector_function_type = 0;
    bool is_comma = false;
    bool vector_complete = false;
    // Track POSITION function context for parameter reordering
    bool in_position_function = false;
    bool position_saw_in_keyword = false;
    size_t position_param_count = 0;
    size_t params_before_position = 0;
    // Track DATE_PART function context - skip first string parameter
    bool in_date_part_function = false;
    bool date_part_saw_first_string = false;
    bool is_bare_word = false;
    bool is_dot = false;
    bool is_negative = false;
    bool is_from = false;
    bool start_system_table_check = false;
    bool previous_is_value = false;

    while (true)
    {
        Token token = lexer.nextToken();
        if (token.type == TokenType::Semicolon)
        {
            hash.update(token.begin, token.size());
            result.normalized_sql += std::string(token.begin, token.size());
            result.new_sql += std::string(token.begin, token.size());
            break;
        }
        if (token.type == TokenType::Whitespace)
        {
            hash.update(token.begin, token.size());
            result.normalized_sql += std::string(token.begin, token.size());
            result.new_sql += std::string(token.begin, token.size());
            continue;
        }
        if (token.type == TokenType::Comment)
        {
            result.new_sql += std::string(token.begin, token.size());
            continue;
        }
        if (token.isEnd() || token.isError())
            break;
        if (token.type == TokenType::BareWord && !tokenCanCache(token))
        {
            result.hash = 0;
            result.normalized_sql = "";
            result.params.clear();
            LOG_DEBUG(logger, "sql({}) has some not support function", std::string(begin, end));
            return result;
        }
        if (vector_function_type && token.type == TokenType::BareWord)
            is_bare_word = true;
        if (token.type == TokenType::BareWord && !vector_function_type
            && (token.size() == 8 || token.size() == 10 || token.size() == 14))
        {
            if (tokenMatchesBareWord(token, getFunctionName(FunctionNames::L2DISTANCE)))
                vector_function_type =  1;
            else if (tokenMatchesBareWord(token, getFunctionName(FunctionNames::HASTOKEN)))
                vector_function_type =  2;
            else if (tokenMatchesBareWord(token, getFunctionName(FunctionNames::COSINEDISTANCE)))
                vector_function_type =  3;
            if (vector_function_type)
                is_bare_word = false;
        }
        if (vector_function_type && is_bare_word && token.type == TokenType::Comma)
        {
            is_comma = true;
            num_literals_in_sequence = 0;
        }
        if (is_comma && (vector_function_type == 1 || vector_function_type == 3) &&
            token.type == TokenType::BareWord && token.size() == 4 &&
            tokenMatchesBareWord(token, getFunctionName(FunctionNames::CAST)))
        {
            is_cast = true;
            vector_complete = true;
            hash.update(CAST_FUNCTION_NAME.data(), CAST_FUNCTION_NAME.size());
            appendFunctionName(result.normalized_sql, FunctionNames::CAST);
            appendFunctionName(result.new_sql, FunctionNames::CAST);
            continue;
        }
        if (vector_function_type && is_comma && token.type == TokenType::ClosingRoundBracket)
        {
            vector_complete = true;
            vector_function_type = 0;
            is_comma = false;
            is_bare_word = false;
        }
        if (is_comma && token.type == TokenType::StringLiteral &&
                (vector_function_type == 2 ||
                    (is_cast &&
                        (vector_function_type == 1 || vector_function_type == 3)
                    )
                )
            )
        {
            result.normalized_sql += "?:string";
            hash.update("\x00", 1);
            if (vector_complete || vector_function_type == 2)
                result.params.emplace_back(String(token.begin, token.size()), ParameterInfo::Type::STRING);
            result.new_sql += std::string(token.begin, token.size());
            vector_complete = false;
            continue;
        }
        /// -------- literal --------
        if (token.type == TokenType::OpeningSquareBracket)
        {
            const char * array_begin = token.begin;
            const char * array_end = token.end;
            size_t depth = 1;
            size_t array_depth = 1;
            Token last_significant = token;
            bool valid = true;
            bool is_function = false;

            std::vector<String> string_array;
            const char * element_start = nullptr;

            while (depth > 0)
            {
                Token nested = lexer.nextToken();
                array_end = nested.end;

                if (nested.isEnd() || nested.isError())
                {
                    valid = false;
                    break;
                }

                if (nested.type == TokenType::BareWord)
                    is_function = true;

                if (!nested.isSignificant())
                    continue;

                if (nested.type == TokenType::OpeningSquareBracket)
                {
                    ++array_depth;
                    ++depth;
                }
                else if (nested.type == TokenType::ClosingSquareBracket)
                    --depth;

                last_significant = nested;
            }
            if (use_cast && array_depth == 1  && !is_function && (vector_function_type == 1 || vector_function_type == 3))
            {
                appendFunctionName(result.new_sql, FunctionNames::CAST);
                result.new_sql += "('";
                appendFunctionName(result.normalized_sql, FunctionNames::CAST);
                result.normalized_sql += "(";
            }

            // Handle the last element after the loop ends
            if (element_start && array_end > element_start)
            {
                // Find the position before the closing bracket
                const char * last_element_end = array_end;
                if (last_significant.type == TokenType::ClosingSquareBracket)
                    last_element_end = last_significant.begin;

                if (last_element_end > element_start)
                {
                    String element(element_start, last_element_end - element_start);
                    // Trim whitespace
                    size_t start_pos = 0;
                    size_t end_pos = element.length();
                    while (start_pos < end_pos && isspace(static_cast<unsigned char>(element[start_pos])))
                        ++start_pos;
                    while (end_pos > start_pos && isspace(static_cast<unsigned char>(element[end_pos - 1])))
                        --end_pos;
                    if (start_pos < end_pos)
                        string_array.emplace_back(element.data() + start_pos, end_pos - start_pos);
                }
            }

            if (valid && depth == 0)
            {
                String original_array(array_begin, static_cast<size_t>(array_end - array_begin));
                result.new_sql += original_array;
                if (array_depth == 1  && !is_function && (vector_function_type == 1 || vector_function_type == 3))
                    result.normalized_sql += "?:array";
                else
                    result.normalized_sql += original_array;
                result.params.emplace_back(original_array, ParameterInfo::Type::NUMERIC_VECTOR);

                vector_complete = true;
                if (num_literals_in_sequence == 0 && (vector_function_type == 1 || vector_function_type == 3))
                    hash.update("\x00", 1);

                ++num_literals_in_sequence;

                if (use_cast && array_depth == 1  && !is_function && (vector_function_type == 1 || vector_function_type == 3))
                {
                    result.new_sql += "','Array(Float)')";
                    result.normalized_sql += ",?:string)";
                }
                continue;
            }
        }
        // add a check for system table queries to prevent caching
        if (!enable_vector_performance_test)
        {
            if (start_system_table_check)
            {
                start_system_table_check = false;
                if (token.type == TokenType::BareWord && token.size() == 6 && tokenMatchesBareWord(token, "system"))
                {
                    result.hash = 0;
                    result.normalized_sql = "";
                    result.params.clear();
                    LOG_DEBUG(logger, "not support system table, sql({})", std::string(begin, end));
                    return result;
                }
            }
            if (token.type == TokenType::BareWord && token.size() == 4 && tokenMatchesBareWord(token, "from"))
            {
                is_from = true;
                start_system_table_check = true;
            }
        }
        if (token.type == TokenType::Dot)
            is_dot = true;
        else if (!only_vector)
        {
            if (token.type == TokenType::BareWord && token.size() == 5 && tokenMatchesBareWord(token, "limit"))
                parse_params = false;
            if (token.type == TokenType::BareWord && token.size() == 8 && tokenMatchesBareWord(token, "settings"))
                parse_params = false;
            // Detect POSITION function start
            if (token.type == TokenType::BareWord && token.size() == 8 && tokenMatchesBareWord(token, "position"))
            {
                in_position_function = true;
                position_saw_in_keyword = false;
                position_param_count = 0;
                params_before_position = result.params.size();
            }
            // Detect IN keyword inside POSITION function
            if (in_position_function && token.type == TokenType::BareWord && token.size() == 2 && tokenMatchesBareWord(token, "in"))
            {
                position_saw_in_keyword = true;
            }
            // Detect DATE_PART function start
            if (token.type == TokenType::BareWord && token.size() == 9 && tokenMatchesBareWord(token, "date_part"))
            {
                in_date_part_function = true;
                date_part_saw_first_string = false;
            }
            // add parsing support for the negative sign '-'.
            if (token.type == TokenType::Minus)
            {
                if (previous_is_value)
                    is_negative = false;
                else
                    is_negative = true;
            }
            else if (is_negative && token.type != TokenType::Number)
                is_negative = false;
            if (!is_negative && tokenIsValue(token))
                previous_is_value = true;
            else
                previous_is_value = false;
            if (parse_params && !vector_function_type && !is_dot && (token.type == TokenType::Number
                    || token.type == TokenType::StringLiteral
                    || token.type == TokenType::HereDoc)
                )
            {
                // Skip first string parameter in date_part function
                if (in_date_part_function && token.type == TokenType::StringLiteral && !date_part_saw_first_string)
                {
                    date_part_saw_first_string = true;
                    hash.update(token.begin, token.size());
                    result.normalized_sql += std::string(token.begin, token.size());
                    result.new_sql += std::string(token.begin, token.size());
                    is_dot = false;
                    continue;
                }
                ParameterInfo::Type param_type = ParameterInfo::Type::STRING;
                if (token.type == TokenType::Number)
                    param_type = ParameterInfo::Type::NUMERIC;

                String params_value = String(token.begin, token.size());
                result.new_sql += params_value;
                if (is_negative && token.type == TokenType::Number)
                {
                    params_value = "-" + params_value;
                    is_negative = false;
                }
                result.params.emplace_back(params_value, param_type);
                // Track parameters inside POSITION function
                if (in_position_function)
                {
                    position_param_count++;
                }

                result.normalized_sql += "?:";
                if (param_type == ParameterInfo::Type::NUMERIC)
                    result.normalized_sql += "number";
                else
                    result.normalized_sql += "string";
                hash.update("\x00", 1);
                is_dot = false;
                continue;
            }
            is_dot = false;

            // Check if we're exiting POSITION function (closing bracket)
            if (in_position_function && token.type == TokenType::ClosingRoundBracket)
            {
                // If we saw IN keyword and collected exactly 2 parameters, swap them
                if (position_saw_in_keyword && position_param_count == 2)
                {
                    // Swap the last two parameters that were collected for this POSITION call
                    size_t first_param_idx = params_before_position;
                    size_t second_param_idx = params_before_position + 1;

                    if (first_param_idx < result.params.size() && second_param_idx < result.params.size())
                    {
                        std::swap(result.params[first_param_idx], result.params[second_param_idx]);
                    }
                }
                // Reset POSITION tracking
                in_position_function = false;
                position_saw_in_keyword = false;
                position_param_count = 0;
            }
            // Check if we're exiting DATE_PART function (closing bracket)
            if (in_date_part_function && token.type == TokenType::ClosingRoundBracket)
            {
                // Reset DATE_PART tracking
                in_date_part_function = false;
                date_part_saw_first_string = false;
            }
        }
        hash.update(token.begin, token.size());
        result.normalized_sql += std::string(token.begin, token.size());
        result.new_sql += std::string(token.begin, token.size());
    }
    if (!enable_vector_performance_test)
    {
        if (!is_from)
        {
            result.hash = 0;
            result.normalized_sql = "";
            result.params.clear();
            LOG_DEBUG(logger, "sql({}) has not from", std::string(begin, end));
            return result;
        }
    }
    result.hash = hash.get64();
    return result;
}


/// Rewrite every constant slot in the cached QueryPlan with the current runtime values.
///
/// Each binding in `plan_constant_bindings` points to a specific COLUMN node inside an
/// ActionsDAG (identified by plan_node_path + dag_node_index).  The method replaces that
/// node's const column with a new ColumnConst holding the parsed runtime Field value.
///
/// The replacement process:
///   1. Navigate to the plan node by its path from the root.
///   2. Identify the step type (FilterStep or ExpressionStep).
///   3. Locate the ActionsDAG node by index.
///   4. Convert the runtime value to the appropriate type.
///   5. Replace the COLUMN node's const column payload.
///
/// Returns false if any replacement fails (type mismatch, missing node, etc.).
bool VectorQueryParameters::replaceConstantsInQueryPlan(
    QueryPlan & plan,
    NormalizedQueryResult & parameters,
    const std::vector<VectorQueryPlanCache::PlanConstantBinding> & plan_constant_bindings)
{
    if (plan_constant_bindings.empty() || parameters.parsed_params.empty())
        return false;
    auto * root = plan.getRootNode();
    if (!root)
        return false;

    /// Lambda to navigate the plan tree by a sequence of child indices.
    auto get_node_by_path = [&](const NodePath & path) -> QueryPlan::Node *
    {
        QueryPlan::Node * current = root;
        for (const auto index : path)
        {
            if (!current || index >= current->children.size())
                return nullptr;
            current = current->children[index];
        }
        return current;
    };

    /// Lambda to replace a single COLUMN node in an ActionsDAG with a new constant value.
    /// Handles type promotion via getType() and array string-to-field conversion.
    auto apply_bindings_to_dag = [&](ActionsDAG & dag, const UInt32 dag_node_index, const UInt32 parameter_index, DataTypePtr data_type_ptr)
    {
        if (parameter_index >= parameters.parsed_params.size())
            return false;
        auto node_it = dag.getNodes().begin();
        std::advance(node_it, std::min<size_t>(dag_node_index, dag.getNodes().size()));
        if (node_it == dag.getNodes().end())
            return false;

        auto & dag_node = const_cast<ActionsDAG::Node &>(*node_it);
        if (dag_node.type != ActionsDAG::ActionType::COLUMN || !dag_node.column || !isColumnConst(*dag_node.column) || !dag_node.result_type)
            return false;
        try
        {
            Field raw_value = parameters.parsed_params[parameter_index];
            if (raw_value.getType() == Field::Types::String && isArray(dag_node.result_type))
            {
                Field converted;
                const auto & raw_text = raw_value.safeGet<String>();
                if (stringToNumericArrayField(raw_text, dag_node.result_type, converted))
                    raw_value = std::move(converted);
            }
            DataTypePtr final_type = getType(data_type_ptr, dag_node.result_type);
            if (final_type->getTypeId() != static_cast<TypeIndex>(raw_value.getType()))
                raw_value = convertFieldToType(raw_value, *final_type);
            // ActionsDAG constants are rewritten by replacing the const column payload
            // at the recorded node index with the parsed runtime Field value.
            ColumnConstPtr new_column = final_type->createColumnConst(1, raw_value);
            const_cast<DataTypePtr &>(dag_node.result_type) = std::move(final_type);
            dag_node.column = std::move(new_column);
            return true;
        }
        catch (...)
        {
            LOG_DEBUG(logger, "Exception caught when updating DAG node column: {}", getCurrentExceptionMessage(false));
            return false;
        }
    };

    // Each binding points to one mutable constant slot inside the cached plan.
    // Rewriting all bindings restores the plan to the runtime literal values of
    // the current execution without rebuilding planner output from scratch.
    //
    // Note that bindings are intentionally heterogeneous: the same ordered runtime
    // parameter vector may feed a DAG node.
    for (const auto & plan_constant_binding : plan_constant_bindings)
    {
        auto * node = get_node_by_path(plan_constant_binding.plan_node_path);
        if (!node || !node->step)
        {
            continue;
        }

        if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
        {
            if (plan_constant_binding.dag_scope != "FilterStep")
                continue;
            ActionsDAG & dag = filter_step->getExpression();
            if (!apply_bindings_to_dag(dag, plan_constant_binding.dag_node_index, plan_constant_binding.parameter_index, plan_constant_binding.target_type))
                return false;
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            if (plan_constant_binding.dag_scope != "ExpressionStep")
                continue;
            ActionsDAG & dag = expression_step->getExpression();
            if (!apply_bindings_to_dag(dag, plan_constant_binding.dag_node_index, plan_constant_binding.parameter_index, plan_constant_binding.target_type))
                return false;
        }
    }
    return true;
}

/// Walk the AST and collect positions of all cacheable literal nodes.
///
/// Each collected position records:
///   - The AST path (sequence of child indices from the root)
///   - The enclosing function chain (e.g. ["cosinedistance", "cast"])
///   - The identifier (column) name from the parent function's arguments
///   - The step type (1=Expression, 2=Filter, 4=VectorExpression)
///   - The literal's target data type
///   - A unique path name for deduplication
///
/// LIMIT/OFFSET literals are excluded (they use plan-step bindings instead).
/// The function returns an empty vector if any unsupported pattern is detected
/// (tuple literals, modulo function, duplicate path names, etc.).
/// When `only_vector` is true, only literals inside vector search functions are collected.
std::vector<VectorQueryPlanCache::ASTLiteralPosition> VectorQueryParameters::collectASTLiteralPositions(
    const ASTPtr & query_ast,
    bool only_vector) const
{
    std::vector<VectorQueryPlanCache::ASTLiteralPosition> positions;
    if (!query_ast)
        return positions;
    std::unordered_set<std::string> unique_strings;

    /// Lambda to skip LIMIT/OFFSET children — they are handled by plan-step bindings.
    auto should_skip_limit_child = [](const ASTPtr & parent, const ASTPtr & child)
    {
        const auto * select = parent ? parent->as<ASTSelectQuery>() : nullptr;
        if (!select || !child)
            return false;

        // LIMIT / OFFSET are restored through plan-step bindings instead of the
        // generic AST literal-position list. Excluding them here keeps the main
        // positional literal order focused on semantic constants used by filters,
        // vector functions, and other expressions that survive plan reuse.
        return child == select->limitLength()
            || child == select->limitOffset()
            || child == select->limitByLength()
            || child == select->limitByOffset();
    };

    bool can_cache = true;

    auto get_node_name = [&](const ASTPtr & child) -> String
    {
        if (!child)
            return "null";
        if (child->as<ASTLiteral>())
            return "";
        // Fallback to generic getID() which returns type name
        return child->getID('-');
    };

    bool is_vector = true;

    std::function<void(const ASTPtr &, std::vector<ASTPtr> &, size_t, NodePath &, std::vector<String> &, String)> collect
        = [&](const ASTPtr & ast, std::vector<ASTPtr> & parent_list, size_t depth, NodePath & path,
        std::vector<String> & function_list, String ast_path_name)
    {
        if (!ast || !can_cache)
            return;
        parent_list.push_back(ast);
        size_t function_size = function_list.size();
        String last_function_name;
        bool is_cast = false;
        if (function_size >= 1)
            last_function_name = function_list[function_size - 1];
        String last_second_function_name;
        if (function_size >= 2)
            last_second_function_name = function_list[function_size - 2];
        if (const auto * literal_node = ast->as<ASTLiteral>())
        {
            // add checks for the tuple function, module function
            // (because tuple handling and replacement are not yet supported; the module function cannot be processed because '%' and '/' cannot be generalized for constant replacement due to type issues)
            if (last_function_name == "tuple" || last_function_name == "modulo")
            {
                LOG_DEBUG(logger, "do not support {} Function", last_function_name);
                can_cache = false;
                return;
            }
            const auto type = literal_node->value.getType();
            const auto target_type = applyVisitor(FieldToDataType(), literal_node->value);
            // add checks for the tuple constants
            if (static_cast<Int32>(type) == FieldRef::Types::Tuple)
            {
                LOG_DEBUG(logger, "do not support Tuple Literal");
                can_cache = false;
                return;
            }
            std::vector<String> ident_name_list;
            VectorQueryPlanCache::ASTLiteralPosition pos;
            pos.identifier_name = "";
            pos.field_type = static_cast<Int32>(type);
            int literal_number = 0;

            size_t parent_size = parent_list.size();
            int parent_index = static_cast<int>(parent_size) - 2;
            if (last_function_name == getFunctionName(FunctionNames::CAST) &&
                    (last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE)
                    || last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                )
            {
                is_cast = true;
                parent_index = parent_index - 2;
            }
            if (parent_index < 0 || static_cast<size_t>(parent_index) >= parent_list.size())
            {
                LOG_DEBUG(logger, "Prevent out-of-bounds access");
                parent_list.pop_back();
                return; // Prevent out-of-bounds access
            }
            auto parent = parent_list[parent_index];
            for (size_t i = 0; i < parent->children.size(); ++i)
            {
                if (parent->children[i]->as<ASTLiteral>())
                    literal_number++;
                if (const auto * ident_node = parent->children[i]->as<ASTIdentifier>())
                    ident_name_list.push_back(ident_node->name());
            }
            parent_list.pop_back();
            if (last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
            {
                if (static_cast<Int32>(type) != FieldRef::Types::Array)
                {
                    can_cache = false;
                    LOG_DEBUG(logger, "1, last_function_name = {} type = {}",
                        last_function_name, static_cast<Int32>(type));
                    return;
                }
            }
            else if (last_function_name == getFunctionName(FunctionNames::HASTOKEN))
            {
                if (static_cast<Int32>(type) != FieldRef::Types::String)
                {
                    can_cache = false;
                    LOG_DEBUG(logger, "2, last_function_name = {} type = {}",
                        last_function_name, static_cast<Int32>(type));
                    return;
                }
            }
            else if (last_function_name == getFunctionName(FunctionNames::CAST))
            {
                if (last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                {
                    pos.field_type = FieldRef::Types::Array;
                    if (is_vector)
                    {
                        is_vector = false;
                        if (static_cast<Int32>(type) != FieldRef::Types::String)
                        {
                            can_cache = false;
                            LOG_DEBUG(logger, "3, last_function_name = {} type = {}",
                                last_function_name, static_cast<Int32>(type));
                            return;
                        }
                    }
                    else
                    {
                        is_vector = true;
                        return;
                    }
                }
                else
                {
                    can_cache = false;
                    LOG_DEBUG(logger, "4, last_function_name = {} type = {}",
                            last_function_name, static_cast<Int32>(type));
                    return;
                }
            }
            else if (!only_vector)
            {
                if (literal_number == 1 && (last_function_name == "and" || last_function_name == "or"))
                {
                    LOG_DEBUG(logger, "found {} constant ", last_function_name);
                    can_cache = false;
                    return;
                }
                if (literal_number >= 2 && (last_function_name == "equals" || last_function_name == "notEquals" ||
                    last_function_name == "less" || last_function_name == "greater" ||
                    last_function_name == "lessOrEquals" || last_function_name == "greaterOrEquals" ||
                    last_function_name == "like" || last_function_name == "notLike" ||
                    last_function_name == "in" || last_function_name == "notIn"))
                {
                    LOG_DEBUG(logger, "last_function_name={} found constant op constant", last_function_name);
                    can_cache = false;
                    return;
                }
                if (!ident_name_list.empty())
                {
                    ast_path_name += "Identifier-" + ident_name_list[0];
                    pos.identifier_name = ident_name_list[0];
                }
                else
                {
                    ast_path_name += "parameter-" + toString(path[path.size() - 1]);
                    pos.identifier_name = " ";
                }
            }
            if (!ident_name_list.empty()  &&
                    (last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::HASTOKEN) || last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE) ||
                        (is_cast && last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE)) ||
                        (is_cast && last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                    )
                )
            {
                ast_path_name += "Identifier-" + ident_name_list[0];
                pos.identifier_name = ident_name_list[0];
            }
            if (!pos.identifier_name.empty())
            {
                ast_path_name += "_Literal-" + dataTypePtrToString(target_type) + "-" + applyVisitor(FieldVisitorToString(), literal_node->value);
                if (unique_strings.contains(ast_path_name))
                {
                    LOG_DEBUG(logger, "ast_path_name={} is exist", ast_path_name);
                    can_cache = false;
                    return;
                }
                unique_strings.insert(ast_path_name);
                pos.step_type = -1;
                // Determine step type from ast_path_name in a more robust way
                if (ast_path_name.length() > static_cast<size_t>(Offset::StepType))
                {
                    char step_char = ast_path_name[static_cast<size_t>(Offset::StepType)];
                    if (step_char == 'E')
                    {
                        if (last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::HASTOKEN) || last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE) ||
                            (is_cast && last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE)) ||
                            (is_cast && last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                        )
                            pos.step_type = 4;
                        else
                            pos.step_type = 1;
                    }
                    if (step_char == 'F')
                        pos.step_type = 2;
                    if (step_char == 'T')
                        pos.step_type = 3;
                }

                if (pos.step_type == 3)
                {
                    LOG_DEBUG(logger, "Join ... On found Literal");
                    can_cache = false;
                    return;
                }
                pos.path = path;
                pos.target_type = target_type;
                pos.function_list = function_list;
                pos.ast_path_name = ast_path_name;
                pos.identifier_name = getFieldName(pos.identifier_name);
                positions.push_back(pos);
            }
            return;
        }
        if (const auto * func = ast->as<ASTFunction>())
        {
            // Preserve a stable traversal order for function nodes:
            // parameters first, then regular arguments, then any remaining children.
            // The same order must be used everywhere that maps parameter index ->
            // AST literal position -> QueryPlan binding.
            String function_name = Poco::toLower(func->name);

            if (!only_vector ||
                    (only_vector &&
                        (function_name == getFunctionName(FunctionNames::COSINEDISTANCE) || function_name == getFunctionName(FunctionNames::L2DISTANCE) || function_name == getFunctionName(FunctionNames::HASTOKEN) ||
                            (function_name == getFunctionName(FunctionNames::CAST) &&
                                (last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE) || last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::HASTOKEN))
                            )
                        )
                    )
                )
            {
                function_list.push_back(function_name);
                if (function_name != getFunctionName(FunctionNames::COSINEDISTANCE) && function_name != getFunctionName(FunctionNames::L2DISTANCE))
                {
                    for (size_t i = 0; i < ast->children.size(); ++i)
                    {
                        const auto & child = ast->children[i];
                        if (child == func->parameters)
                        {
                            path.push_back(static_cast<UInt32>(i));
                            collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
                            path.pop_back();
                        }
                    }
                }
                for (size_t i = 0; i < ast->children.size(); ++i)
                {
                    const auto & child = ast->children[i];
                    if (child == func->arguments)
                    {
                        path.push_back(static_cast<UInt32>(i));
                        collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
                        path.pop_back();
                    }
                }
                for (size_t i = 0; i < ast->children.size(); ++i)
                {
                    const auto & child = ast->children[i];
                    if (child == func->parameters || child == func->arguments)
                        continue;
                    if (should_skip_limit_child(ast, child))
                        continue;
                    path.push_back(static_cast<UInt32>(i));
                    collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
                    path.pop_back();
                }
                function_list.pop_back();
                parent_list.pop_back();
                return;
            }
            parent_list.pop_back();
            return;
        }

        for (size_t i = 0; i < ast->children.size(); ++i)
        {
            const auto & child = ast->children[i];
            if (should_skip_limit_child(ast, child))
                continue;
            path.push_back(static_cast<UInt32>(i));
            collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
            path.pop_back();
        }
        parent_list.pop_back();
    };

    NodePath root_path;
    std::vector<ASTPtr> parent_list;
    std::vector<String> function_list;
    collect(query_ast, parent_list, 0, root_path, function_list, "");
    if (!can_cache)
        positions.clear();
    return positions;
}

/// Normalize an AST in-place by replacing each collectable literal with a placeholder.
///
/// This is the AST-based alternative to normalizeQueryAndExtractParams (which works on
/// raw SQL text).  It clones the AST, walks it to find cacheable literals, replaces each
/// with the sentinel string '__VEC_PLACEHOLDER__', and records the original values in
/// `query_result.parsed_params`.
///
/// Returns a NormalizedQueryResult containing:
///   - normalized_sql: the formatted AST after placeholder substitution
///   - parsed_params: the original literal values in traversal order
///   - ast_literal_position_list: the AST paths and metadata for each replaced literal
/// Returns an empty result if the AST contains unsupported patterns.
VectorQueryParameters::NormalizedQueryResult VectorQueryParameters::normalizedAST(
    const ASTPtr & query_ast,
    bool only_vector) const
{
    NormalizedQueryResult query_result;

    if (!query_ast)
        return query_result;

    Field converted;
    std::string_view raw = "'__VEC_PLACEHOLDER__'";
    try
    {
        parseStringLiteral(raw, converted);
    }
    catch (...)
    {
        LOG_DEBUG(logger, "parse string error,raw={},size={},error={}", raw, raw.size(), getCurrentExceptionMessage(false));
        return query_result;
    }

    std::vector<VectorQueryPlanCache::ASTLiteralPosition> positions;
    std::unordered_set<std::string> unique_strings;

    auto should_skip_limit_child = [](const ASTPtr & parent, const ASTPtr & child)
    {
        const auto * select = parent ? parent->as<ASTSelectQuery>() : nullptr;
        if (!select || !child)
            return false;

        // LIMIT / OFFSET are restored through plan-step bindings instead of the
        // generic AST literal-position list. Excluding them here keeps the main
        // positional literal order focused on semantic constants used by filters,
        // vector functions, and other expressions that survive plan reuse.
        return child == select->limitLength()
            || child == select->limitOffset()
            || child == select->limitByLength()
            || child == select->limitByOffset();
    };

    bool can_cache = true;

    auto get_node_name = [&](const ASTPtr & child) -> String
    {
        if (!child)
            return "null";
        if (child->as<ASTLiteral>())
            return "";
        // Fallback to generic getID() which returns type name
        return child->getID('-');
    };

    bool is_vector = true;

    std::function<void(ASTPtr &, std::vector<ASTPtr> &, size_t, NodePath &, std::vector<String> &, String)> collect
        = [&](ASTPtr & ast, std::vector<ASTPtr> & parent_list, size_t depth, NodePath & path,
        std::vector<String> & function_list, String ast_path_name)
    {
        if (!ast || !can_cache)
            return;
        parent_list.push_back(ast);
        size_t function_size = function_list.size();
        String last_function_name;
        bool is_cast = false;
        if (function_size >= 1)
            last_function_name = function_list[function_size - 1];
        String last_second_function_name;
        if (function_size >= 2)
            last_second_function_name = function_list[function_size - 2];
        if (auto * literal_node = ast->as<ASTLiteral>())
        {
            if (!functionCanCache(last_function_name))
            {
                can_cache = false;
                return;
            }
            if (last_function_name == "modulo")
            {
                LOG_DEBUG(logger, "do not support {} Function", last_function_name);
                can_cache = false;
                return;
            }
            const auto type = literal_node->value.getType();
            const auto target_type = applyVisitor(FieldToDataType(), literal_node->value);
            std::vector<String> ident_name_list;
            VectorQueryPlanCache::ASTLiteralPosition pos;
            pos.identifier_name = "";
            pos.field_type = static_cast<Int32>(type);
            int literal_number = 0;

            size_t parent_size = parent_list.size();
            int parent_index = static_cast<int>(parent_size) - 2;
            if (last_function_name == getFunctionName(FunctionNames::CAST) &&
                    (last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE)
                    || last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                )
            {
                is_cast = true;
                parent_index = parent_index - 2;
            }
            if (parent_index < 0 || static_cast<size_t>(parent_index) >= parent_list.size())
            {
                LOG_DEBUG(logger, "Prevent out-of-bounds access");
                parent_list.pop_back();
                return; // Prevent out-of-bounds access
            }
            auto parent = parent_list[parent_index];
            for (size_t i = 0; i < parent->children.size(); ++i)
            {
                if (parent->children[i]->as<ASTLiteral>())
                    literal_number++;
                if (const auto * ident_node = parent->children[i]->as<ASTIdentifier>())
                    ident_name_list.push_back(ident_node->name());
            }
            parent_list.pop_back();
            if (last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
            {
                if (static_cast<Int32>(type) != FieldRef::Types::Array)
                {
                    can_cache = false;
                    LOG_DEBUG(logger, "1, last_function_name = {} type = {}",
                        last_function_name, static_cast<Int32>(type));
                    return;
                }
            }
            else if (last_function_name == getFunctionName(FunctionNames::HASTOKEN))
            {
                if (static_cast<Int32>(type) != FieldRef::Types::String)
                {
                    can_cache = false;
                    LOG_DEBUG(logger, "2, last_function_name = {} type = {}",
                        last_function_name, static_cast<Int32>(type));
                    return;
                }
            }
            else if (last_function_name == getFunctionName(FunctionNames::CAST))
            {
                if (last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                {
                    pos.field_type = FieldRef::Types::Array;
                    if (is_vector)
                    {
                        is_vector = false;
                        if (static_cast<Int32>(type) != FieldRef::Types::String)
                        {
                            can_cache = false;
                            LOG_DEBUG(logger, "3, last_function_name = {} type = {}",
                                last_function_name, static_cast<Int32>(type));
                            return;
                        }
                    }
                    else
                    {
                        is_vector = true;
                        return;
                    }
                }
                else
                {
                    can_cache = false;
                    LOG_DEBUG(logger, "4, last_function_name = {} type = {}",
                            last_function_name, static_cast<Int32>(type));
                    return;
                }
            }
            else if (!only_vector)
            {
                if (literal_number == 1 && (last_function_name == "and" || last_function_name == "or"))
                {
                    LOG_DEBUG(logger, "found {} constant ", last_function_name);
                    can_cache = false;
                    return;
                }
                if (literal_number >= 2 && (last_function_name == "equals" || last_function_name == "notEquals" ||
                    last_function_name == "less" || last_function_name == "greater" ||
                    last_function_name == "lessOrEquals" || last_function_name == "greaterOrEquals" ||
                    last_function_name == "like" || last_function_name == "notLike" ||
                    last_function_name == "in" || last_function_name == "notIn"))
                {
                    LOG_DEBUG(logger, "last_function_name={} found constant op constant", last_function_name);
                    can_cache = false;
                    return;
                }
                if (!ident_name_list.empty())
                {
                    ast_path_name += "Identifier-" + ident_name_list[0];
                    pos.identifier_name = ident_name_list[0];
                }
                else
                {
                    ast_path_name += "parameter-" + toString(path[path.size() - 1]);
                    pos.identifier_name = " ";
                }
            }
            if (!ident_name_list.empty()  &&
                    (last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::HASTOKEN) || last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE) ||
                        (is_cast && last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE)) ||
                        (is_cast && last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                    )
                )
            {
                ast_path_name += "Identifier-" + ident_name_list[0];
                pos.identifier_name = ident_name_list[0];
            }
            if (!pos.identifier_name.empty())
            {
                ast_path_name += "_Literal-" + dataTypePtrToString(target_type) + "-" + applyVisitor(FieldVisitorToString(), literal_node->value);
                if (unique_strings.contains(ast_path_name))
                {
                    LOG_DEBUG(logger, "ast_path_name={} is exist", ast_path_name);
                    can_cache = false;
                    return;
                }
                unique_strings.insert(ast_path_name);
                pos.step_type = -1;
                // Determine step type from ast_path_name in a more robust way
                if (ast_path_name.length() > static_cast<size_t>(Offset::StepType))
                {
                    char step_char = ast_path_name[static_cast<size_t>(Offset::StepType)];
                    if (step_char == 'E')
                    {
                        if (last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::HASTOKEN) || last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE) ||
                            (is_cast && last_second_function_name == getFunctionName(FunctionNames::L2DISTANCE)) ||
                            (is_cast && last_second_function_name == getFunctionName(FunctionNames::COSINEDISTANCE))
                        )
                            pos.step_type = 4;
                        else
                            pos.step_type = 1;
                    }
                    if (step_char == 'F')
                        pos.step_type = 2;
                    if (step_char == 'T')
                        pos.step_type = 3;
                }

                if (pos.step_type == 3)
                {
                    LOG_DEBUG(logger, "Join ... On found Literal");
                    can_cache = false;
                    return;
                }
                pos.path = path;
                pos.target_type = target_type;
                pos.function_list = function_list;
                pos.ast_path_name = ast_path_name;
                pos.identifier_name = getFieldName(pos.identifier_name);
                positions.push_back(pos);
                query_result.parsed_params.push_back(literal_node->value);
                literal_node->value = converted;
            }
            return;
        }
        if (const auto * func = ast->as<ASTFunction>())
        {
            // Preserve a stable traversal order for function nodes:
            // parameters first, then regular arguments, then any remaining children.
            // The same order must be used everywhere that maps parameter index ->
            // AST literal position -> QueryPlan binding.
            String function_name = Poco::toLower(func->name);

            if (!only_vector ||
                    (only_vector &&
                        (function_name == getFunctionName(FunctionNames::COSINEDISTANCE) || function_name == getFunctionName(FunctionNames::L2DISTANCE) || function_name == getFunctionName(FunctionNames::HASTOKEN) ||
                            (function_name == getFunctionName(FunctionNames::CAST) &&
                                (last_function_name == getFunctionName(FunctionNames::COSINEDISTANCE) || last_function_name == getFunctionName(FunctionNames::L2DISTANCE) || last_function_name == getFunctionName(FunctionNames::HASTOKEN))
                            )
                        )
                    )
                )
            {
                function_list.push_back(function_name);
                if (function_name != getFunctionName(FunctionNames::COSINEDISTANCE) && function_name != getFunctionName(FunctionNames::L2DISTANCE))
                {
                    for (size_t i = 0; i < ast->children.size(); ++i)
                    {
                        auto & child = ast->children[i];
                        if (child == func->parameters)
                        {
                            path.push_back(static_cast<UInt32>(i));
                            collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
                            path.pop_back();
                        }
                    }
                }
                for (size_t i = 0; i < ast->children.size(); ++i)
                {
                    auto & child = ast->children[i];
                    if (child == func->arguments)
                    {
                        path.push_back(static_cast<UInt32>(i));
                        collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
                        path.pop_back();
                    }
                }
                for (size_t i = 0; i < ast->children.size(); ++i)
                {
                    auto & child = ast->children[i];
                    if (child == func->parameters || child == func->arguments)
                        continue;
                    if (should_skip_limit_child(ast, child))
                        continue;
                    path.push_back(static_cast<UInt32>(i));
                    collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
                    path.pop_back();
                }
                function_list.pop_back();
                parent_list.pop_back();
                return;
            }
            parent_list.pop_back();
            return;
        }

        for (size_t i = 0; i < ast->children.size(); ++i)
        {
            auto & child = ast->children[i];
            if (should_skip_limit_child(ast, child))
                continue;
            path.push_back(static_cast<UInt32>(i));
            collect(child, parent_list, depth + 1, path, function_list, ast_path_name + "_" + get_node_name(child));
            path.pop_back();
        }
        parent_list.pop_back();
    };

    NodePath root_path;
    std::vector<ASTPtr> parent_list;
    std::vector<String> function_list;
    ASTPtr ast_clone = query_ast->clone();
    collect(ast_clone, parent_list, 0, root_path, function_list, "");
    if (!can_cache)
    {
        query_result.parsed_params.clear();
        return query_result;
    }
    query_result.normalized_sql = ast_clone->formatForLogging();
    query_result.ast_literal_position_list = std::move(positions);
    return query_result;
}

/// Extract literal values from an AST by following the recorded position paths.
/// Used to build the parameter vector from a live AST (e.g. for cache key comparison).
/// Returns an empty vector if any path does not resolve to an ASTLiteral node.
std::vector<Field> VectorQueryParameters::buildParameterValuesFromAST(
    const ASTPtr & query_ast,
    const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions)
{
    std::vector<Field> values;
    if (!query_ast || positions.empty())
        return values;

    values.reserve(positions.size());
    for (const auto & pos : positions)
    {
        ASTPtr node = getASTNodeByPath(query_ast, pos.path);
        const auto * literal = node ? node->as<ASTLiteral>() : nullptr;
        if (!literal)
            return {};
        values.push_back(literal->value);
    }

    return values;
}

/// Re-inject parsed parameter values into a cached AST at the recorded literal positions.
/// This restores a cached AST template with the current query's runtime values.
/// The parameter index maps 1:1 to the AST literal position index (same traversal order).
/// Returns true if at least one literal was successfully replaced.
bool VectorQueryParameters::applyParametersByASTLiteralPositions(
    ASTPtr & query_ast,
    NormalizedQueryResult & parameters,
    const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions) const
{
    if (!query_ast || parameters.params.empty() || positions.empty())
        return false;

    try
    {
        size_t replaced_count = 0;
        const size_t count = std::min(positions.size(), parameters.parsed_params.size());
        // Parameter tokens and literal positions are collected in the same traversal
        // order, so the same index can be used to reconnect each token to one AST node.
        for (size_t i = 0; i < count; ++i)
        {
            ASTPtr node = getASTNodeByPath(query_ast, positions[i].path);
            if (!node)
                continue;
            auto * literal = node->as<ASTLiteral>();
            if (!literal)
                continue;

            literal->value = parameters.parsed_params[i];
            ++replaced_count;
        }
        return replaced_count > 0;
    }
    catch (...)
    {
        LOG_DEBUG(logger, "Exception caught when applyParametersByASTLiteralPositions: {}", getCurrentExceptionMessage(false));
        return false;
    }
}

/// Parse raw parameter strings into typed Fields using type hints from AST literal positions.
/// For each parameter, the corresponding AST position provides:
///   - field_type: the Field::Types::Which value (String, UInt64, Array, etc.)
///   - target_type: the DataTypePtr for type-specific parsing (e.g. Array(Float32))
/// Delegates to the shared parseNormalizedParams() helper.
bool VectorQueryParameters::parseNormalizedParamsWithAST(
    NormalizedQueryResult & parameters,
    const std::vector<VectorQueryPlanCache::ASTLiteralPosition> * positions,
    bool only_vector) const
{
    if (parameters.params.empty())
        return false;
    if (!positions)
        return false;
    std::vector<DataTypePtr> target_types(parameters.params.size());
    std::vector<Int32> literal_types(parameters.params.size(), -1);

    if (positions)
    {
        const size_t positions_count = std::min(parameters.params.size(), positions->size());
        for (size_t i = 0; i < positions_count; ++i)
        {
            literal_types[i] = (*positions)[i].field_type;
            if (!target_types[i] && (*positions)[i].target_type)
                target_types[i] = (*positions)[i].target_type;
        }
    }

    return parseNormalizedParams(parameters, target_types, literal_types, only_vector);
}

/// Rewrite bare vector array literals into explicit CAST expressions.
/// For example, transforms:
///   SELECT * FROM t WHERE l2distance(vec, [1.0, 2.0, 3.0]) < 0.5
/// into:
///   SELECT * FROM t WHERE l2distance(vec, CAST([1.0, 2.0, 3.0], 'Array(Float)')) < 0.5
///
/// This is used when `vector_use_cast` is enabled but plan caching is not active.
/// Only top-level array literals inside l2distance/cosinedistance functions are rewritten.
/// Nested arrays and function calls inside arrays are left unchanged.
String VectorQueryParameters::rewriteVectorLiteralsToCasts(
    const char * begin,
    const char * end) const
{
    String new_sql;
    Lexer lexer(begin, end);

    if (!isSelectStatement(lexer))
    {
        return new_sql;
    }

    UInt32 vector_function_type = 0;  // 1=l2distance, 2=hastoken, 3=cosinedistance
    bool is_comma = false;
    bool is_bare_word = false;

    while (true)
    {
        Token token = lexer.nextToken();
        if (token.isEnd() || token.isError())
        {
            new_sql += std::string(token.begin, token.size());
            break;
        }
        if (vector_function_type && token.type == TokenType::BareWord)
            is_bare_word = true;
        if (token.type == TokenType::BareWord && !vector_function_type
            && (token.size() == 8 || token.size() == 10 || token.size() == 14))
        {
            if (tokenMatchesBareWord(token, getFunctionName(FunctionNames::L2DISTANCE)))
                vector_function_type =  1;
            else if (tokenMatchesBareWord(token, getFunctionName(FunctionNames::HASTOKEN)))
                vector_function_type =  2;
            else if (tokenMatchesBareWord(token, getFunctionName(FunctionNames::COSINEDISTANCE)))
                vector_function_type =  3;
            if (vector_function_type)
                is_bare_word = false;
        }
        if (vector_function_type && is_bare_word && token.type == TokenType::Comma)
        {
            is_comma = true;
        }
        if (vector_function_type && is_comma && token.type == TokenType::ClosingRoundBracket)
        {
            vector_function_type = 0;
            is_comma = false;
            is_bare_word = false;
        }
        /// -------- literal --------
        if ((vector_function_type == 1 || vector_function_type == 3) && token.type == TokenType::OpeningSquareBracket)
        {
            const char * array_begin = token.begin;
            const char * array_end = token.end;
            bool valid = false;
            bool is_function = false;
            size_t array_depth = 1;

            // Fast path: scan raw characters to find the matching ']'.
            // This avoids per-element Lexer tokenization (~1535 tokens for a 768-dim vector).
            {
                const char * p = token.end;
                size_t depth = 1;
                bool in_string = false;
                char string_quote = '\0';
                while (p < end && depth > 0)
                {
                    char c = *p;
                    if (in_string)
                    {
                        if (c == '\\')
                        {
                            ++p;
                        }
                        else if (c == string_quote)
                        {
                            in_string = false;
                        }
                    }
                    else if (c == '\'' || c == '"')
                    {
                        in_string = true;
                        string_quote = c;
                    }
                    else if (c == '[')
                    {
                        ++array_depth;
                        ++depth;
                    }
                    else if (c == ']')
                    {
                        --depth;
                        if (depth == 0)
                        {
                            array_end = p + 1;
                            valid = true;
                            break;
                        }
                    }
                    else if (!is_function && depth == 1 && ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'))
                    {
                        is_function = true;
                    }
                    ++p;
                }
                lexer.setPosition(array_end);
            }

            if (array_depth == 1 && !is_function)
            {
                appendFunctionName(new_sql, FunctionNames::CAST);
                new_sql += "('";
            }

            if (valid)
            {
                String original_array(array_begin, static_cast<size_t>(array_end - array_begin));
                new_sql += original_array;
                if (array_depth == 1 && !is_function)
                    new_sql += "','Array(Float32)')";
                continue;
            }
        }
        new_sql += std::string(token.begin, token.size());
    }

    return new_sql;
}

/// Parse raw parameter strings into typed Fields using type hints from QueryPlan constant bindings.
/// For each parameter, the binding provides field_type and target_type.  VectorScan-scoped
/// bindings with Array field type take priority over other bindings for the same parameter index.
/// Falls back to AST positions for type hints when bindings are incomplete.
/// Delegates to the shared parseNormalizedParams() helper.
bool VectorQueryParameters::parseNormalizedParamsWithPlan(
    NormalizedQueryResult & parameters,
    const std::vector<VectorQueryPlanCache::PlanConstantBinding> * plan_constant_bindings,
    bool only_vector) const
{
    if (parameters.params.empty())
        return false;
    if (!plan_constant_bindings)
        return false;
    std::vector<DataTypePtr> target_types(parameters.params.size());
    std::vector<Int32> literal_types(parameters.params.size(), -1);

    if (plan_constant_bindings)
    {
        for (const auto & binding : *plan_constant_bindings)
        {
            if (binding.parameter_index >= target_types.size())
                continue;

            if (isVectorScanBindingScope(binding.dag_scope) && binding.field_type == static_cast<Int32>(Field::Types::Array))
                literal_types[binding.parameter_index] = binding.field_type;
            else if (literal_types[binding.parameter_index] < 0)
                literal_types[binding.parameter_index] = binding.field_type;

            if (!target_types[binding.parameter_index] && binding.target_type)
                target_types[binding.parameter_index] = binding.target_type;
        }
    }

    return parseNormalizedParams(parameters, target_types, literal_types, only_vector);
}

/// Scan a built QueryPlan and match every mutable constant slot back to the ordered AST
/// literal metadata collected earlier for the same query.
///
/// The algorithm:
///   1. Walk the plan tree, collecting PlanConstantCandidate from each ExpressionStep
///      and FilterStep's ActionsDAG (via findActionsDAGAndCollectConstants).
///   2. For each AST literal position, find exactly one matching candidate using
///      candidateMatchesAstLiteral() (matching on scope, identifier, function chain, value).
///   3. If any AST literal has zero or multiple matches, the entire result is invalidated
///      (returns empty bindings) to prevent incorrect plan reuse.
///   4. The final bindings list is reversed to match the AST traversal order.
///
/// Returns an empty vector on any mismatch (logged at DEBUG level).
std::vector<VectorQueryPlanCache::PlanConstantBinding> VectorQueryParameters::CollectQueryPlanConstants(
    QueryPlan & query_plan,
    const NormalizedQueryResult & parameters, bool only_vector)
{
    std::vector<VectorQueryPlanCache::PlanConstantBinding> bindings;
    QueryPlan::Node * root = query_plan.getRootNode();
    if (!root)
        return bindings;
    if (parameters.ast_literal_position_list.empty())
        return bindings;

    std::vector<std::pair<QueryPlan::Node *, NodePath>> stack;
    stack.emplace_back(root, NodePath{});
    std::unordered_set<QueryPlan::Node *> visited;
    visited.reserve(64);
    std::vector<PlanConstantCandidate> candidates;
    candidates.reserve(parameters.ast_literal_position_list.size());
    while (!stack.empty())
    {
        auto [node, path] = stack.back();
        stack.pop_back();
        if (!node || !visited.insert(node).second)
            continue;

        if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
            findActionsDAGAndCollectConstants(
                expression_step->getExpression(),
                path,
                "ExpressionStep",
                1,
                candidates, only_vector);

        if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
            findActionsDAGAndCollectConstants(
                filter_step->getExpression(),
                path,
                "FilterStep",
                2,
                candidates, only_vector);

        for (size_t i = 0; i < node->children.size(); ++i)
        {
            NodePath child_path = path;
            child_path.push_back(static_cast<UInt32>(i));
            stack.emplace_back(node->children[i], std::move(child_path));
        }
    }

    std::vector<bool> candidate_used(candidates.size(), false);
    bindings.reserve(parameters.ast_literal_position_list.size());

    for (size_t ast_index = 0; ast_index < parameters.ast_literal_position_list.size(); ++ast_index)
    {
        const auto & ast_position = parameters.ast_literal_position_list[ast_index];
        std::vector<size_t> matched_candidate_indexes;

        for (size_t candidate_index = 0; candidate_index < candidates.size(); ++candidate_index)
        {
            if (candidate_used[candidate_index])
                continue;

            if (candidateMatchesAstLiteral(candidates[candidate_index], ast_index, ast_position, parameters))
                matched_candidate_indexes.push_back(candidate_index);
        }
        if (matched_candidate_indexes.empty())
        {
            String function_chain;
            for (size_t function_index = 0; function_index < ast_position.function_list.size(); ++function_index)
            {
                if (function_index)
                    function_chain += "->";
                function_chain += ast_position.function_list[function_index];
            }

            LOG_DEBUG(
                logger,
                "CollectQueryPlanConstants failed: no QueryPlan constant matches AST literal index={} step_type={} identifier_name={} ast_path_name={} parsed_param={} function_list={}",
                ast_index,
                ast_position.step_type,
                ast_position.identifier_name,
                ast_position.ast_path_name,
                ast_index < parameters.parsed_params.size() ? applyVisitor(FieldVisitorToString(), parameters.parsed_params[ast_index]) : String{},
                function_chain);
            bindings.clear();
            return bindings;
        }

        if (matched_candidate_indexes.size() > 1)
        {
            std::vector<String> candidate_scopes;
            candidate_scopes.reserve(matched_candidate_indexes.size());
            for (const auto candidate_index : matched_candidate_indexes)
                candidate_scopes.push_back(candidates[candidate_index].binding.dag_scope + "#" + toString(candidates[candidate_index].binding.dag_node_index));

            String scopes_text;
            for (size_t scope_index = 0; scope_index < candidate_scopes.size(); ++scope_index)
            {
                if (scope_index)
                    scopes_text += ", ";
                scopes_text += candidate_scopes[scope_index];
            }

            LOG_DEBUG(
                logger,
                "CollectQueryPlanConstants failed: AST literal index={} step_type={} identifier_name={} ast_path_name={} matched multiple QueryPlan constants: {}",
                ast_index,
                ast_position.step_type,
                ast_position.identifier_name,
                ast_position.ast_path_name,
                scopes_text);
            bindings.clear();
            return bindings;
        }

        const size_t matched_candidate_index = matched_candidate_indexes.front();
        candidate_used[matched_candidate_index] = true;
        auto binding = candidates[matched_candidate_index].binding;
        binding.parameter_index = static_cast<UInt32>(ast_index);
        bindings.push_back(std::move(binding));
    }

    for (size_t candidate_index = 0; candidate_index < candidates.size(); ++candidate_index)
    {
        if (candidate_used[candidate_index])
            continue;

        LOG_DEBUG(
            logger,
            "CollectQueryPlanConstants failed: QueryPlan constant scope={} dag_node_index={} value={} has no matching AST literal position",
            candidates[candidate_index].binding.dag_scope,
            candidates[candidate_index].binding.dag_node_index,
            candidates[candidate_index].binding.value_text);
        bindings.clear();
        return bindings;
    }

    std::reverse(bindings.begin(), bindings.end());
    return bindings;
}

}
