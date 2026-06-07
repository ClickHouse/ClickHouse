#include <Interpreters/VectorQueryParameters.h>

#include <Core/Defines.h>
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

constexpr std::string_view COSINEDISTANCE_FUNCTION_NAME = "cosinedistance";
constexpr std::string_view L2DISTANCE_FUNCTION_NAME = "l2distance";
constexpr std::string_view HASTOKEN_FUNCTION_NAME = "hastoken";
constexpr std::string_view CAST_FUNCTION_NAME = "cast";

String dataTypePtrToString(const DataTypePtr & type)
{
    if (!type)
        return "nullptr";
    return type->getName();
}


bool isVectorScanBindingScope(const String & dag_scope)
{
    return dag_scope.find("VectorScan") != String::npos;
}

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

std::string_view stripOuterQuotes(std::string_view value)
{
    if (value.size() >= 2 && ((value.front() == '\'' && value.back() == '\'') || (value.front() == '"' && value.back() == '"')))
        return value.substr(1, value.size() - 2);
    return value;
}


/// Parse a SQL string literal into a Field using SQL-style quoting rules.
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
        Float64 float_value;
        if (tryReadFloatTextFast(float_value, buf) && buf.eof())
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
        Int64 int_value;
        UInt64 uint_value;
        
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


bool isNumericFieldType(Field::Types::Which type)
{
    return type == Field::Types::UInt64
        || type == Field::Types::Int64
        || type == Field::Types::Float64;
}

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
    __builtin_unreachable();
}

void appendFunctionName(String & out, FunctionNames fn_enum)
{
    const auto name = getFunctionName(fn_enum);
    out.append(name.data(), name.size());
}

String getFieldName(String input_name)
{
    if (input_name.empty())
        return "";
    size_t index = input_name.find_last_of('.');
    if (input_name.size() > index + 1)
        return input_name.substr(index + 1);
    return "";
}


String getLastFunctionName(const VectorQueryPlanCache::ASTLiteralPosition & position)
{
    if (position.function_list.empty())
        return {};
    return Poco::toLower(position.function_list.back());
}

String getSecondLastFunctionName(const VectorQueryPlanCache::ASTLiteralPosition & position)
{
    if (position.function_list.size() < 2)
        return {};
    return Poco::toLower(position.function_list[position.function_list.size() - 2]);
}

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
    const String ast_last_function = getLastFunctionName(position);
    const String ast_second_last_function = getSecondLastFunctionName(position);

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
            if (number == position.function_list.size())
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
            
            bool match = parameters.params[ast_index].original_string == candidate.value.safeGet<String>();
            return match;
        }

        if (candidate.value.getType() == Field::Types::String)
        {
            return stripOuterQuotes(parameters.params[ast_index].original_string) == candidate.value.safeGet<String>();
        }
        return fieldsEquivalent(parameters.parsed_params[ast_index], candidate.value);
    }
    return false;
}

void findActionsDAGAndCollectConstants(
    const ActionsDAG & dag,
    const std::vector<UInt32> & plan_node_path,
    const String & dag_scope,
    Int32 step_type,
    std::vector<PlanConstantCandidate> & out_candidates)
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
        // Process based on node type
        switch (node->type)
        {
            case ActionsDAG::ActionType::FUNCTION:
            {
                // Push function name to the list
                if (node->function_base)
                {
                    String func_name = Poco::toLower(node->function_base->getName());
                    function_names.push_back(func_name);
                    // Recursively traverse children
                    for (const auto * child : node->children)
                        traverse_node(child, node, function_names);
                    function_names.pop_back();
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
                    String last_function_name = "";
                    if (function_size >= 1)
                        last_function_name = function_names[function_size - 1];
                    current_field_name = " ";
                    DataTypePtr result_type;
                    if (parent_node)
                    {
                        for (const auto * child : parent_node->children)
                        {
                            switch(child->type)
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
}

namespace
{
/// Helper function to check if a lexer token starts with the SELECT keyword.
/// Filters out non-significant tokens before checking.
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

/// Helper function to check if a token matches a bare word by name (case-insensitive).
/// The token and bare_word_name must have the same length for an exact match.
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

VectorQueryParameters::NormalizedQueryResult VectorQueryParameters::normalizeQueryAndExtractParams(
    const char * begin,
    const char * end,
    bool only_vector,
    bool use_cast)
{
    // The normalized SQL text becomes the cache-key payload, while params preserves
    // the original literal token stream in the exact order later expected by AST / plan restore.
    //
    // The function does two jobs at once:
    // 1. Build a cache-key-friendly SQL template where literal runs collapse to '?'.
    // 2. Preserve the original literal text in `params` so cache hits can rebuild
    //    AST / QueryPlan snapshots with the current runtime values.
    NormalizedQueryResult result;
    SipHash hash;
    Lexer lexer(begin, end);

    if (!isSelectStatement(lexer))
    {
        result.hash = 0;
        result.normalized_sql = "";
        return result;
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
    bool is_bare_word = false;
    bool is_dot = false;
    bool is_negative = false;
    bool is_where = false;

    while (true)
    {
        Token token = lexer.nextToken();
        if (token.type == TokenType::Semicolon)
            break;
        if (token.isEnd() || token.isError())
            break;
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
            result.normalized_sql += "?";
            hash.update("\x00", 1);
            if (vector_complete || vector_function_type == 2)
                result.params.emplace_back(String(token.begin, token.size()), ParameterInfo::Type::STRING);
            result.new_sql += std::string(token.begin, token.size());
            vector_complete = false;
            continue;
        }
        /// -------- literal --------
        if (!is_where && token.type == TokenType::OpeningSquareBracket)
        {
            const char * array_begin = token.begin;
            const char * array_end = token.end;
            size_t depth = 1;
            size_t array_depth = 1;
            Token last_significant = token;
            bool valid = true;
            
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
            if (use_cast && array_depth == 1 && (vector_function_type == 1 || vector_function_type == 3))
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
                if (array_depth == 1 && (vector_function_type == 1 || vector_function_type == 3))
                    result.normalized_sql += "?";
                else
                    result.normalized_sql += original_array;
                result.params.emplace_back(original_array, ParameterInfo::Type::NUMERIC_VECTOR);
                
                vector_complete = true;
                if (num_literals_in_sequence == 0 && (vector_function_type == 1 || vector_function_type == 3))
                    hash.update("\x00", 1);

                ++num_literals_in_sequence;

                if (use_cast && array_depth == 1 && (vector_function_type == 1 || vector_function_type == 3))
                {
                    result.new_sql += "','Array(Float)')";
                    result.normalized_sql += ",?)";
                }
                continue;
            }
        }
        if (token.type == TokenType::BareWord && token.size() == 5 && tokenMatchesBareWord(token, "select"))
            is_where = false;
        if (token.type == TokenType::BareWord && token.size() == 5 && tokenMatchesBareWord(token, "where"))
            is_where = true;
        if (token.type == TokenType::Dot)
            is_dot = true;
        else if (!only_vector)
        {
            if (token.type == TokenType::BareWord && token.size() == 5 && tokenMatchesBareWord(token, "limit"))
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
            if (parse_params && !vector_function_type && !is_dot && (token.type == TokenType::Number
                    || token.type == TokenType::StringLiteral
                    || token.type == TokenType::HereDoc)
                )
            {
                
                ParameterInfo::Type param_type;
                if (token.type == TokenType::Number)
                    param_type = ParameterInfo::Type::NUMERIC;
                else if (token.type == TokenType::StringLiteral || token.type == TokenType::HereDoc)
                    param_type = ParameterInfo::Type::STRING;
                else
                    param_type = ParameterInfo::Type::STRING;
                
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
                
                result.normalized_sql += "?";
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
        }
        hash.update(token.begin, token.size());
        result.normalized_sql += std::string(token.begin, token.size());
        result.new_sql += std::string(token.begin, token.size());
    }

    result.hash = hash.get64();
    return result;
}


void VectorQueryParameters::replaceConstantsInQueryPlan(
    QueryPlan & plan,
    NormalizedQueryResult & parameters,
    const std::vector<VectorQueryPlanCache::PlanConstantBinding> & plan_constant_bindings)
{
    if (plan_constant_bindings.empty() || parameters.params.empty() || parameters.parsed_params.empty())
        return;
    auto * root = plan.getRootNode();
    if (!root)
        return;

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

    auto apply_bindings_to_dag = [&](ActionsDAG & dag, const UInt32 dag_node_index, const UInt32 parameter_index)
    {
        if (parameter_index >= parameters.parsed_params.size())
            return;

        auto node_it = dag.getNodes().begin();
        std::advance(node_it, std::min<size_t>(dag_node_index, dag.getNodes().size()));
        if (node_it == dag.getNodes().end())
            return;

        auto & dag_node = const_cast<ActionsDAG::Node &>(*node_it);
        if (dag_node.type != ActionsDAG::ActionType::COLUMN || !dag_node.column || !isColumnConst(*dag_node.column) || !dag_node.result_type)
            return;
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
            // ActionsDAG constants are rewritten by replacing the const column payload
            // at the recorded node index with the parsed runtime Field value.
            ColumnPtr new_column = dag_node.result_type->createColumnConst(0, raw_value);
            const_cast<ColumnPtr &>(dag_node.column) = std::move(new_column);
        }
        catch (...)
        {
            LOG_TRACE(logger, "Exception caught when updating DAG node column: {}", getCurrentExceptionMessage(false));
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
            apply_bindings_to_dag(dag,  plan_constant_binding.dag_node_index, plan_constant_binding.parameter_index);
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            if (plan_constant_binding.dag_scope != "ExpressionStep")
                continue;
            ActionsDAG & dag = expression_step->getExpression();
            apply_bindings_to_dag(dag,  plan_constant_binding.dag_node_index, plan_constant_binding.parameter_index);
        }
    }
}

std::vector<VectorQueryPlanCache::ASTLiteralPosition> VectorQueryParameters::collectASTLiteralPositions(
    const ASTPtr & query_ast,
    bool only_vector) const
{
    std::vector<VectorQueryPlanCache::ASTLiteralPosition> positions;
    if (!query_ast)
        return positions;
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

    std::function<void(const ASTPtr &, std::vector<ASTPtr> &, size_t, NodePath &, std::vector<String> &, String)> collect
        = [&](const ASTPtr & ast, std::vector<ASTPtr> & parent_list, size_t depth, NodePath & path,
        std::vector<String> & function_list, String ast_path_name)
    {
        if (!ast || !can_cache)
            return;
        parent_list.push_back(ast);
        size_t function_size = function_list.size();
        String last_function_name = "";
        bool is_cast = false;
        if (function_size >= 1)
            last_function_name = function_list[function_size - 1];
        String last_second_function_name = "";
        if (function_size >= 2)
            last_second_function_name = function_list[function_size - 2];
        if (const auto * literal_node = ast->as<ASTLiteral>())
        {
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
            if (ident_name_list.size() >= 1  &&
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

bool VectorQueryParameters::applyParametersByASTLiteralPositions(
    ASTPtr & query_ast,
    NormalizedQueryResult & parameters,
    const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & positions) const
{
    if (!query_ast || parameters.params.empty() || positions.empty())
        return false;

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

String VectorQueryParameters::rewriteVectorLiteralsToCasts(
    const char * begin,
    const char * end) const
{
    String new_sql = "";
    Lexer lexer(begin, end);

    if (!isSelectStatement(lexer))
    {
        new_sql.assign(begin, end);
        return new_sql;
    }

    bool is_cast = false;
    UInt32 vector_function_type = 0;
    bool is_comma = false;
    bool is_bare_word = false;

    while (true)
    {
        Token token = lexer.nextToken();
        if (token.type == TokenType::Semicolon)
            break;
        if (token.isEnd() || token.isError())
            break;
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
        if (is_comma && (vector_function_type == 1 || vector_function_type == 3) &&
            token.type == TokenType::BareWord && token.size() == 4 &&
            tokenMatchesBareWord(token, getFunctionName(FunctionNames::CAST)))
        {
            is_cast = true;
            appendFunctionName(new_sql, FunctionNames::CAST);
            continue;
        }
        if (vector_function_type && is_comma && token.type == TokenType::ClosingRoundBracket)
        {
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
            new_sql += std::string(token.begin, token.size());   
            continue;
        }
        /// -------- literal --------
        if ((vector_function_type == 1 || vector_function_type == 3) && token.type == TokenType::OpeningSquareBracket)
        {
            const char * array_begin = token.begin;
            const char * array_end = token.end;
            size_t depth = 1;
            size_t array_depth = 1;
            bool valid = true;

            while (depth > 0)
            {
                Token nested = lexer.nextToken();
                array_end = nested.end;

                if (nested.isEnd() || nested.isError())
                {
                    valid = false;
                    break;
                }

                if (!nested.isSignificant())
                    continue;

                if (nested.type == TokenType::OpeningSquareBracket)
                {
                    ++array_depth;
                    ++depth;
                }
                else if (nested.type == TokenType::ClosingSquareBracket)
                    --depth;

            }
            
            if (array_depth == 1)
            {
                appendFunctionName(new_sql, FunctionNames::CAST);
                new_sql += "('";
            }

            if (valid && depth == 0)
            {
                String original_array(array_begin, static_cast<size_t>(array_end - array_begin));
                
                new_sql += original_array;
                
                new_sql += "','Array(Float)')";
                continue;
            }
        }
        new_sql += std::string(token.begin, token.size());
    }

    return new_sql;
}

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

std::vector<VectorQueryPlanCache::PlanConstantBinding> VectorQueryParameters::CollectQueryPlanConstants(
    QueryPlan & query_plan,
    const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & ast_literal_positions,
    const NormalizedQueryResult & parameters)
{
    std::vector<VectorQueryPlanCache::PlanConstantBinding> bindings;
    QueryPlan::Node * root = query_plan.getRootNode();
    if (!root)
        return bindings;
    if (ast_literal_positions.empty())
        return bindings;

    std::vector<std::pair<QueryPlan::Node *, NodePath>> stack;
    stack.emplace_back(root, NodePath{});
    std::unordered_set<QueryPlan::Node *> visited;
    visited.reserve(64);
    std::vector<PlanConstantCandidate> candidates;
    candidates.reserve(ast_literal_positions.size());
    while (!stack.empty())
    {
        auto [node, path] = stack.back();
        stack.pop_back();
        if (!node || !visited.insert(node).second)
            continue;

        const String step_name = node->step ? node->step->getName() : "Unknown";
        if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
            findActionsDAGAndCollectConstants(
                expression_step->getExpression(),
                path,
                "ExpressionStep",
                1,
                candidates);

        if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
            findActionsDAGAndCollectConstants(
                filter_step->getExpression(),
                path,
                "FilterStep",
                2,
                candidates);

        for (size_t i = 0; i < node->children.size(); ++i)
        {
            NodePath child_path = path;
            child_path.push_back(static_cast<UInt32>(i));
            stack.emplace_back(node->children[i], std::move(child_path));
        }
    }

    std::vector<bool> candidate_used(candidates.size(), false);
    bindings.reserve(ast_literal_positions.size());
    
    for (size_t ast_index = 0; ast_index < ast_literal_positions.size(); ++ast_index)
    {
        const auto & ast_position = ast_literal_positions[ast_index];
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
                "CollectQueryPlanConstants failed: no QueryPlan constant matches AST literal index={} step_type={} identifier_name={} ast_path_name={} raw_param={} parsed_param={} function_list={}",
                ast_index,
                ast_position.step_type,
                ast_position.identifier_name,
                ast_position.ast_path_name,
                ast_index < parameters.params.size() ? parameters.params[ast_index].original_string : String{},
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
