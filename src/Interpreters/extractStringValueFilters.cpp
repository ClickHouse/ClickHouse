#include <Interpreters/extractStringValueFilters.h>

#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>

#include <algorithm>
#include <optional>

namespace DB
{

namespace
{

using Condition = StringValueFilter::Condition;
using ConditionsByColumn = std::unordered_map<String, std::vector<Condition>>;

/// Too many conditions would make checking values too expensive.
constexpr size_t MAX_CONDITIONS_PER_COLUMN = 8;

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();
    return node;
}

/// Returns the name of the column if the node is an input of type String or Nullable(String).
std::optional<String> tryGetStringInputName(const ActionsDAG::Node * node)
{
    node = skipAliases(node);

    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    if (!isString(removeNullable(node->result_type)))
        return {};

    return node->result_name;
}

std::optional<String> tryGetConstString(const ActionsDAG::Node * node)
{
    node = skipAliases(node);

    if (!node->column || !isColumnConst(*node->column))
        return {};

    Field field = (*node->column)[0];
    if (field.getType() != Field::Types::String)
        return {};

    return field.safeGet<String>();
}

/// Parses a LIKE pattern into a list of required fixed substrings. For example, for the pattern
/// `abc%def_xyz` the value must start with `abc`, contain `def` and end with `xyz`. The conditions
/// are necessary but not sufficient for the pattern to match, which is enough for the filter.
/// Returns an empty list if nothing can be extracted (e.g. the pattern is `%` or is malformed).
std::vector<Condition> parseLikePattern(const String & pattern)
{
    std::vector<Condition> res;
    String chunk;
    bool wildcard_before_chunk = false;

    auto flush_chunk = [&](bool wildcard_after_chunk)
    {
        if (!chunk.empty())
        {
            Condition::Type type = Condition::Type::Substring;
            if (!wildcard_before_chunk && !wildcard_after_chunk)
                type = Condition::Type::Equals;
            else if (!wildcard_before_chunk)
                type = Condition::Type::Prefix;
            else if (!wildcard_after_chunk)
                type = Condition::Type::Suffix;
            else
                type = Condition::Type::Substring;

            res.push_back({type, std::move(chunk)});
            chunk = {};
        }
    };

    size_t i = 0;
    while (i < pattern.size())
    {
        char c = pattern[i];
        if (c == '\\')
        {
            /// The pattern is malformed and LIKE itself will throw an exception on it.
            if (i + 1 == pattern.size())
                return {};

            char next = pattern[i + 1];
            if (next == '%' || next == '_' || next == '\\')
            {
                chunk += next;
                i += 2;
            }
            else
            {
                /// For an unknown escape sequence LIKE treats the backslash as a literal character.
                chunk += '\\';
                ++i;
            }
        }
        else if (c == '%' || c == '_')
        {
            flush_chunk(true);
            wildcard_before_chunk = true;
            ++i;
        }
        else
        {
            chunk += c;
            ++i;
        }
    }

    flush_chunk(false);
    return res;
}

/// Returns (column name, needle) for a node of the form `position(column, 'needle')` with a non-empty needle.
std::optional<std::pair<String, String>> tryGetPosition(const ActionsDAG::Node * node)
{
    node = skipAliases(node);

    if (node->type != ActionsDAG::ActionType::FUNCTION || node->function_base->getName() != "position" || node->children.size() != 2)
        return {};

    auto column_name = tryGetStringInputName(node->children[0]);
    auto needle = tryGetConstString(node->children[1]);
    if (!column_name || !needle || needle->empty())
        return {};

    return std::make_pair(*column_name, *needle);
}

}

std::optional<bool> evaluatePositionComparisonAtZero(const String & function_name, const Field & constant, bool position_is_left_argument)
{
    /// Comparisons of 0 with the constant.
    bool equals = false;
    bool less = false;
    bool greater = false;

    switch (constant.getType())
    {
        case Field::Types::UInt64:
        {
            UInt64 value = constant.safeGet<UInt64>();
            equals = value == 0;
            less = value > 0;
            greater = false;
            break;
        }
        case Field::Types::Int64:
        {
            Int64 value = constant.safeGet<Int64>();
            equals = value == 0;
            less = value > 0;
            greater = value < 0;
            break;
        }
        case Field::Types::Float64:
        {
            Float64 value = constant.safeGet<Float64>();
            equals = value == 0.0;
            less = 0.0 < value;
            greater = 0.0 > value;
            break;
        }
        case Field::Types::Bool:
        {
            bool value = constant.safeGet<bool>();
            equals = !value;
            less = value;
            greater = false;
            break;
        }
        default:
            return {};
    }

    if (function_name == "equals")
        return equals;
    if (function_name == "notEquals")
        return !equals;
    if (function_name == "greater")
        return position_is_left_argument ? greater : less;
    if (function_name == "less")
        return position_is_left_argument ? less : greater;
    if (function_name == "greaterOrEquals")
        return position_is_left_argument ? (greater || equals) : (less || equals);
    if (function_name == "lessOrEquals")
        return position_is_left_argument ? (less || equals) : (greater || equals);

    return {};
}

namespace
{

void tryExtractConditionsFromAtom(const ActionsDAG::Node * atom, ConditionsByColumn & res)
{
    if (atom->type != ActionsDAG::ActionType::FUNCTION)
        return;

    const String & name = atom->function_base->getName();

    if (name == "like")
    {
        if (atom->children.size() != 2)
            return;

        auto column_name = tryGetStringInputName(atom->children[0]);
        auto pattern = tryGetConstString(atom->children[1]);
        if (!column_name || !pattern)
            return;

        auto conditions = parseLikePattern(*pattern);
        if (conditions.empty())
            return;

        auto & column_conditions = res[*column_name];
        column_conditions.insert(column_conditions.end(), conditions.begin(), conditions.end());
    }
    else if (name == "startsWith" || name == "endsWith")
    {
        if (atom->children.size() != 2)
            return;

        auto column_name = tryGetStringInputName(atom->children[0]);
        auto needle = tryGetConstString(atom->children[1]);
        if (!column_name || !needle || needle->empty())
            return;

        auto type = name == "startsWith" ? Condition::Type::Prefix : Condition::Type::Suffix;
        res[*column_name].push_back({type, std::move(*needle)});
    }
    else if (name == "position")
    {
        /// A `position` result used directly as a condition means it must be non-zero,
        /// i.e. the needle must be present in the value.
        if (auto position = tryGetPosition(atom))
            res[position->first].push_back({Condition::Type::Substring, std::move(position->second)});
    }
    else if (name == "equals" || name == "notEquals" || name == "greater" || name == "less" || name == "greaterOrEquals" || name == "lessOrEquals")
    {
        if (atom->children.size() != 2)
            return;

        if (name == "equals")
        {
            /// Equality with a non-empty constant string.
            for (size_t column_pos : {0, 1})
            {
                auto column_name = tryGetStringInputName(atom->children[column_pos]);
                auto needle = tryGetConstString(atom->children[1 - column_pos]);
                if (column_name && needle && !needle->empty())
                {
                    res[*column_name].push_back({Condition::Type::Equals, std::move(*needle)});
                    return;
                }
            }
        }

        /// A comparison of `position(column, 'needle')` with a constant.
        for (size_t position_pos : {0, 1})
        {
            auto position = tryGetPosition(atom->children[position_pos]);
            if (!position)
                continue;

            const auto * constant_node = skipAliases(atom->children[1 - position_pos]);
            if (!constant_node->column || !isColumnConst(*constant_node->column))
                return;

            Field constant = (*constant_node->column)[0];
            auto result_at_zero = evaluatePositionComparisonAtZero(name, constant, position_pos == 0);
            if (result_at_zero && !*result_at_zero)
                res[position->first].push_back({Condition::Type::Substring, std::move(position->second)});
            return;
        }
    }
}

}

bool likePatternHasStringValueFilterConditions(const String & pattern)
{
    return !parseLikePattern(pattern).empty();
}

StringValueFiltersPtr extractStringValueFilters(const ActionsDAG & filter_dag, const String & filter_column_name)
{
    const auto * root = filter_dag.tryFindInOutputs(filter_column_name);
    if (!root)
        return nullptr;

    /// Flatten the top-level AND chain into atoms, unwrapping aliases.
    /// Only conjuncts can be used: every row not matching a conjunct is filtered out regardless
    /// of the rest of the expression.
    std::vector<const ActionsDAG::Node *> atoms;
    std::vector<const ActionsDAG::Node *> stack;
    stack.push_back(skipAliases(root));

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "and")
        {
            for (const auto * child : node->children)
                stack.push_back(skipAliases(child));
            continue;
        }

        atoms.push_back(node);
    }

    ConditionsByColumn conditions_by_column;
    for (const auto * atom : atoms)
        tryExtractConditionsFromAtom(atom, conditions_by_column);

    auto filters = std::make_shared<StringValueFilters>();
    for (auto & [column_name, conditions] : conditions_by_column)
    {
        /// Prefer longer needles: they are more selective and cheaper to check.
        std::stable_sort(
            conditions.begin(), conditions.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.needle.size() > rhs.needle.size(); });

        /// Remove duplicates.
        std::vector<Condition> unique_conditions;
        for (auto & condition : conditions)
        {
            bool duplicate = std::any_of(
                unique_conditions.begin(), unique_conditions.end(),
                [&](const auto & other) { return other.type == condition.type && other.needle == condition.needle; });

            if (!duplicate)
                unique_conditions.push_back(std::move(condition));

            if (unique_conditions.size() == MAX_CONDITIONS_PER_COLUMN)
                break;
        }

        filters->emplace(column_name, std::make_shared<StringValueFilter>(std::move(unique_conditions)));
    }

    if (filters->empty())
        return nullptr;

    return filters;
}

}
