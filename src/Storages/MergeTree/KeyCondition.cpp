#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/BoolMask.h>
#include <Core/PlainRanges.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/Utils.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/misc.h>
#include <Functions/FunctionFactory.h>
#include <Functions/indexHint.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <Functions/geometryConverters.h>
#include <Common/FieldVisitorToString.h>
#include <Common/HilbertUtils.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/MortonUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <algorithm>
#include <cassert>
#include <stack>
#include <limits>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/multi_polygon.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsBool analyze_index_with_space_filling_curves;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TYPE_OF_FIELD;
}

/// Returns the prefix of like_pattern before the first wildcard, e.g. 'Hello\_World% ...' --> 'Hello\_World'
/// We call a pattern "perfect prefix" if:
/// - (1) the pattern has a wildcard
/// - (2) the first wildcard is '%' and is only followed by nothing or other '%'
/// e.g. 'test%' or 'test%% has perfect prefix 'test', 'test%x', 'test%_' or 'test_' has no perfect prefix.
String extractFixedPrefixFromLikePattern(std::string_view like_pattern, bool requires_perfect_prefix)
{
    String fixed_prefix;
    fixed_prefix.reserve(like_pattern.size());

    const char * pos = like_pattern.data();
    const char * end = pos + like_pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
                if (requires_perfect_prefix)
                {
                    bool is_prefect_prefix = std::all_of(pos, end, [](auto c) { return c == '%'; });
                    return is_prefect_prefix ? fixed_prefix : "";
                }
                return fixed_prefix;
            case '\\':
                ++pos;
                if (pos == end)
                    break;
                [[fallthrough]];
            default:
                fixed_prefix += *pos;
        }

        ++pos;
    }
    /// If we can reach this code, it means there was no wildcard found in the pattern, so it is not a perfect prefix
    if (requires_perfect_prefix)
        return "";
    return fixed_prefix;
}

/// for "^prefix..." string it returns "prefix"
static String extractFixedPrefixFromRegularExpression(const String & regexp)
{
    if (regexp.size() <= 1 || regexp[0] != '^')
        return {};

    String fixed_prefix;
    const char * begin = regexp.data() + 1;
    const char * pos = begin;
    const char * end = regexp.data() + regexp.size();

    while (pos != end)
    {
        switch (*pos)
        {
            case '\0':
                pos = end;
                break;

            case '\\':
            {
                ++pos;
                if (pos == end)
                    break;

                switch (*pos)
                {
                    case '|':
                    case '(':
                    case ')':
                    case '^':
                    case '$':
                    case '.':
                    case '[':
                    case '?':
                    case '*':
                    case '+':
                    case '{':
                        fixed_prefix += *pos;
                        break;
                    default:
                        /// all other escape sequences are not supported
                        pos = end;
                        break;
                }

                ++pos;
                break;
            }

            /// non-trivial cases
            case '|':
                fixed_prefix.clear();
                [[fallthrough]];
            case '(':
            case '[':
            case '^':
            case '$':
            case '.':
            case '+':
                pos = end;
                break;

            /// Quantifiers that allow a zero number of occurrences.
            case '{':
            case '?':
            case '*':
                if (!fixed_prefix.empty())
                    fixed_prefix.pop_back();

                pos = end;
                break;
            default:
                fixed_prefix += *pos;
                pos++;
                break;
        }
    }

    return fixed_prefix;
}


/** For a given string, get a minimum string that is strictly greater than all strings with this prefix,
  *  or return an empty string if there are no such strings.
  */
static String firstStringThatIsGreaterThanAllStringsWithPrefix(const String & prefix)
{
    /** Increment the last byte of the prefix by one. But if it is max (255), then remove it and increase the previous one.
      * Example (for convenience, suppose that the maximum value of byte is `z`)
      * abcx -> abcy
      * abcz -> abd
      * zzz -> empty string
      * z -> empty string
      */

    String res = prefix;

    while (!res.empty() && static_cast<UInt8>(res.back()) == std::numeric_limits<UInt8>::max())
        res.pop_back();

    if (res.empty())
        return res;

    res.back() = static_cast<char>(1 + static_cast<UInt8>(res.back()));
    return res;
}

const KeyCondition::AtomMap KeyCondition::atom_map
{
    {
        "notEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "equals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "less",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, false);
            return true;
        }
    },
    {
        "greater",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, false);
            return true;
        }
    },
    {
        "lessOrEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, true);
            return true;
        }
    },
    {
        "greaterOrEquals",
        [] (RPNElement & out, const Field & value)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, true);
            return true;
        }
    },
    {
        "in",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "notIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "globalIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "globalNotIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "nullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "notNullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "globalNullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            return true;
        }
    },
    {
        "globalNotNullIn",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            return true;
        }
    },
    {
        "empty",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range("");
            return true;
        }
    },
    {
        "notEmpty",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = Range("");
            return true;
        }
    },
    {
        "like",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            String prefix = extractFixedPrefixFromLikePattern(value.safeGet<const String &>(), /*requires_perfect_prefix*/ false);
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    },
    {
        "notLike",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            String prefix = extractFixedPrefixFromLikePattern(value.safeGet<const String &>(), /*requires_perfect_prefix*/ true);
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    },
    {
        "startsWith",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            String prefix = value.safeGet<const String &>();
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    },
    {
        "match",
        [] (RPNElement & out, const Field & value)
        {
            if (value.getType() != Field::Types::String)
                return false;

            const String & expression = value.safeGet<const String &>();

            /// This optimization can't process alternation - this would require
            /// a comprehensive parsing of regular expression.
            if (expression.contains('|'))
                return false;

            String prefix = extractFixedPrefixFromRegularExpression(expression);
            if (prefix.empty())
                return false;

            String right_bound = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);

            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = !right_bound.empty()
                ? Range(prefix, true, right_bound, false)
                : Range::createLeftBounded(prefix, true);

            return true;
        }
    },
    {
        "isNotNull",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IS_NOT_NULL;
            // isNotNull means (-Inf, +Inf)
            out.range = Range::createWholeUniverseWithoutNull();
            return true;
        }
    },
    {
        "isNull",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_IS_NULL;
            // isNull means +Inf (NULLS_LAST) or -Inf (NULLS_FIRST), We don't support discrete
            // ranges, instead will use the inverse of (-Inf, +Inf). The inversion happens in
            // checkInHyperrectangle.
            out.range = Range::createWholeUniverseWithoutNull();
            return true;
        }
    },
    {
        "pointInPolygon",
        [] (RPNElement & out, const Field &)
        {
            out.function = RPNElement::FUNCTION_POINT_IN_POLYGON;
            return true;
        }
    }
};

static const std::set<std::string_view> always_relaxed_atom_functions = {"match"};
static const std::set<KeyCondition::RPNElement::Function> always_relaxed_atom_elements
    = {KeyCondition::RPNElement::FUNCTION_UNKNOWN, KeyCondition::RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE, KeyCondition::RPNElement::FUNCTION_POINT_IN_POLYGON};

/// Functions with range inversion cannot be relaxed. It will become stricter instead.
/// For example:
/// create table test(d Date, k Int64, s String) Engine=MergeTree order by toYYYYMM(d);
/// insert into test values ('2020-01-01', 1, '');
/// insert into test values ('2020-01-02', 1, '');
/// select * from test where d != '2020-01-01'; -- If relaxed, no record will return
static const std::set<std::string_view> no_relaxed_atom_functions
    = {"notLike", "notIn", "globalNotIn", "notNullIn", "globalNotNullIn", "notEquals", "notEmpty"};

static const std::map<std::string, std::string> inverse_relations =
{
    {"equals", "notEquals"},
    {"notEquals", "equals"},
    {"less", "greaterOrEquals"},
    {"greaterOrEquals", "less"},
    {"greater", "lessOrEquals"},
    {"lessOrEquals", "greater"},
    {"in", "notIn"},
    {"notIn", "in"},
    {"globalIn", "globalNotIn"},
    {"globalNotIn", "globalIn"},
    {"nullIn", "notNullIn"},
    {"notNullIn", "nullIn"},
    {"globalNullIn", "globalNotNullIn"},
    {"globalNullNotIn", "globalNullIn"},
    {"isNull", "isNotNull"},
    {"isNotNull", "isNull"},
    {"like", "notLike"},
    {"notLike", "like"},
    {"ilike", "notILike"},
    {"notILike", "ilike"},
    {"empty", "notEmpty"},
    {"notEmpty", "empty"},
};


static bool isLogicalOperator(const String & func_name)
{
    return (func_name == "and" || func_name == "or" || func_name == "not" || func_name == "indexHint");
}

/// The node can be one of:
///   - Logical operator (AND, OR, NOT and indexHint() - logical NOOP)
///   - An "atom" (relational operator, constant, expression)
///   - A logical constant expression
///   - Any other function
ASTPtr cloneASTWithInversionPushDown(const ASTPtr node, const bool need_inversion = false)
{
    const ASTFunction * func = node->as<ASTFunction>();

    if (func && isLogicalOperator(func->name))
    {
        if (func->name == "not")
        {
            return cloneASTWithInversionPushDown(func->arguments->children.front(), !need_inversion);
        }

        const auto result_node = makeASTFunction(func->name);

        /// indexHint() is a special case - logical NOOP function
        if (result_node->name != "indexHint" && need_inversion)
        {
            result_node->name = (result_node->name == "and") ? "or" : "and";
        }

        if (func->arguments)
        {
            for (const auto & child : func->arguments->children)
            {
                result_node->arguments->children.push_back(cloneASTWithInversionPushDown(child, need_inversion));
            }
        }

        return result_node;
    }

    auto cloned_node = node->clone();

    if (func && inverse_relations.find(func->name) != inverse_relations.cend())
    {
        if (need_inversion)
        {
            cloned_node->as<ASTFunction>()->name = inverse_relations.at(func->name);
        }

        return cloned_node;
    }

    return need_inversion ? makeASTFunction("not", cloned_node) : cloned_node;
}

static const ActionsDAG::Node & cloneASTWithInversionPushDown(
    const ActionsDAG::Node & node,
    ActionsDAG & inverted_dag,
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> to_inverted,
    const ContextPtr & context,
    const bool need_inversion)
{
    {
        auto it = to_inverted.find(&node);
        if (it != to_inverted.end())
            return *it->second;
    }

    const ActionsDAG::Node * res = nullptr;
    bool handled_inversion = false;

    switch (node.type)
    {
        case (ActionsDAG::ActionType::INPUT):
        {
            /// Note: inputs order is not important here. Will match columns by names.
            res = &inverted_dag.addInput({node.column, node.result_type, node.result_name});
            break;
        }
        case (ActionsDAG::ActionType::COLUMN):
        {
            String name;
            if (const auto * column_const = typeid_cast<const ColumnConst *>(node.column.get()))
                /// Re-generate column name for constant.
                /// DAG form query (with enabled analyzer) uses suffixes for constants, like 1_UInt8.
                /// DAG from PK does not use it. This breaks matching by column name sometimes.
                /// Ideally, we should not compare names, but DAG subtrees instead.
                name = ASTLiteral(column_const->getDataColumn()[0]).getColumnName();
            else
                name = node.result_name;

            res = &inverted_dag.addColumn({node.column, node.result_type, name});
            break;
        }
        case (ActionsDAG::ActionType::ALIAS):
        {
            /// Ignore aliases
            res = &cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, need_inversion);
            handled_inversion = true;
            break;
        }
        case (ActionsDAG::ActionType::ARRAY_JOIN):
        {
            const auto & arg = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, false);
            res = &inverted_dag.addArrayJoin(arg, {});
            break;
        }
        case (ActionsDAG::ActionType::FUNCTION):
        {
            auto name = node.function_base->getName();
            if (name == "not")
            {
                res = &cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, !need_inversion);
                handled_inversion = true;
            }
            else if (name == "indexHint")
            {
                ActionsDAG::NodeRawConstPtrs children;
                if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get()))
                {
                    if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                    {
                        const auto & index_hint_dag = index_hint->getActions();
                        children = index_hint_dag.getOutputs();

                        for (auto & arg : children)
                            arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, need_inversion);
                    }
                }

                res = &inverted_dag.addFunction(node.function_base, children, "");
                handled_inversion = true;
            }
            else if (need_inversion && (name == "and" || name == "or"))
            {
                ActionsDAG::NodeRawConstPtrs children(node.children);

                for (auto & arg : children)
                    arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, need_inversion);

                FunctionOverloadResolverPtr function_builder;

                if (name == "and")
                    function_builder = FunctionFactory::instance().get("or", context);
                else if (name == "or")
                    function_builder = FunctionFactory::instance().get("and", context);

                assert(function_builder);

                /// We match columns by name, so it is important to fill name correctly.
                /// So, use empty string to make it automatically.
                res = &inverted_dag.addFunction(function_builder, children, "");
                handled_inversion = true;
            }
            else
            {
                ActionsDAG::NodeRawConstPtrs children(node.children);

                for (auto & arg : children)
                    arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, false);

                auto it = inverse_relations.find(name);
                if (it != inverse_relations.end())
                {
                    const auto & func_name = need_inversion ? it->second : it->first;
                    auto function_builder = FunctionFactory::instance().get(func_name, context);
                    res = &inverted_dag.addFunction(function_builder, children, "");
                    handled_inversion = true;
                }
                else
                {
                    /// Argument types could change slightly because of our transformations, e.g.
                    /// LowCardinality can be added because some subexpressions became constant
                    /// (in particular, sets). If that happens, re-run function overload resolver.
                    /// Otherwise don't re-run it because some functions may not be available
                    /// through FunctionFactory::get(), e.g. FunctionCapture.
                    bool types_changed = false;
                    for (size_t i = 0; i < children.size(); ++i)
                    {
                        if (!node.children[i]->result_type->equals(*children[i]->result_type))
                        {
                            types_changed = true;
                            break;
                        }
                    }

                    if (types_changed)
                    {
                        auto function_builder = FunctionFactory::instance().get(name, context);
                        res = &inverted_dag.addFunction(function_builder, children, "");
                    }
                    else
                    {
                        res = &inverted_dag.addFunction(node.function_base, children, "");
                    }
                }
            }
        }
    }

    if (!handled_inversion && need_inversion)
        res = &inverted_dag.addFunction(FunctionFactory::instance().get("not", context), {res}, "");

    to_inverted[&node] = res;
    return *res;
}

const std::unordered_map<String, KeyCondition::SpaceFillingCurveType> KeyCondition::space_filling_curve_name_to_type {
    {"mortonEncode", SpaceFillingCurveType::Morton},
    {"hilbertEncode", SpaceFillingCurveType::Hilbert}
};

ActionsDAG KeyCondition::cloneASTWithInversionPushDown(ActionsDAG::NodeRawConstPtrs nodes, const ContextPtr & context)
{
    ActionsDAG res;

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> to_inverted;

    for (auto & node : nodes)
        node = &DB::cloneASTWithInversionPushDown(*node, res, to_inverted, context, false);

    if (nodes.size() > 1)
    {
        auto function_builder = FunctionFactory::instance().get("and", context);
        nodes = {&res.addFunction(function_builder, std::move(nodes), "")};
    }

    res.getOutputs().swap(nodes);

    return res;
}

/** Calculate expressions, that depend only on constants.
  * For index to work when something like "WHERE Date = toDate(now())" is written.
  */
Block KeyCondition::getBlockWithConstants(
    const ASTPtr & query, const TreeRewriterResultPtr & syntax_analyzer_result, ContextPtr context)
{
    Block result
    {
        { DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }
    };

    if (syntax_analyzer_result)
    {
        auto actions = ExpressionAnalyzer(query, syntax_analyzer_result, context).getConstActionsDAG();
        for (const auto & action_node : actions.getOutputs())
        {
            if (action_node->column)
                result.insert(ColumnWithTypeAndName{action_node->column, action_node->result_type, action_node->result_name});
        }
    }

    return result;
}

static NameSet getAllSubexpressionNames(const ExpressionActions & key_expr)
{
    NameSet names;
    for (const auto & action : key_expr.getActions())
        names.insert(action.node->result_name);

    return names;
}

void KeyCondition::getAllSpaceFillingCurves()
{
    /// So far the only supported function is mortonEncode and hilbertEncode (Morton and Hilbert curves).

    for (const auto & action : key_expr->getActions())
    {
        if (action.node->type == ActionsDAG::ActionType::FUNCTION
            && action.node->children.size() >= 2
            && space_filling_curve_name_to_type.contains(action.node->function_base->getName()))
        {
            SpaceFillingCurveDescription curve;
            curve.function_name = action.node->function_base->getName();
            curve.type = space_filling_curve_name_to_type.at(curve.function_name);
            curve.key_column_pos = key_columns.at(action.node->result_name);
            for (const auto & child : action.node->children)
            {
                /// All arguments should be regular input columns.
                if (child->type == ActionsDAG::ActionType::INPUT)
                {
                    curve.arguments.push_back(child->result_name);
                }
                else
                {
                    curve.arguments.clear();
                    break;
                }
            }

            /// So far we only support the case of two arguments.
            if (2 == curve.arguments.size())
                key_space_filling_curves.push_back(std::move(curve));
        }
    }
}

KeyCondition::KeyCondition(
    const ActionsDAG * filter_dag,
    ContextPtr context,
    const Names & key_column_names_,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    , single_point(single_point_)
{
    size_t key_index = 0;
    for (const auto & name : key_column_names_)
    {
        if (!key_columns.contains(name))
        {
            key_columns[name] = key_columns.size();
            key_indices.push_back(key_index);
        }
        ++key_index;
    }

    if (context->getSettingsRef()[Setting::analyze_index_with_space_filling_curves])
        getAllSpaceFillingCurves();

    if (!filter_dag)
    {
        has_filter = false;
        relaxed = true;
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    has_filter = true;

    /** When non-strictly monotonic functions are employed in functional index (e.g. ORDER BY toStartOfHour(dateTime)),
      * the use of NOT operator in predicate will result in the indexing algorithm leave out some data.
      * This is caused by rewriting in KeyCondition::tryParseAtomFromAST of relational operators to less strict
      * when parsing the AST into internal RPN representation.
      * To overcome the problem, before parsing the AST we transform it to its semantically equivalent form where all NOT's
      * are pushed down and applied (when possible) to leaf nodes.
      */
    auto inverted_dag = cloneASTWithInversionPushDown({filter_dag->getOutputs().at(0)}, context);
    assert(inverted_dag.getOutputs().size() == 1);

    const auto * inverted_dag_filter_node = inverted_dag.getOutputs()[0];

    RPNBuilder<RPNElement> builder(inverted_dag_filter_node, context, [&](const RPNBuilderTreeNode & node, RPNElement & out)
    {
        return extractAtomFromTree(node, out);
    });

    rpn = std::move(builder).extractRPN();

    findHyperrectanglesForArgumentsOfSpaceFillingCurves();

    if (std::any_of(rpn.begin(), rpn.end(), [&](const auto & elem) { return always_relaxed_atom_elements.contains(elem.function); }))
        relaxed = true;
}

bool KeyCondition::addCondition(const String & column, const Range & range)
{
    if (!key_columns.contains(column))
        return false;
    rpn.emplace_back(RPNElement::FUNCTION_IN_RANGE, key_columns[column], range);
    rpn.emplace_back(RPNElement::FUNCTION_AND);
    return true;
}

bool KeyCondition::getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type)
{
    RPNBuilderTreeContext tree_context(nullptr, block_with_constants, nullptr);
    RPNBuilderTreeNode node(expr.get(), tree_context);

    return node.tryGetConstant(out_value, out_type);
}


static Field applyFunctionForField(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    ColumnsWithTypeAndName columns
    {
        { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
    };

    auto col = func->execute(columns, func->getResultType(), 1);
    return (*col)[0];
}

/// applyFunction will execute the function with one `field` or the column which `field` refers to.
static FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    chassert(func != nullptr);
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    /// We will cache the function result inside `field.columns`, because this function will call many times
    /// from many fields from same column. When the column is huge, for example there are thousands of marks, we need a cache.
    /// The cache key is like `_[function_pointer]_[param_column_id]` to identify a unique <function, param> pair.
    WriteBufferFromOwnString buf;
    writeText("_", buf);
    writePointerHex(func.get(), buf);
    writeText("_" + toString(field.column_idx), buf);
    String result_name = buf.str();
    const auto & columns = field.columns;
    size_t result_idx = columns->size();

    for (size_t i = 0; i < result_idx; ++i)
    {
        if ((*columns)[i].name == result_name)
            result_idx = i;
    }

    if (result_idx == columns->size())
    {
        /// When cache is missed, we calculate the whole column where the field comes from. This will avoid repeated calculation.
        ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
        field.columns->emplace_back(ColumnWithTypeAndName {nullptr, func->getResultType(), result_name});
        (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
    }

    return {field.columns, field.row_idx, result_idx};
}

/// Sequentially applies functions to the column, returns `true`
/// if all function arguments are compatible with functions
/// signatures, and none of the functions produce `NULL` output.
///
/// After functions chain execution, fills result column and its type.
bool applyFunctionChainToColumn(
    const ColumnPtr & in_column,
    const DataTypePtr & in_data_type,
    const std::vector<FunctionBasePtr> & functions,
    ColumnPtr & out_column,
    DataTypePtr & out_data_type)
{
    // Remove LowCardinality from input column, and convert it to regular one
    auto result_column = in_column->convertToFullIfNeeded();
    auto result_type = removeLowCardinality(in_data_type);

    // In case function sequence is empty, return full non-LowCardinality column
    if (functions.empty())
    {
        out_column = result_column;
        out_data_type = result_type;
        return true;
    }

    // If first function arguments are empty, cannot transform input column
    if (functions[0]->getArgumentTypes().empty())
    {
        return false;
    }

    // And cast it to the argument type of the first function in the chain
    auto in_argument_type = functions[0]->getArgumentTypes()[0];
    if (canBeSafelyCasted(result_type, in_argument_type))
    {
        result_column = castColumnAccurate({result_column, result_type, ""}, in_argument_type);
        result_type = in_argument_type;
    }
    // If column cannot be casted accurate, casting with OrNull, and in case all
    // values has been casted (no nulls), unpacking nested column from nullable.
    // In case any further functions require Nullable input, they'll be able
    // to cast it.
    else
    {
        result_column = castColumnAccurateOrNull({result_column, result_type, ""}, in_argument_type);
        const auto & result_column_nullable = assert_cast<const ColumnNullable &>(*result_column);
        const auto & null_map_data = result_column_nullable.getNullMapData();
        for (char8_t i : null_map_data)
        {
            if (i != 0)
                return false;
        }
        result_column = result_column_nullable.getNestedColumnPtr();
        result_type = removeNullable(in_argument_type);
    }

    for (const auto & func : functions)
    {
        if (func->getArgumentTypes().empty())
            return false;

        auto argument_type = func->getArgumentTypes()[0];
        if (!canBeSafelyCasted(result_type, argument_type))
            return false;

        result_column = castColumnAccurate({result_column, result_type, ""}, argument_type);
        result_column = func->execute({{result_column, argument_type, ""}}, func->getResultType(), result_column->size());
        result_type = func->getResultType();

        // Transforming nullable columns to the nested ones, in case no nulls found
        if (result_column->isNullable())
        {
            const auto & result_column_nullable = assert_cast<const ColumnNullable &>(*result_column);
            const auto & null_map_data = result_column_nullable.getNullMapData();
            for (char8_t i : null_map_data)
            {
                if (i != 0)
                    return false;
            }
            result_column = result_column_nullable.getNestedColumnPtr();
            result_type = removeNullable(func->getResultType());
        }
    }
    out_column = result_column;
    out_data_type = result_type;

    return true;
}

bool KeyCondition::canConstantBeWrappedByMonotonicFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node.getColumnName();

    if (array_joined_column_names.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
        return false;

    if (out_value.isNull())
        return false;

    MonotonicFunctionsChain transform_functions;
    auto can_transform_constant = extractMonotonicFunctionsChainFromKey(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_column_type,
        transform_functions,
        [](const IFunctionBase & func, const IDataType & type)
        {
            if (!func.hasInformationAboutMonotonicity())
                return false;

            /// Range is irrelevant in this case.
            auto monotonicity = func.getMonotonicityForRange(type, Field(), Field());
            if (!monotonicity.is_always_monotonic)
                return false;

            return true;
        });

    if (!can_transform_constant)
        return false;

    auto const_column = out_type->createColumnConst(1, out_value);

    ColumnPtr transformed_const_column;
    DataTypePtr transformed_const_type;
    bool constant_transformed = applyFunctionChainToColumn(
        const_column,
        out_type,
        transform_functions,
        transformed_const_column,
        transformed_const_type);

    if (!constant_transformed)
        return false;

    out_value = (*transformed_const_column)[0];
    out_type = transformed_const_type;
    return true;
}

/// Looking for possible transformation of `column = constant` into `partition_expr = function(constant)`
bool KeyCondition::canConstantBeWrappedByFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node.getColumnName();

    if (array_joined_column_names.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
    {
        /// Let's check another one case.
        /// If our storage was created with moduloLegacy in partition key,
        /// We can assume that `modulo(...) = const` is the same as `moduloLegacy(...) = const`.
        /// Replace modulo to moduloLegacy in AST and check if we also have such a column.
        ///
        /// We do not check this in canConstantBeWrappedByMonotonicFunctions.
        /// The case `f(modulo(...))` for totally monotonic `f ` is considered to be rare.
        ///
        /// Note: for negative values, we can filter more partitions then needed.
        expr_name = node.getColumnNameWithModuloLegacy();

        if (!key_subexpr_names.contains(expr_name))
            return false;
    }

    if (out_value.isNull())
        return false;

    MonotonicFunctionsChain transform_functions;
    auto can_transform_constant = extractMonotonicFunctionsChainFromKey(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_column_type,
        transform_functions,
        [](const IFunctionBase & func, const IDataType &) { return func.isDeterministic(); });

    if (!can_transform_constant)
        return false;

    auto const_column = out_type->createColumnConst(1, out_value);

    ColumnPtr transformed_const_column;
    DataTypePtr transformed_const_type;
    bool constant_transformed = applyFunctionChainToColumn(
        const_column,
        out_type,
        transform_functions,
        transformed_const_column,
        transformed_const_type);

    if (!constant_transformed)
        return false;

    out_value = (*transformed_const_column)[0];
    out_type = transformed_const_type;
    return true;
}

bool KeyCondition::tryPrepareSetIndex(
    const RPNBuilderFunctionTreeNode & func,
    RPNElement & out,
    size_t & out_key_column_num,
    bool & is_constant_transformed)
{
    const auto & left_arg = func.getArgumentAt(0);

    out_key_column_num = 0;
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;
    std::vector<MonotonicFunctionsChain> set_transforming_chains;
    DataTypes data_types;

    auto get_key_tuple_position_mapping = [&](const RPNBuilderTreeNode & node, size_t tuple_index)
    {
        MergeTreeSetIndex::KeyTuplePositionMapping index_mapping;
        index_mapping.tuple_index = tuple_index;
        DataTypePtr data_type;
        std::optional<size_t> key_space_filling_curve_argument_pos;
        MonotonicFunctionsChain set_transforming_chain;
        if (isKeyPossiblyWrappedByMonotonicFunctions(
                node, index_mapping.key_index, key_space_filling_curve_argument_pos, data_type, index_mapping.functions)
            && !key_space_filling_curve_argument_pos) /// We don't support the analysis of space-filling curves and IN set.
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            out_key_column_num = std::max(out_key_column_num, index_mapping.key_index);
            set_transforming_chains.push_back(set_transforming_chain);
        }
        // For partition index, checking if set can be transformed to prune any partitions
        else if (single_point && canSetValuesBeWrappedByFunctions(node, index_mapping.key_index, data_type, set_transforming_chain))
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            out_key_column_num = std::max(out_key_column_num, index_mapping.key_index);
            set_transforming_chains.push_back(set_transforming_chain);
        }
    };

    size_t left_args_count = 1;
    if (left_arg.isFunction())
    {
        /// Note: in case of ActionsDAG, tuple may be a constant.
        /// In this case, there is no keys in tuple. So, we don't have to check it.
        auto left_arg_tuple = left_arg.toFunctionNode();
        if (left_arg_tuple.getFunctionName() == "tuple" && left_arg_tuple.getArgumentsSize() > 1)
        {
            left_args_count = left_arg_tuple.getArgumentsSize();
            for (size_t i = 0; i < left_args_count; ++i)
                get_key_tuple_position_mapping(left_arg_tuple.getArgumentAt(i), i);
        }
        else
        {
            get_key_tuple_position_mapping(left_arg, 0);
        }
    }
    else
    {
        get_key_tuple_position_mapping(left_arg, 0);
    }

    if (indexes_mapping.empty())
        return false;

    const auto right_arg = func.getArgumentAt(1);

    auto future_set = right_arg.tryGetPreparedSet();
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(right_arg.getTreeContext().getQueryContext());
    if (!prepared_set)
        return false;

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    /** Try to convert set columns to primary key columns.
      * Example: SELECT id FROM test_table WHERE id IN (SELECT 1);
      * In this example table `id` column has type UInt64, Set column has type UInt8. To use index
      * we need to convert set column to primary key column.
      */
    auto set_columns = prepared_set->getSetElements();
    auto set_types = future_set->getTypes();
    {
        Columns new_columns;
        DataTypes new_types;
        while (set_columns.size() < left_args_count) /// If we have an unpacked tuple inside, we unpack it
        {
            bool has_tuple = false;
            for (size_t i = 0; i < set_columns.size(); ++i)
            {
                if (isTuple(set_types[i]))
                {
                    has_tuple = true;
                    auto columns_tuple = assert_cast<const ColumnTuple*>(set_columns[i].get())->getColumns();
                    auto subtypes = assert_cast<const DataTypeTuple&>(*set_types[i]).getElements();
                    new_columns.insert(new_columns.end(), columns_tuple.begin(), columns_tuple.end());
                    new_types.insert(new_types.end(), subtypes.begin(), subtypes.end());
                }
                else
                {
                    new_columns.push_back(set_columns[i]);
                    new_types.push_back(set_types[i]);
                }
            }
            if (!has_tuple)
                return false;

            set_columns.swap(new_columns);
            set_types.swap(new_types);
            new_columns.clear();
            new_types.clear();
        }
    }
    size_t set_types_size = set_types.size();
    size_t indexes_mapping_size = indexes_mapping.size();
    assert(set_types_size == set_columns.size());

    Columns transformed_set_columns = set_columns;

    for (size_t indexes_mapping_index = 0; indexes_mapping_index < indexes_mapping_size; ++indexes_mapping_index)
    {
        const auto & key_column_type = data_types[indexes_mapping_index];
        size_t set_element_index = indexes_mapping[indexes_mapping_index].tuple_index;
        auto set_element_type = set_types[set_element_index];
        ColumnPtr set_column = set_columns[set_element_index];

        if (!set_transforming_chains[indexes_mapping_index].empty())
        {
            ColumnPtr transformed_set_column;
            DataTypePtr transformed_set_type;
            if (!applyFunctionChainToColumn(
                    set_column,
                    set_element_type,
                    set_transforming_chains[indexes_mapping_index],
                    transformed_set_column,
                    transformed_set_type))
                return false;

            set_column = transformed_set_column;
            set_element_type = transformed_set_type;
            is_constant_transformed = true;
        }

        if (canBeSafelyCasted(set_element_type, key_column_type))
        {
            transformed_set_columns[set_element_index] = castColumn({set_column, set_element_type, {}}, key_column_type);
            continue;
        }

        if (!key_column_type->canBeInsideNullable())
            return false;

        const NullMap * set_column_null_map = nullptr;

        // Keep a reference to the original set_column to ensure the data remains valid
        ColumnPtr original_set_column = set_column;

        if (isNullableOrLowCardinalityNullable(set_element_type))
        {
            if (WhichDataType(set_element_type).isLowCardinality())
            {
                set_element_type = removeLowCardinality(set_element_type);
                transformed_set_columns[set_element_index] = set_column->convertToFullColumnIfLowCardinality();
            }

            set_element_type = removeNullable(set_element_type);

            // Obtain the nullable column without reassigning set_column immediately
            const auto * set_column_nullable = typeid_cast<const ColumnNullable *>(transformed_set_columns[set_element_index].get());
            if (!set_column_nullable)
                return false;

            const NullMap & null_map_data = set_column_nullable->getNullMapData();
            if (!null_map_data.empty())
                set_column_null_map = &null_map_data;

            ColumnPtr nested_column = set_column_nullable->getNestedColumnPtr();

            // Reassign set_column after we have obtained necessary references
            set_column = nested_column;
        }

        ColumnPtr nullable_set_column = castColumnAccurateOrNull({set_column, set_element_type, {}}, key_column_type);
        const auto * nullable_set_column_typed = typeid_cast<const ColumnNullable *>(nullable_set_column.get());
        if (!nullable_set_column_typed)
            return false;

        const NullMap & nullable_set_column_null_map = nullable_set_column_typed->getNullMapData();
        size_t nullable_set_column_null_map_size = nullable_set_column_null_map.size();

        IColumn::Filter filter(nullable_set_column_null_map_size);

        if (set_column_null_map)
        {
            for (size_t i = 0; i < nullable_set_column_null_map_size; ++i)
            {
                if (nullable_set_column_null_map_size < set_column_null_map->size())
                    filter[i] = (*set_column_null_map)[i] || !nullable_set_column_null_map[i];
                else
                    filter[i] = !nullable_set_column_null_map[i];
            }

            set_column = nullable_set_column_typed->filter(filter, 0);
        }
        else
        {
            for (size_t i = 0; i < nullable_set_column_null_map_size; ++i)
                filter[i] = !nullable_set_column_null_map[i];

            set_column = nullable_set_column_typed->getNestedColumn().filter(filter, 0);
        }

        transformed_set_columns[set_element_index] = std::move(set_column);
    }

    set_columns = std::move(transformed_set_columns);

    out.set_index = std::make_shared<MergeTreeSetIndex>(set_columns, std::move(indexes_mapping));

    /// When not all key columns are used or when there are multiple elements in
    /// the set, the atom's hyperrectangle is expanded to encompass the missing
    /// dimensions and any "gaps".
    if (indexes_mapping_size < set_types_size || out.set_index->size() > 1)
        relaxed = true;

    return true;
}


/** Allow to use two argument function with constant argument to be analyzed as a single argument function.
  * In other words, it performs "currying" (binding of arguments).
  * This is needed, for example, to support correct analysis of `toDate(time, 'UTC')`.
  */
class FunctionWithOptionalConstArg : public IFunctionBase
{
public:
    enum Kind
    {
        NO_CONST = 0,
        LEFT_CONST,
        RIGHT_CONST,
    };

    explicit FunctionWithOptionalConstArg(const FunctionBasePtr & func_) : func(func_) {}
    FunctionWithOptionalConstArg(const FunctionBasePtr & func_, const ColumnWithTypeAndName & const_arg_, Kind kind_)
        : func(func_), const_arg(const_arg_), kind(kind_)
    {
    }

    String getName() const override { return func->getName(); }

    const DataTypes & getArgumentTypes() const override { return func->getArgumentTypes(); }

    const DataTypePtr & getResultType() const override { return func->getResultType(); }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const override { return func->prepare(arguments); }

    ColumnPtr
    execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const override
    {
        if (kind == Kind::LEFT_CONST)
        {
            ColumnsWithTypeAndName new_arguments;
            new_arguments.reserve(arguments.size() + 1);
            new_arguments.push_back(const_arg);
            new_arguments.front().column = new_arguments.front().column->cloneResized(input_rows_count);
            for (const auto & arg : arguments)
                new_arguments.push_back(arg);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        if (kind == Kind::RIGHT_CONST)
        {
            auto new_arguments = arguments;
            new_arguments.push_back(const_arg);
            new_arguments.back().column = new_arguments.back().column->cloneResized(input_rows_count);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        return func->prepare(arguments)->execute(arguments, result_type, input_rows_count, dry_run);
    }

    bool isDeterministic() const override { return func->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return func->isDeterministicInScopeOfQuery(); }

    bool hasInformationAboutMonotonicity() const override { return func->hasInformationAboutMonotonicity(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override { return func->isSuitableForShortCircuitArgumentsExecution(arguments); }

    IFunctionBase::Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return func->getMonotonicityForRange(type, left, right);
    }

    Kind getKind() const { return kind; }
    const ColumnWithTypeAndName & getConstArg() const { return const_arg; }

private:
    FunctionBasePtr func;
    ColumnWithTypeAndName const_arg;
    Kind kind = Kind::NO_CONST;
};


bool KeyCondition::isKeyPossiblyWrappedByMonotonicFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    std::optional<size_t> & out_argument_num_of_space_filling_curve,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain,
    bool assume_function_monotonicity)
{
    std::vector<RPNBuilderFunctionTreeNode> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        node, out_key_column_num, out_argument_num_of_space_filling_curve, key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        auto function = *it;
        auto func_builder = FunctionFactory::instance().tryGet(function.getFunctionName(), node.getTreeContext().getQueryContext());
        if (!func_builder)
            return false;
        ColumnsWithTypeAndName arguments;
        ColumnWithTypeAndName const_arg;
        FunctionWithOptionalConstArg::Kind kind = FunctionWithOptionalConstArg::Kind::NO_CONST;
        if (function.getArgumentsSize() == 2)
        {
            if (function.getArgumentAt(0).isConstant())
            {
                const_arg = function.getArgumentAt(0).getConstantColumn();
                arguments.push_back(const_arg);
                arguments.push_back({ nullptr, key_column_type, "" });
                kind = FunctionWithOptionalConstArg::Kind::LEFT_CONST;
            }
            else if (function.getArgumentAt(1).isConstant())
            {
                arguments.push_back({ nullptr, key_column_type, "" });
                const_arg = function.getArgumentAt(1).getConstantColumn();
                arguments.push_back(const_arg);
                kind = FunctionWithOptionalConstArg::Kind::RIGHT_CONST;
            }

            /// If constant arg of binary operator is NULL, there will be no monotonicity.
            if (const_arg.column->isNullAt(0))
                return false;
        }
        else
            arguments.push_back({ nullptr, key_column_type, "" });
        auto func = func_builder->build(arguments);

        if (!func || !func->isDeterministicInScopeOfQuery() || (!assume_function_monotonicity && !func->hasInformationAboutMonotonicity()))
            return false;

        key_column_type = func->getResultType();
        if (kind == FunctionWithOptionalConstArg::Kind::NO_CONST)
            out_functions_chain.push_back(func);
        else
            out_functions_chain.push_back(std::make_shared<FunctionWithOptionalConstArg>(func, const_arg, kind));
    }

    out_key_res_column_type = key_column_type;

    return true;
}

bool KeyCondition::isKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    std::optional<size_t> & out_argument_num_of_space_filling_curve,
    DataTypePtr & out_key_column_type,
    std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = key_expr->getSampleBlock();

    /// Key columns should use canonical names for the index analysis.
    String name = node.getColumnName();

    if (array_joined_column_names.contains(name))
        return false;

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(name).type;
        return true;
    }

    /** The case of space-filling curves.
      * When the node is not a key column (e.g. mortonEncode(x, y))
      * but one of the arguments of a key column (e.g. x or y).
      *
      * For example, the table has ORDER BY mortonEncode(x, y)
      * and query has WHERE x >= 10 AND x < 15 AND y > 20 AND y <= 25
      */
    for (const auto & curve : key_space_filling_curves)
    {
        for (size_t i = 0, size = curve.arguments.size(); i < size; ++i)
        {
            if (curve.arguments[i] == name)
            {
                out_key_column_num = curve.key_column_pos;
                out_argument_num_of_space_filling_curve = i;
                out_key_column_type = sample_block.getByName(name).type;
                return true;
            }
        }
    }

    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();

        size_t arguments_size = function_node.getArgumentsSize();
        if (arguments_size > 2 || arguments_size == 0)
            return false;

        out_functions_chain.push_back(function_node);

        bool result = false;
        if (arguments_size == 2)
        {
            if (function_node.getArgumentAt(0).isConstant())
            {
                result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(
                    function_node.getArgumentAt(1),
                    out_key_column_num,
                    out_argument_num_of_space_filling_curve,
                    out_key_column_type,
                    out_functions_chain);
            }
            else if (function_node.getArgumentAt(1).isConstant())
            {
                result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(
                    function_node.getArgumentAt(0),
                    out_key_column_num,
                    out_argument_num_of_space_filling_curve,
                    out_key_column_type,
                    out_functions_chain);
            }
        }
        else
        {
            result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(
                function_node.getArgumentAt(0),
                out_key_column_num,
                out_argument_num_of_space_filling_curve,
                out_key_column_type,
                out_functions_chain);
        }

        return result;
    }

    return false;
}

/** When table's key has expression with these functions from a column,
  * and when a column in a query is compared with a constant, such as:
  * CREATE TABLE (x String) ORDER BY toDate(x)
  * SELECT ... WHERE x LIKE 'Hello%'
  * we want to apply the function to the constant for index analysis,
  * but should modify it to pass on un-parsable values.
  */
static std::set<std::string_view> date_time_parsing_functions = {
    "toDate",
    "toDate32",
    "toDateTime",
    "toDateTime64",
    "parseDateTimeBestEffort",
    "parseDateTimeBestEffortUS",
    "parseDateTime32BestEffort",
    "parseDateTime64BestEffort",
    "parseDateTime",
    "parseDateTimeInJodaSyntax",
};

/** The key functional expression constraint may be inferred from a plain column in the expression.
  * For example, if the key contains `toStartOfHour(Timestamp)` and query contains `WHERE Timestamp >= now()`,
  * it can be assumed that if `toStartOfHour()` is monotonic on [now(), inf), the `toStartOfHour(Timestamp) >= toStartOfHour(now())`
  * condition also holds, so the index may be used to select only parts satisfying this condition.
  *
  * To check the assumption, we'd need to assert that the inverse function to this transformation is also monotonic, however the
  * inversion isn't exported (or even viable for not strictly monotonic functions such as `toStartOfHour()`).
  * Instead, we can qualify only functions that do not transform the range (for example rounding),
  * which while not strictly monotonic, are monotonic everywhere on the input range.
  */
bool KeyCondition::extractMonotonicFunctionsChainFromKey(
    ContextPtr context,
    const String & expr_name,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    MonotonicFunctionsChain & out_functions_chain,
    std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const
{
    const auto & sample_block = key_expr->getSampleBlock();

    for (const auto & node : key_expr->getNodes())
    {
        auto it = key_columns.find(node.result_name);
        if (it != key_columns.end())
        {
            std::stack<const ActionsDAG::Node *> chain;

            const auto * cur_node = &node;
            bool is_valid_chain = true;

            while (is_valid_chain)
            {
                if (cur_node->result_name == expr_name)
                    break;

                chain.push(cur_node);

                if (cur_node->type == ActionsDAG::ActionType::FUNCTION && cur_node->children.size() <= 2)
                {
                    is_valid_chain = always_monotonic(*cur_node->function_base, *cur_node->result_type);

                    const ActionsDAG::Node * next_node = nullptr;
                    for (const auto * arg : cur_node->children)
                    {
                        if (arg->column && isColumnConst(*arg->column))
                            continue;

                        if (next_node)
                            is_valid_chain = false;

                        next_node = arg;
                    }

                    if (!next_node)
                        is_valid_chain = false;

                    cur_node = next_node;
                }
                else if (cur_node->type == ActionsDAG::ActionType::ALIAS)
                    cur_node = cur_node->children.front();
                else
                    is_valid_chain = false;
            }

            if (is_valid_chain)
            {
                while (!chain.empty())
                {
                    const auto * func = chain.top();
                    chain.pop();

                    if (func->type != ActionsDAG::ActionType::FUNCTION)
                        continue;

                    auto func_name = func->function_base->getName();
                    auto func_base = func->function_base;

                    ColumnsWithTypeAndName arguments;
                    ColumnWithTypeAndName const_arg;
                    FunctionWithOptionalConstArg::Kind kind = FunctionWithOptionalConstArg::Kind::NO_CONST;

                    if (date_time_parsing_functions.contains(func_name))
                    {
                        const auto & arg_types = func_base->getArgumentTypes();
                        if (!arg_types.empty() && isStringOrFixedString(arg_types[0]))
                        {
                            func_name = func_name + "OrNull";
                        }

                    }

                    auto func_builder = FunctionFactory::instance().tryGet(func_name, context);

                    if (func->children.size() == 1)
                    {
                        arguments.push_back({nullptr, removeLowCardinality(func->children[0]->result_type), ""});
                    }
                    else if (func->children.size() == 2)
                    {
                        const auto * left = func->children[0];
                        const auto * right = func->children[1];
                        if (left->column && isColumnConst(*left->column))
                        {
                            const_arg = {left->result_type->createColumnConst(0, (*left->column)[0]), left->result_type, ""};
                            arguments.push_back(const_arg);
                            arguments.push_back({nullptr, removeLowCardinality(right->result_type), ""});
                            kind = FunctionWithOptionalConstArg::Kind::LEFT_CONST;
                        }
                        else
                        {
                            const_arg = {right->result_type->createColumnConst(0, (*right->column)[0]), right->result_type, ""};
                            arguments.push_back({nullptr, removeLowCardinality(left->result_type), ""});
                            arguments.push_back(const_arg);
                            kind = FunctionWithOptionalConstArg::Kind::RIGHT_CONST;
                        }
                    }

                    auto out_func = func_builder->build(arguments);
                    if (kind == FunctionWithOptionalConstArg::Kind::NO_CONST)
                        out_functions_chain.push_back(out_func);
                    else
                        out_functions_chain.push_back(std::make_shared<FunctionWithOptionalConstArg>(out_func, const_arg, kind));
                }

                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                return true;
            }
        }
    }

    return false;
}

bool KeyCondition::canSetValuesBeWrappedByFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain)
{
    // Checking if column name matches any of key subexpressions
    String expr_name = node.getColumnName();

    if (array_joined_column_names.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
    {
        expr_name = node.getColumnNameWithModuloLegacy();

        if (!key_subexpr_names.contains(expr_name))
            return false;
    }

    return extractMonotonicFunctionsChainFromKey(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_res_column_type,
        out_functions_chain,
        [](const IFunctionBase & func, const IDataType &)
        {
            return func.isDeterministic();
        });
}

static void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const String & node_column_name)
{
    try
    {
        src_value = convertFieldToType(src_value, *desired_type, src_type.get());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Key expression contains comparison between inconvertible types: "
            "{} and {} inside {}", desired_type->getName(), src_type->getName(), node_column_name);
    }
}


bool KeyCondition::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    const auto * node_dag = node.getDAGNode();
    if (node_dag && node_dag->result_type->equals(DataTypeNullable(std::make_shared<DataTypeNothing>())))
    {
        /// If the inferred result type is Nullable(Nothing) at the query analysis stage,
        /// we don't analyze this node further as its condition will always be false.
        out.function = RPNElement::ALWAYS_FALSE;
        return true;
    }

    /** Functions < > = != <= >= in `notIn` isNull isNotNull, where one argument is a constant, and the other is one of columns of key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  (for example, if the table has ORDER BY time, we will check the conditions like
      *   toDate(time) = '2023-10-14', toMonth(time) = 12, etc)
      *  or any of arguments of a space-filling curve function if it is in the key,
      *  (for example, if the table has ORDER BY mortonEncode(x, y), we will check the conditions like x > c, y <= c, etc.)
      *  or constant expression - number
      *  (for example x AND 0)
      */
    Field const_value;
    DataTypePtr const_type;
    if (node.isFunction())
    {
        auto func = node.toFunctionNode();
        size_t num_args = func.getArgumentsSize();

        /// Type of expression containing key column
        DataTypePtr key_expr_type;

        /// Number of a key column (inside key_column_names array)
        size_t key_column_num = -1;

        /// For example, if the key is mortonEncode(x, y), and the atom is x, then the argument num is 0.
        std::optional<size_t> argument_num_of_space_filling_curve;

        MonotonicFunctionsChain chain;
        std::string func_name = func.getFunctionName();

        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        auto analyze_point_in_polygon = [&, this]() -> bool
        {
            /// pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)])
            if (func.getArgumentAt(0).tryGetConstant(const_value, const_type))
                return false;
            if (!func.getArgumentAt(1).tryGetConstant(const_value, const_type))
                return false;

            const auto atom_it = atom_map.find(func_name);

            /// Analyze (x, y)
            RPNElement::MultiColumnsFunctionDescription column_desc;
            column_desc.function_name = func_name;
            auto first_argument = func.getArgumentAt(0).toFunctionNode();

            if (first_argument.getArgumentsSize() != 2 || first_argument.getFunctionName() != "tuple")
                return false;

            for (size_t i = 0; i < 2; ++i)
            {
                auto name = first_argument.getArgumentAt(i).getColumnName();
                auto it = key_columns.find(name);
                if (it == key_columns.end())
                    return false;
                column_desc.key_columns.push_back(name);
                column_desc.key_column_positions.push_back(key_columns[name]);
            }
            out.point_in_polygon_column_description = column_desc;

            /// Analyze [(0, 0), (8, 4), (5, 8), (0, 2)]
            chassert(WhichDataType(const_type).isArray());
            for (const auto & elem : const_value.safeGet<Array>())
            {
                if (elem.getType() != Field::Types::Tuple)
                    return false;

                const auto & elem_tuple = elem.safeGet<Tuple>();
                if (elem_tuple.size() != 2)
                    return false;

                auto x = applyVisitor(FieldVisitorConvertToNumber<Float64>(), elem_tuple[0]);
                auto y = applyVisitor(FieldVisitorConvertToNumber<Float64>(), elem_tuple[1]);
                out.polygon.outer().push_back({x, y});
            }
            boost::geometry::correct(out.polygon);
            return atom_it->second(out, const_value);
        };

        if (always_relaxed_atom_functions.contains(func_name))
            relaxed = true;

        bool allow_constant_transformation = !no_relaxed_atom_functions.contains(func_name);
        if (num_args == 1)
        {
            if (!(isKeyPossiblyWrappedByMonotonicFunctions(
                func.getArgumentAt(0), key_column_num, argument_num_of_space_filling_curve, key_expr_type, chain)))
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");
        }
        else if (num_args == 2)
        {
            size_t key_arg_pos;           /// Position of argument with key column (non-const argument)
            bool is_set_const = false;
            bool is_constant_transformed = false;

            if (functionIsInOrGlobalInOperator(func_name))
            {
                if (tryPrepareSetIndex(func, out, key_column_num, is_constant_transformed))
                {
                    key_arg_pos = 0;
                    is_set_const = true;
                }
                else
                    return false;
            }
            else if (func_name == "pointInPolygon")
            {
                /// Case1 no holes in polygon
                return analyze_point_in_polygon();
            }
            else if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }

                if (isKeyPossiblyWrappedByMonotonicFunctions(
                        func.getArgumentAt(0),
                        key_column_num,
                        argument_num_of_space_filling_curve,
                        key_expr_type,
                        chain,
                        single_point && func_name == "equals"))
                {
                    key_arg_pos = 0;
                }
                else if (
                    allow_constant_transformation
                    && canConstantBeWrappedByMonotonicFunctions(
                        func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals"
                    && canConstantBeWrappedByFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else if (func.getArgumentAt(0).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }

                if (isKeyPossiblyWrappedByMonotonicFunctions(
                        func.getArgumentAt(1),
                        key_column_num,
                        argument_num_of_space_filling_curve,
                        key_expr_type,
                        chain,
                        single_point && func_name == "equals"))
                {
                    key_arg_pos = 1;
                }
                else if (
                    allow_constant_transformation
                    && canConstantBeWrappedByMonotonicFunctions(
                        func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 1;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals"
                    && canConstantBeWrappedByFunctions(func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");

            /// Replace <const> <sign> <data> on to <data> <-sign> <const>
            if (key_arg_pos == 1)
            {
                if (func_name == "less")
                    func_name = "greater";
                else if (func_name == "greater")
                    func_name = "less";
                else if (func_name == "greaterOrEquals")
                    func_name = "lessOrEquals";
                else if (func_name == "lessOrEquals")
                    func_name = "greaterOrEquals";
                else if (func_name == "in" || func_name == "notIn" ||
                         func_name == "like" || func_name == "notLike" ||
                         func_name == "ilike" || func_name == "notILike" ||
                         func_name == "startsWith" || func_name == "match")
                {
                    /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                    return false;
                }
            }

            key_expr_type = recursiveRemoveLowCardinality(key_expr_type);
            DataTypePtr key_expr_type_not_null;
            bool key_expr_type_is_nullable = false;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(key_expr_type.get()))
            {
                key_expr_type_is_nullable = true;
                key_expr_type_not_null = nullable_type->getNestedType();
            }
            else
                key_expr_type_not_null = key_expr_type;

            bool cast_not_needed = is_set_const /// Set args are already casted inside Set::createFromAST
                || ((isNativeInteger(key_expr_type_not_null) || isDateTime(key_expr_type_not_null))
                    && (isNativeInteger(const_type) || isDateTime(const_type))); /// Native integers and DateTime are accurately compared without cast.

            if (!cast_not_needed && !key_expr_type_not_null->equals(*const_type))
            {
                if (const_value.getType() == Field::Types::String)
                {
                    const_value = convertFieldToType(const_value, *key_expr_type_not_null);
                    if (const_value.isNull())
                        return false;
                    // No need to set is_constant_transformed because we're doing exact conversion
                }
                else
                {
                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{key_expr_type_not_null, const_type});
                    if (!common_type)
                        return false;

                    if (!const_type->equals(*common_type))
                    {
                        castValueToType(common_type, const_value, const_type, node.getColumnName());

                        // Need to set is_constant_transformed unless we're doing exact conversion
                        if (!key_expr_type_not_null->equals(*common_type))
                            is_constant_transformed = true;
                    }
                    if (!key_expr_type_not_null->equals(*common_type))
                    {
                        auto common_type_maybe_nullable = (key_expr_type_is_nullable && !common_type->isNullable())
                            ? DataTypePtr(std::make_shared<DataTypeNullable>(common_type))
                            : common_type;

                        auto func_cast = createInternalCast({key_expr_type, {}}, common_type_maybe_nullable, CastType::nonAccurate, {});

                        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
                        if (!single_point && !func_cast->hasInformationAboutMonotonicity())
                            return false;
                        chain.push_back(func_cast);
                    }
                }
            }

            /// Transformed constant must weaken the condition, for example "x > 5" must weaken to "round(x) >= 5"
            if (is_constant_transformed)
            {
                if (func_name == "less")
                    func_name = "lessOrEquals";
                else if (func_name == "greater")
                    func_name = "greaterOrEquals";

                relaxed = true;
            }

        }
        else
        {
            if (func_name == "pointInPolygon")
            {
                /// Case2 has holes in polygon, when checking skip index, the hole will be ignored.
                return analyze_point_in_polygon();
            }

            return false;
        }

        const auto atom_it = atom_map.find(func_name);

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);
        out.argument_num_of_space_filling_curve = argument_num_of_space_filling_curve;

        return atom_it->second(out, const_value);
    }
    if (node.tryGetConstant(const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`

        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        if (const_value.getType() == Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        if (const_value.getType() == Field::Types::Float64)
        {
            out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
}


void KeyCondition::findHyperrectanglesForArgumentsOfSpaceFillingCurves()
{
    /// Traverse chains of AND with conditions on arguments of a space filling curve, and construct hyperrectangles from them.
    /// For example, a chain:
    ///   x >= 10 AND x <= 20 AND y >= 20 AND y <= 30
    /// will be transformed to a single atom:
    ///   args in [10, 20] × [20, 30]

    RPN new_rpn;
    new_rpn.reserve(rpn.size());

    auto num_arguments_of_a_curve = [&](size_t key_column_pos)
    {
        for (const auto & curve : key_space_filling_curves)
            if (curve.key_column_pos == key_column_pos)
                return curve.arguments.size();
        return 0uz;
    };

    for (const auto & elem : rpn)
    {
        if (elem.function == RPNElement::FUNCTION_IN_RANGE && elem.argument_num_of_space_filling_curve.has_value())
        {
            /// A range of an argument of a space-filling curve

            size_t arg_num = *elem.argument_num_of_space_filling_curve;
            size_t curve_total_args = num_arguments_of_a_curve(elem.key_column);

            if (!curve_total_args)
            {
                /// If we didn't find a space-filling curve - replace the condition to unknown.
                new_rpn.emplace_back();
                continue;
            }

            chassert(arg_num < curve_total_args);

            /// Replace the condition to a hyperrectangle

            Hyperrectangle hyperrectangle(curve_total_args, Range::createWholeUniverseWithoutNull());
            hyperrectangle[arg_num] = elem.range;

            RPNElement collapsed_elem;
            collapsed_elem.function = RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE;
            collapsed_elem.key_column = elem.key_column;
            collapsed_elem.space_filling_curve_args_hyperrectangle = std::move(hyperrectangle);

            new_rpn.push_back(std::move(collapsed_elem));
            continue;
        }
        if (elem.function == RPNElement::FUNCTION_AND && new_rpn.size() >= 2)
        {
            /// AND of two conditions

            const auto & cond1 = new_rpn[new_rpn.size() - 2];
            const auto & cond2 = new_rpn[new_rpn.size() - 1];

            /// Related to the same column of the key, represented by a space-filling curve

            if (cond1.key_column == cond2.key_column && cond1.function == RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE
                && cond2.function == RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE)
            {
                /// Intersect these two conditions (applying AND)

                RPNElement collapsed_elem;
                collapsed_elem.function = RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE;
                collapsed_elem.key_column = cond1.key_column;
                collapsed_elem.space_filling_curve_args_hyperrectangle
                    = intersect(cond1.space_filling_curve_args_hyperrectangle, cond2.space_filling_curve_args_hyperrectangle);

                /// Replace the AND operation with its arguments to the collapsed condition

                new_rpn.pop_back();
                new_rpn.pop_back();
                new_rpn.push_back(std::move(collapsed_elem));
                continue;
            }
        }

        new_rpn.push_back(elem);
    }

    rpn = std::move(new_rpn);
}


String KeyCondition::toString() const
{
    String res;
    for (size_t i = 0; i < rpn.size(); ++i)
    {
        if (i)
            res += ", ";
        res += rpn[i].toString();
    }
    return res;
}

KeyCondition::Description KeyCondition::getDescription() const
{
    /// This code may seem to be too difficult.
    /// Here we want to convert RPN back to tree, and also simplify some logical expressions like `and(x, true) -> x`.
    Description description;

    /// That's a binary tree. Explicit.
    /// Build and optimize it simultaneously.
    struct Node
    {
        enum class Type : uint8_t
        {
            /// Leaf, which is RPNElement.
            Leaf,
            /// Leafs, which are logical constants.
            True,
            False,
            /// Binary operators.
            And,
            Or,
        };

        Type type{};

        /// Only for Leaf
        const RPNElement * element = nullptr;
        /// This means that logical NOT is applied to leaf.
        bool negate = false;

        std::unique_ptr<Node> left = nullptr;
        std::unique_ptr<Node> right = nullptr;
    };

    /// The algorithm is the same as in KeyCondition::checkInHyperrectangle
    /// We build a pair of trees on stack. For checking if key condition may be true, and if it may be false.
    /// We need only `can_be_true` in result.
    struct Frame
    {
        std::unique_ptr<Node> can_be_true;
        std::unique_ptr<Node> can_be_false;
    };

    /// Combine two subtrees using logical operator.
    auto combine = [](std::unique_ptr<Node> left, std::unique_ptr<Node> right, Node::Type type)
    {
        /// Simplify operators with for one constant condition.

        if (type == Node::Type::And)
        {
            /// false AND right
            if (left->type == Node::Type::False)
                return left;

            /// left AND false
            if (right->type == Node::Type::False)
                return right;

            /// true AND right
            if (left->type == Node::Type::True)
                return right;

            /// left AND true
            if (right->type == Node::Type::True)
                return left;
        }

        if (type == Node::Type::Or)
        {
            /// false OR right
            if (left->type == Node::Type::False)
                return right;

            /// left OR false
            if (right->type == Node::Type::False)
                return left;

            /// true OR right
            if (left->type == Node::Type::True)
                return left;

            /// left OR true
            if (right->type == Node::Type::True)
                return right;
        }

        return std::make_unique<Node>(Node{
                .type = type,
                .left = std::move(left),
                .right = std::move(right)
            });
    };

    std::vector<Frame> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::True});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::True});
            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else if (
               element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET
            || element.function == RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::Leaf, .element = &element, .negate = false});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::Leaf, .element = &element, .negate = true});
            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            assert(!rpn_stack.empty());

            std::swap(rpn_stack.back().can_be_true, rpn_stack.back().can_be_false);
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());
            auto arg1 = std::move(rpn_stack.back());

            rpn_stack.pop_back();

            assert(!rpn_stack.empty());
            auto arg2 = std::move(rpn_stack.back());

            Frame frame;
            frame.can_be_true = combine(std::move(arg1.can_be_true), std::move(arg2.can_be_true), Node::Type::And);
            frame.can_be_false = combine(std::move(arg1.can_be_false), std::move(arg2.can_be_false), Node::Type::Or);

            rpn_stack.back() = std::move(frame);
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());
            auto arg1 = std::move(rpn_stack.back());

            rpn_stack.pop_back();

            assert(!rpn_stack.empty());
            auto arg2 = std::move(rpn_stack.back());

            Frame frame;
            frame.can_be_true = combine(std::move(arg1.can_be_true), std::move(arg2.can_be_true), Node::Type::Or);
            frame.can_be_false = combine(std::move(arg1.can_be_false), std::move(arg2.can_be_false), Node::Type::And);

            rpn_stack.back() = std::move(frame);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::False});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::True});

            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            auto can_be_true = std::make_unique<Node>(Node{.type = Node::Type::True});
            auto can_be_false = std::make_unique<Node>(Node{.type = Node::Type::False});
            rpn_stack.emplace_back(Frame{.can_be_true = std::move(can_be_true), .can_be_false = std::move(can_be_false)});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::getDescription");

    std::vector<String> key_names(key_columns.size());
    std::vector<bool> is_key_used(key_columns.size(), false);

    for (const auto & key : key_columns)
        key_names[key.second] = key.first;

    WriteBufferFromOwnString buf;

    std::function<void(const Node *)> describe;
    describe = [&describe, &key_names, &is_key_used, &buf](const Node * node)
    {
        switch (node->type)
        {
            case Node::Type::Leaf:
            {
                is_key_used[node->element->key_column] = true;

                /// Note: for condition with double negation, like `not(x not in set)`,
                /// we can replace it to `x in set` here.
                /// But I won't do it, because `cloneASTWithInversionPushDown` already push down `not`.
                /// So, this seem to be impossible for `can_be_true` tree.
                if (node->negate)
                    buf << "not(";
                buf << node->element->toString(key_names[node->element->key_column], true);
                if (node->negate)
                    buf << ")";
                break;
            }
            case Node::Type::True:
                buf << "true";
                break;
            case Node::Type::False:
                buf << "false";
                break;
            case Node::Type::And:
                buf << "and(";
                describe(node->left.get());
                buf << ", ";
                describe(node->right.get());
                buf << ")";
                break;
            case Node::Type::Or:
                buf << "or(";
                describe(node->left.get());
                buf << ", ";
                describe(node->right.get());
                buf << ")";
                break;
        }
    };

    describe(rpn_stack.front().can_be_true.get());
    description.condition = std::move(buf.str());

    for (size_t i = 0; i < key_names.size(); ++i)
        if (is_key_used[i])
            description.used_keys.emplace_back(key_names[i]);

    return description;
}

/** Index is the value of key every `index_granularity` rows.
  * This value is called a "mark". That is, the index consists of marks.
  *
  * The key is the tuple.
  * The data is sorted by key in the sense of lexicographic order over tuples.
  *
  * A pair of marks specifies a segment with respect to the order over the tuples.
  * Denote it like this: [ x1 y1 z1 .. x2 y2 z2 ],
  *  where x1 y1 z1 - tuple - value of key in left border of segment;
  *        x2 y2 z2 - tuple - value of key in right boundary of segment.
  * In this section there are data between these marks.
  *
  * Or, the last mark specifies the range open on the right: [ a b c .. + inf )
  *
  * The set of all possible tuples can be considered as an n-dimensional space, where n is the size of the tuple.
  * A range of tuples specifies some subset of this space.
  *
  * Hyperrectangles will be the subrange of an n-dimensional space that is a direct product of one-dimensional ranges.
  * In this case, the one-dimensional range can be:
  * a point, a segment, an open interval, a half-open interval;
  * unlimited on the left, unlimited on the right ...
  *
  * The range of tuples can always be represented as a combination (union) of hyperrectangles.
  * For example, the range [ x1 y1 .. x2 y2 ] given x1 != x2 is equal to the union of the following three hyperrectangles:
  * [x1]       × [y1 .. +inf)
  * (x1 .. x2) × (-inf .. +inf)
  * [x2]       × (-inf .. y2]
  *
  * Or, for example, the range [ x1 y1 .. +inf ] is equal to the union of the following two hyperrectangles:
  * [x1]         × [y1 .. +inf)
  * (x1 .. +inf) × (-inf .. +inf)
  * It's easy to see that this is a special case of the variant above.
  *
  * This is important because it is easy for us to check the feasibility of the condition over the hyperrectangle,
  *  and therefore, feasibility of condition on the range of tuples will be checked by feasibility of condition
  *  over at least one hyperrectangle from which this range consists.
  */

/** For the range between tuples, determined by left_keys, left_bounded, right_keys, right_bounded,
  * invoke the callback on every hyperrectangle composing this range (see the description above),
  * and returns the OR of the callback results (meaning if callback returned true on any part of the range).
  */
template <typename F>
static BoolMask forAnyHyperrectangle(
    size_t key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    bool left_bounded,
    bool right_bounded,
    Hyperrectangle & hyperrectangle, /// This argument is modified in-place for the callback
    const DataTypes & data_types,
    size_t prefix_size,
    BoolMask initial_mask,
    F && callback)
{
    if (!left_bounded && !right_bounded)
        return callback(hyperrectangle);

    if (left_bounded && right_bounded)
    {
        /// Let's go through the matching elements of the key.
        while (prefix_size < key_size)
        {
            if (left_keys[prefix_size] == right_keys[prefix_size])
            {
                /// Point ranges.
                hyperrectangle[prefix_size] = Range(left_keys[prefix_size]);
                ++prefix_size;
            }
            else
                break;
        }
    }

    if (prefix_size == key_size)
        return callback(hyperrectangle);

    if (prefix_size + 1 == key_size)
    {
        if (left_bounded && right_bounded)
            hyperrectangle[prefix_size] = Range(left_keys[prefix_size], true, right_keys[prefix_size], true);
        else if (left_bounded)
            hyperrectangle[prefix_size]
                = Range::createLeftBounded(left_keys[prefix_size], true, isNullableOrLowCardinalityNullable(data_types[prefix_size]));
        else if (right_bounded)
            hyperrectangle[prefix_size]
                = Range::createRightBounded(right_keys[prefix_size], true, isNullableOrLowCardinalityNullable(data_types[prefix_size]));

        return callback(hyperrectangle);
    }

    /// (x1 .. x2) × (-inf .. +inf)

    if (left_bounded && right_bounded)
        hyperrectangle[prefix_size] = Range(left_keys[prefix_size], false, right_keys[prefix_size], false);
    else if (left_bounded)
        hyperrectangle[prefix_size]
            = Range::createLeftBounded(left_keys[prefix_size], false, isNullableOrLowCardinalityNullable(data_types[prefix_size]));
    else if (right_bounded)
        hyperrectangle[prefix_size]
            = Range::createRightBounded(right_keys[prefix_size], false, isNullableOrLowCardinalityNullable(data_types[prefix_size]));

    for (size_t i = prefix_size + 1; i < key_size; ++i)
    {
        if (isNullableOrLowCardinalityNullable(data_types[i]))
            hyperrectangle[i] = Range::createWholeUniverse();
        else
            hyperrectangle[i] = Range::createWholeUniverseWithoutNull();
    }

    auto result = BoolMask::combine(initial_mask, callback(hyperrectangle));

    /// There are several early-exit conditions (like the one below) hereinafter.
    /// They provide significant speedup, which may be observed on merge_tree_huge_pk performance test.
    if (result.isComplete())
        return result;

    /// [x1]       × [y1 .. +inf)

    if (left_bounded)
    {
        hyperrectangle[prefix_size] = Range(left_keys[prefix_size]);
        result = BoolMask::combine(
            result,
            forAnyHyperrectangle(
                key_size, left_keys, right_keys, true, false, hyperrectangle, data_types, prefix_size + 1, initial_mask, callback));

        if (result.isComplete())
            return result;
    }

    /// [x2]       × (-inf .. y2]

    if (right_bounded)
    {
        hyperrectangle[prefix_size] = Range(right_keys[prefix_size]);
        result = BoolMask::combine(
            result,
            forAnyHyperrectangle(
                key_size, left_keys, right_keys, false, true, hyperrectangle, data_types, prefix_size + 1, initial_mask, callback));
    }

    return result;
}


BoolMask KeyCondition::checkInRange(
    size_t used_key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    const DataTypes & data_types,
    BoolMask initial_mask) const
{
    Hyperrectangle key_ranges;

    key_ranges.reserve(used_key_size);
    for (size_t i = 0; i < used_key_size; ++i)
    {
        if (isNullableOrLowCardinalityNullable(data_types[i]))
            key_ranges.push_back(Range::createWholeUniverse());
        else
            key_ranges.push_back(Range::createWholeUniverseWithoutNull());
    }

    // std::cerr << "Checking for: [";
    // for (size_t i = 0; i != used_key_size; ++i)
    //     std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), left_keys[i]);
    // std::cerr << " ... ";

    // for (size_t i = 0; i != used_key_size; ++i)
    //     std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), right_keys[i]);
    // std::cerr << "]" << ": " << initial_mask.can_be_true << " : " << initial_mask.can_be_false << "\n";

    return forAnyHyperrectangle(used_key_size, left_keys, right_keys, true, true, key_ranges, data_types, 0, initial_mask,
        [&] (const Hyperrectangle & key_ranges_hyperrectangle)
    {
        auto res = checkInHyperrectangle(key_ranges_hyperrectangle, data_types);

        // std::cerr << "Hyperrectangle: ";
        // for (size_t i = 0, size = key_ranges.size(); i != size; ++i)
        //     std::cerr << (i != 0 ? " × " : "") << key_ranges[i].toString();
        // std::cerr << ": " << res.can_be_true << " : " << res.can_be_false << "\n";

        return res;
    });
}

std::optional<Range> KeyCondition::applyMonotonicFunctionsChainToRange(
    Range key_range,
    const MonotonicFunctionsChain & functions,
    DataTypePtr current_type,
    bool single_point)
{
    for (const auto & func : functions)
    {
        /// We check the monotonicity of each function on a specific range.
        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
        IFunction::Monotonicity monotonicity = single_point
            ? IFunction::Monotonicity{true}
            : func->getMonotonicityForRange(*current_type.get(), key_range.left, key_range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }

        /// If we apply function to open interval, we can get empty intervals in result.
        /// E.g. for ('2020-01-03', '2020-01-20') after applying 'toYYYYMM' we will get ('202001', '202001').
        /// To avoid this we make range left and right included.
        /// Any function that treats NULL specially is not monotonic.
        /// Thus we can safely use isNull() as an -Inf/+Inf indicator here.
        if (!key_range.left.isNull())
        {
            key_range.left = applyFunction(func, current_type, key_range.left);
            key_range.left_included = true;
        }

        if (!key_range.right.isNull())
        {
            key_range.right = applyFunction(func, current_type, key_range.right);
            key_range.right_included = true;
        }

        current_type = func->getResultType();

        if (!monotonicity.is_positive)
            key_range.invert();
    }
    return key_range;
}

// Returns whether the condition is one continuous range of the primary key,
// where every field is matched by range or a single element set.
// This allows to use a more efficient lookup with no extra reads.
bool KeyCondition::matchesExactContinuousRange() const
{
    // Not implemented yet.
    if (hasMonotonicFunctionsChain())
        return false;

    enum Constraint
    {
        POINT,
        RANGE,
        UNKNOWN,
    };

    std::vector<Constraint> column_constraints(key_columns.size(), Constraint::UNKNOWN);

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::Function::FUNCTION_AND)
        {
            continue;
        }

        if (element.function == RPNElement::Function::FUNCTION_IN_SET && element.set_index && element.set_index->size() == 1)
        {
            column_constraints[element.key_column] = Constraint::POINT;
            continue;
        }

        if (element.function == RPNElement::Function::FUNCTION_IN_RANGE)
        {
            if (element.range.left == element.range.right)
            {
                column_constraints[element.key_column] = Constraint::POINT;
            }
            if (column_constraints[element.key_column] != Constraint::POINT)
            {
                column_constraints[element.key_column] = Constraint::RANGE;
            }
            continue;
        }

        if (element.function == RPNElement::Function::FUNCTION_UNKNOWN)
        {
            continue;
        }

        return false;
    }

    auto min_constraint = column_constraints[0];

    if (min_constraint > Constraint::RANGE)
    {
        return false;
    }

    for (size_t i = 1; i < key_columns.size(); ++i)
    {
        if (column_constraints[i] < min_constraint)
        {
            return false;
        }

        if (column_constraints[i] == Constraint::RANGE && min_constraint == Constraint::RANGE)
        {
            return false;
        }

        min_constraint = column_constraints[i];
    }

    return true;
}

bool KeyCondition::extractPlainRanges(Ranges & ranges) const
{
    if (key_indices.size() != 1)
        return false;

    if (hasMonotonicFunctionsChain())
        return false;

    /// All Ranges in rpn_stack is plain.
    std::stack<PlainRanges> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_AND)
        {
            auto right_ranges = rpn_stack.top();
            rpn_stack.pop();

            auto left_ranges = rpn_stack.top();
            rpn_stack.pop();

            auto new_range = left_ranges.intersectWith(right_ranges);
            rpn_stack.emplace(std::move(new_range));
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto right_ranges = rpn_stack.top();
            rpn_stack.pop();

            auto left_ranges = rpn_stack.top();
            rpn_stack.pop();

            auto new_range = left_ranges.unionWith(right_ranges);
            rpn_stack.emplace(std::move(new_range));
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            auto to_invert_ranges = rpn_stack.top();
            rpn_stack.pop();

            std::vector<Ranges> reverted_ranges = PlainRanges::invert(to_invert_ranges.ranges);

            if (reverted_ranges.size() == 1)
                rpn_stack.emplace(std::move(reverted_ranges[0]));
            else
            {
                /// intersect reverted ranges
                PlainRanges intersected_ranges(reverted_ranges[0]);
                for (size_t i = 1; i < reverted_ranges.size(); i++)
                {
                    intersected_ranges = intersected_ranges.intersectWith(PlainRanges(reverted_ranges[i]));
                }
                rpn_stack.emplace(std::move(intersected_ranges));
            }
        }
        else /// atom relational expression or constants
        {
            if (element.function == RPNElement::FUNCTION_IN_RANGE)
            {
                rpn_stack.push(PlainRanges(element.range));
            }
            else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
            {
                rpn_stack.push(PlainRanges(element.range.invertRange()));
            }
            else if (element.function == RPNElement::FUNCTION_IN_SET)
            {
                if (element.set_index->hasMonotonicFunctionsChain())
                    return false;

                if (element.set_index->size() == 0)
                {
                    rpn_stack.push(PlainRanges::makeBlank()); /// skip blank range
                    continue;
                }

                const auto & values = element.set_index->getOrderedSet();
                Ranges points_range;

                /// values in set_index are ordered and no duplication
                for (size_t i=0; i<element.set_index->size(); i++)
                {
                    FieldRef f;
                    values[0]->get(i, f);
                    if (f.isNull())
                        return false;
                    points_range.push_back({f});
                }
                rpn_stack.push(PlainRanges(points_range));
            }
            else if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
            {
                if (element.set_index->hasMonotonicFunctionsChain())
                    return false;

                if (element.set_index->size() == 0)
                {
                    rpn_stack.push(PlainRanges::makeUniverse());
                    continue;
                }

                const auto & values = element.set_index->getOrderedSet();
                Ranges points_range;

                std::optional<FieldRef> pre;
                for (size_t i=0; i<element.set_index->size(); i++)
                {
                    FieldRef cur;
                    values[0]->get(i, cur);

                    if (cur.isNull())
                        return false;
                    if (pre)
                    {
                        Range r(*pre, false, cur, false);
                        /// skip blank range
                        if (!(r.left > r.right || (r.left == r.right && !r.left_included && !r.right_included)))
                            points_range.push_back(r);
                    }
                    else
                    {
                        points_range.push_back(Range::createRightBounded(cur, false));
                    }
                    pre = cur;
                }

                points_range.push_back(Range::createLeftBounded(*pre, false));
                rpn_stack.push(PlainRanges(points_range));
            }
            else if (element.function == RPNElement::ALWAYS_FALSE)
            {
                /// skip blank range
                rpn_stack.push(PlainRanges::makeBlank());
            }
            else if (element.function == RPNElement::ALWAYS_TRUE)
            {
                rpn_stack.push(PlainRanges::makeUniverse());
            }
            else if (element.function == RPNElement::FUNCTION_IS_NULL)
            {
                /// key values can not be null, so isNull will get blank range.
                rpn_stack.push(PlainRanges::makeBlank());
            }
            else if (element.function == RPNElement::FUNCTION_IS_NOT_NULL)
            {
                rpn_stack.push(PlainRanges::makeUniverse());
            }
            else /// FUNCTION_UNKNOWN
            {
                if (!has_filter)
                    rpn_stack.push(PlainRanges::makeUniverse());
                else
                    return false;
            }
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::extractPlainRanges");

    ranges = std::move(rpn_stack.top().ranges);
    return true;
}

BoolMask KeyCondition::checkInHyperrectangle(
    const Hyperrectangle & hyperrectangle,
    const DataTypes & data_types) const
{
    std::vector<BoolMask> rpn_stack;

    auto curve_type = [&](size_t key_column_pos)
    {
        for (const auto & curve : key_space_filling_curves)
            if (curve.key_column_pos == key_column_pos)
                return curve.type;
        return SpaceFillingCurveType::Unknown;
    };

    for (const auto & element : rpn)
    {
        if (element.argument_num_of_space_filling_curve.has_value())
        {
            /// If a condition on argument of a space filling curve wasn't collapsed into FUNCTION_ARGS_IN_HYPERRECTANGLE,
            /// we cannot process it.
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            if (element.key_column >= hyperrectangle.size())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Hyperrectangle size is {}, but requested element at posittion {} ({})",
                                hyperrectangle.size(), element.key_column, element.toString());
            }

            const Range * key_range = &hyperrectangle[element.key_column];

            /// The case when the column is wrapped in a chain of possibly monotonic functions.
            Range transformed_range = Range::createWholeUniverse();
            if (!element.monotonic_functions_chain.empty())
            {
                std::optional<Range> new_range = applyMonotonicFunctionsChainToRange(
                    *key_range,
                    element.monotonic_functions_chain,
                    data_types[element.key_column],
                    single_point
                );

                if (!new_range)
                {
                    rpn_stack.emplace_back(true, true);
                    continue;
                }
                transformed_range = *new_range;
                key_range = &transformed_range;
            }

            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);
            if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE)
        {
            /** The case of space-filling curves.
              * We unpack the range of a space filling curve into hyperrectangles of their arguments,
              * and then check the intersection of them with the given hyperrectangle from the key condition.
              *
              * Note: you might find this code hard to understand,
              * because there are three different hyperrectangles involved:
              *
              * 1. A hyperrectangle derived from the range of the table's sparse index (marks granule): `hyperrectangle`
              *    We analyze its dimension `key_range`, corresponding to the `key_column`.
              *    For example, the table's key is a single column `mortonEncode(x, y)`,
              *    the current granule is [500, 600], and it means that
              *    mortonEncode(x, y) in [500, 600]
              *
              * 2. A hyperrectangle derived from the key condition, e.g.
              *    `x >= 10 AND x <= 20 AND y >= 20 AND y <= 30` defines: (x, y) in [10, 20] × [20, 30]
              *
              * 3. A set of hyperrectangles that we obtain by inverting the space-filling curve on the range:
              *    From mortonEncode(x, y) in [500, 600]
              *    We get (x, y) in [30, 31] × [12, 13]
              *        or (x, y) in [28, 31] × [14, 15];
              *        or (x, y) in [0, 7] × [16, 23];
              *        or (x, y) in [8, 11] × [16, 19];
              *        or (x, y) in [12, 15] × [16, 17];
              *        or (x, y) in [12, 12] × [18, 18];
              *
              *  And we analyze the intersection of (2) and (3).
              */

            Range key_range = hyperrectangle[element.key_column];

            /// The only possible result type of a space filling curve is UInt64.
            /// We also only check bounded ranges.
            if (key_range.left.getType() == Field::Types::UInt64
                && key_range.right.getType() == Field::Types::UInt64)
            {
                key_range.shrinkToIncludedIfPossible();

                size_t num_dimensions = element.space_filling_curve_args_hyperrectangle.size();

                /// Let's support only the case of 2d, because I'm not confident in other cases.
                if (num_dimensions == 2)
                {
                    UInt64 left = key_range.left.safeGet<UInt64>();
                    UInt64 right = key_range.right.safeGet<UInt64>();

                    BoolMask mask(false, true);
                    auto hyperrectangle_intersection_callback = [&](std::array<std::pair<UInt64, UInt64>, 2> curve_hyperrectangle)
                    {
                        BoolMask current_intersection(true, false);
                        for (size_t dim = 0; dim < num_dimensions; ++dim)
                        {
                            const Range & condition_arg_range = element.space_filling_curve_args_hyperrectangle[dim];

                            const Range curve_arg_range(
                                curve_hyperrectangle[dim].first, true,
                                curve_hyperrectangle[dim].second, true);

                            bool intersects = condition_arg_range.intersectsRange(curve_arg_range);
                            bool contains = condition_arg_range.containsRange(curve_arg_range);

                            current_intersection = current_intersection & BoolMask(intersects, !contains);
                        }

                        mask = mask | current_intersection;
                    };

                    switch (curve_type(element.key_column))
                    {
                        case SpaceFillingCurveType::Hilbert:
                        {
                            hilbertIntervalToHyperrectangles2D(left, right, hyperrectangle_intersection_callback);
                            break;
                        }
                        case SpaceFillingCurveType::Morton:
                        {
                            mortonIntervalToHyperrectangles<2>(left, right, hyperrectangle_intersection_callback);
                            break;
                        }
                        case SpaceFillingCurveType::Unknown:
                        {
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "curve_type is `Unknown`. It is a bug.");
                        }
                    }

                    rpn_stack.emplace_back(mask);
                }
                else
                    rpn_stack.emplace_back(true, true);
            }
            else
                rpn_stack.emplace_back(true, true);

            /** Note: we can consider implementing a simpler solution, based on "hidden keys".
              * It means, when we have a table's key like (a, b, mortonCurve(x, y))
              * we extract the arguments from the curves, and append them to the key,
              * imagining that we have the key (a, b, mortonCurve(x, y), x, y)
              *
              * Then while we analyze the granule's range between (a, b, mortonCurve(x, y))
              * and decompose it to the series of hyperrectangles,
              * we can construct a series of hyperrectangles of the extended key (a, b, mortonCurve(x, y), x, y),
              * and then do everything as usual.
              *
              * This approach is generalizable to any functions, that have preimage of interval
              * represented by a set of hyperrectangles.
              */
        }
        else if (element.function == RPNElement::FUNCTION_POINT_IN_POLYGON)
        {
            /** There are 2 kinds of polygons:
              *   1. Polygon by minmax index
              *   2. Polygons which is provided by user
              *
              * Polygon by minmax index:
              *   For hyperactangle [1, 2] × [3, 4] we can create a polygon with 4 points: (1, 3), (1, 4), (2, 4), (2, 3)
              *
              * Algorithm:
              *   Check whether there is any intersection of the 2 polygons. If true return {true, true}, else return {false, true}.
              */
            const auto & key_column_positions = element.point_in_polygon_column_description->key_column_positions;

            Float64 x_min = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[0]].left);
            Float64 x_max = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[0]].right);
            Float64 y_min = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[1]].left);
            Float64 y_max = applyVisitor(FieldVisitorConvertToNumber<Float64>(), hyperrectangle[key_column_positions[1]].right);

            if (unlikely(isNaN(x_min) || isNaN(x_max) || isNaN(y_min) || isNaN(y_max)))
            {
                rpn_stack.emplace_back(true, true);
                continue;
            }

            using Point = boost::geometry::model::d2::point_xy<Float64>;
            using Polygon = boost::geometry::model::polygon<Point>;
            Polygon  polygon_by_minmax_index;
            polygon_by_minmax_index.outer().emplace_back(x_min, y_min);
            polygon_by_minmax_index.outer().emplace_back(x_min, y_max);
            polygon_by_minmax_index.outer().emplace_back(x_max, y_max);
            polygon_by_minmax_index.outer().emplace_back(x_max, y_min);

            /// Close ring
            boost::geometry::correct(polygon_by_minmax_index);

            /// Because the polygon may have a hole so the "can_be_false" should always be true.
            rpn_stack.emplace_back(
                boost::geometry::intersects(polygon_by_minmax_index, element.polygon), true);
        }
        else if (
            element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL)
        {
            const Range * key_range = &hyperrectangle[element.key_column];

            /// No need to apply monotonic functions as nulls are kept.
            bool intersects = element.range.intersectsRange(*key_range);
            bool contains = element.range.containsRange(*key_range);

            rpn_stack.emplace_back(intersects, !contains);
            if (element.function == RPNElement::FUNCTION_IS_NULL)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (
            element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            if (!element.set_index)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Set for IN is not created yet");

            rpn_stack.emplace_back(element.set_index->checkInRange(hyperrectangle, data_types, single_point));
            if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            assert(!rpn_stack.empty());

            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::checkInHyperrectangle");

    return rpn_stack[0];
}

bool KeyCondition::mayBeTrueInRange(
    size_t used_key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    const DataTypes & data_types) const
{
    return checkInRange(used_key_size, left_keys, right_keys, data_types, BoolMask::consider_only_can_be_true).can_be_true;
}

String KeyCondition::RPNElement::toString() const
{
    if (argument_num_of_space_filling_curve)
        return toString(fmt::format("argument {} of column {}", *argument_num_of_space_filling_curve, key_column), false);

    if (point_in_polygon_column_description)
        return toString(
            fmt::format(
                "column ({}, {})",
                point_in_polygon_column_description->key_columns[0],
                point_in_polygon_column_description->key_columns[1]),
            false);

    return toString(fmt::format("column {}", key_column), true);
}

String KeyCondition::RPNElement::toString(std::string_view column_name, bool print_constants) const
{
    auto print_wrapped_column = [this, column_name, print_constants](WriteBuffer & buf)
    {
        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
        {
            buf << (*it)->getName() << "(";
            if (print_constants)
            {
                if (const auto * func = typeid_cast<const FunctionWithOptionalConstArg *>(it->get()))
                {
                    if (func->getKind() == FunctionWithOptionalConstArg::Kind::LEFT_CONST)
                        buf << applyVisitor(FieldVisitorToString(), (*func->getConstArg().column)[0]) << ", ";
                }
            }
        }

        buf << column_name;

        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
        {
            if (print_constants)
            {
                if (const auto * func = typeid_cast<const FunctionWithOptionalConstArg *>(it->get()))
                {
                    if (func->getKind() == FunctionWithOptionalConstArg::Kind::RIGHT_CONST)
                        buf << ", " << applyVisitor(FieldVisitorToString(), (*func->getConstArg().column)[0]);
                }
            }
            buf << ")";
        }
    };

    WriteBufferFromOwnString buf;
    switch (function)
    {
        case FUNCTION_AND:
            return "and";
        case FUNCTION_OR:
            return "or";
        case FUNCTION_NOT:
            return "not";
        case FUNCTION_UNKNOWN:
            return "unknown";
        case FUNCTION_NOT_IN_SET:
        case FUNCTION_IN_SET:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << (function == FUNCTION_IN_SET ? " in " : " notIn ");
            if (!set_index)
                buf << "unknown size set";
            else
                buf << set_index->size() << "-element set";
            buf << ")";
            return buf.str();
        }
        case FUNCTION_IN_RANGE:
        case FUNCTION_NOT_IN_RANGE:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString();
            buf << ")";
            return buf.str();
        }
        case FUNCTION_ARGS_IN_HYPERRECTANGLE:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << " has args in ";
            buf << DB::toString(space_filling_curve_args_hyperrectangle);
            buf << ")";
            return buf.str();
        }
        case FUNCTION_POINT_IN_POLYGON:
        {
            auto points_in_polygon = polygon.outer();
            buf << "(";
            print_wrapped_column(buf);
            buf << " in ";
            buf << "[";
            for (size_t i = 0; i < points_in_polygon.size(); ++i)
            {
                if (i != 0)
                    buf << ", ";
                buf << "(" << points_in_polygon[i].x() << ", " << points_in_polygon[i].y() << ")";
            }
            buf << "]";
            buf << ")";
            return buf.str();
        }
        case FUNCTION_IS_NULL:
        case FUNCTION_IS_NOT_NULL:
        {
            buf << "(";
            print_wrapped_column(buf);
            buf << (function == FUNCTION_IS_NULL ? " isNull" : " isNotNull");
            buf << ")";
            return buf.str();
        }
        case ALWAYS_FALSE:
            return "false";
        case ALWAYS_TRUE:
            return "true";
    }
}


bool KeyCondition::alwaysUnknownOrTrue() const
{
    return unknownOrAlwaysTrue(false);
}

bool KeyCondition::anyUnknownOrAlwaysTrue() const
{
    return unknownOrAlwaysTrue(true);
}

bool KeyCondition::unknownOrAlwaysTrue(bool unknown_any) const
{
    std::vector<UInt8> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            /// If unknown_any is true, return instantly,
            /// to avoid processing it with FUNCTION_AND, and change the outcome.
            if (unknown_any)
                return true;
            /// Otherwise, it may be AND'ed via FUNCTION_AND
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET
            || element.function == RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE
            || element.function == RPNElement::FUNCTION_POINT_IN_POLYGON
            || element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL
            || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::unknownOrAlwaysTrue");

    return rpn_stack[0];
}

bool KeyCondition::alwaysFalse() const
{
    /// 0: always_false, 1: always_true, 2: non_const
    std::vector<UInt8> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(1);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(0);
        }
        else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET
            || element.function == RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE
            || element.function == RPNElement::FUNCTION_IS_NULL
            || element.function == RPNElement::FUNCTION_IS_NOT_NULL
            || element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.push_back(2);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            assert(!rpn_stack.empty());

            auto & arg = rpn_stack.back();
            if (arg == 0)
                arg = 1;
            else if (arg == 1)
                arg = 0;
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();

            if (arg1 == 0 || arg2 == 0)
                rpn_stack.back() = 0;
            else if (arg1 == 1 && arg2 == 1)
                rpn_stack.back() = 1;
            else
                rpn_stack.back() = 2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            assert(!rpn_stack.empty());

            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();

            if (arg1 == 1 || arg2 == 1)
                rpn_stack.back() = 1;
            else if (arg1 == 0 && arg2 == 0)
                rpn_stack.back() = 0;
            else
                rpn_stack.back() = 2;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::alwaysFalse");

    return rpn_stack[0] == 0;
}

bool KeyCondition::hasMonotonicFunctionsChain() const
{
    for (const auto & element : rpn)
        if (!element.monotonic_functions_chain.empty()
            || (element.set_index && element.set_index->hasMonotonicFunctionsChain()))
            return true;
    return false;
}

}
