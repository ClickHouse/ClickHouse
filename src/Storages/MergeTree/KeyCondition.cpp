#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/BoolMask.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/misc.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/indexHint.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeIndexUtils.h>

#include <base/defines.h>

#include <algorithm>
#include <cassert>
#include <stack>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TYPE_OF_FIELD;
}


String Range::toString() const
{
    WriteBufferFromOwnString str;

    str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left) << ", ";
    str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

    return str.str();
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

            String prefix = extractFixedPrefixFromLikePattern(value.get<const String &>(), /*requires_perfect_prefix*/ false);
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

            String prefix = extractFixedPrefixFromLikePattern(value.get<const String &>(), /*requires_perfect_prefix*/ true);
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

            String prefix = value.get<const String &>();
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

            const String & expression = value.get<const String &>();
            // This optimization can't process alternation - this would require a comprehensive parsing of regular expression.
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
    }
};


static const std::map<std::string, std::string> inverse_relations = {
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
        {"empty", "notEmpty"},
        {"notEmpty", "empty"},
};


bool isLogicalOperator(const String & func_name)
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
                /// DAG from PK does not use it. This is breakig match by column name sometimes.
                /// Ideally, we should not compare manes, but DAG subtrees instead.
                name = ASTLiteral(column_const->getDataColumn()[0]).getColumnName();
            else
                name = node.result_name;

            res = &inverted_dag.addColumn({node.column, node.result_type, name});
            break;
        }
        case (ActionsDAG::ActionType::ALIAS):
        {
            /// Ignore aliases
            const auto & alias = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, need_inversion);
            to_inverted[&node] = &alias;
            return alias;
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
                const auto & arg = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, !need_inversion);
                to_inverted[&node] = &arg;
                return arg;
            }

            if (name == "materialize")
            {
                /// Ignore materialize
                const auto & arg = cloneASTWithInversionPushDown(*node.children.front(), inverted_dag, to_inverted, context, need_inversion);
                to_inverted[&node] = &arg;
                return arg;
            }

            if (name == "indexHint")
            {
                ActionsDAG::NodeRawConstPtrs children;
                if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get()))
                {
                    if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                    {
                        const auto & index_hint_dag = index_hint->getActions();
                        children = index_hint_dag->getOutputs();

                        for (auto & arg : children)
                            arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, need_inversion);
                    }
                }

                const auto & func = inverted_dag.addFunction(node.function_base, children, "");
                to_inverted[&node] = &func;
                return func;
            }

            if (need_inversion && (name == "and" || name == "or"))
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
                const auto & func = inverted_dag.addFunction(function_builder, children, "");
                to_inverted[&node] = &func;
                return func;
            }

            ActionsDAG::NodeRawConstPtrs children(node.children);

            for (auto & arg : children)
                arg = &cloneASTWithInversionPushDown(*arg, inverted_dag, to_inverted, context, false);

            auto it = inverse_relations.find(name);
            if (it != inverse_relations.end())
            {
                const auto & func_name = need_inversion ? it->second : it->first;
                auto function_builder = FunctionFactory::instance().get(func_name, context);
                const auto & func = inverted_dag.addFunction(function_builder, children, "");
                to_inverted[&node] = &func;
                return func;
            }

            res = &inverted_dag.addFunction(node.function_base, children, "");
            chassert(res->result_type == node.result_type);
        }
    }

    if (need_inversion)
        res = &inverted_dag.addFunction(FunctionFactory::instance().get("not", context), {res}, "");

    to_inverted[&node] = res;
    return *res;
}

static ActionsDAGPtr cloneASTWithInversionPushDown(ActionsDAG::NodeRawConstPtrs nodes, const ContextPtr & context)
{
    auto res = std::make_shared<ActionsDAG>();

    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> to_inverted;

    for (auto & node : nodes)
        node = &cloneASTWithInversionPushDown(*node, *res, to_inverted, context, false);

    if (nodes.size() > 1)
    {
        auto function_builder = FunctionFactory::instance().get("and", context);
        nodes = {&res->addFunction(function_builder, std::move(nodes), "")};
    }

    res->getOutputs().swap(nodes);

    return res;
}


inline bool Range::equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }
inline bool Range::less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs); }


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
        for (const auto & action_node : actions->getOutputs())
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

KeyCondition::KeyCondition(
    const ASTPtr & query,
    const ASTs & additional_filter_asts,
    Block block_with_constants,
    PreparedSetsPtr prepared_sets,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    NameSet array_joined_column_names_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    , array_joined_column_names(std::move(array_joined_column_names_))
    , single_point(single_point_)
    , strict(strict_)
{
    size_t key_index = 0;
    for (const auto & name : key_column_names)
    {
        if (!key_columns.contains(name))
        {
            key_columns[name] = key_columns.size();
            key_indices.push_back(key_index);
        }
        ++key_index;
    }

    ASTPtr filter_node;
    if (query)
        filter_node = buildFilterNode(query, additional_filter_asts);

    if (!filter_node)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    /** When non-strictly monotonic functions are employed in functional index (e.g. ORDER BY toStartOfHour(dateTime)),
      * the use of NOT operator in predicate will result in the indexing algorithm leave out some data.
      * This is caused by rewriting in KeyCondition::tryParseAtomFromAST of relational operators to less strict
      * when parsing the AST into internal RPN representation.
      * To overcome the problem, before parsing the AST we transform it to its semantically equivalent form where all NOT's
      * are pushed down and applied (when possible) to leaf nodes.
      */
    auto inverted_filter_node = cloneASTWithInversionPushDown(filter_node);

    RPNBuilder<RPNElement> builder(
        inverted_filter_node,
        std::move(context),
        std::move(block_with_constants),
        std::move(prepared_sets),
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

KeyCondition::KeyCondition(
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_,
    bool strict_)
    : KeyCondition(
        query_info.query,
        query_info.filter_asts,
        KeyCondition::getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context),
        query_info.prepared_sets,
        context,
        key_column_names,
        key_expr_,
        query_info.syntax_analyzer_result ? query_info.syntax_analyzer_result->getArrayJoinSourceNameSet() : NameSet{},
        single_point_,
        strict_)
{
}

KeyCondition::KeyCondition(
    ActionsDAGPtr filter_dag,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    NameSet array_joined_column_names_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    , array_joined_column_names(std::move(array_joined_column_names_))
    , single_point(single_point_)
    , strict(strict_)
{
    size_t key_index = 0;
    for (const auto & name : key_column_names)
    {
        if (!key_columns.contains(name))
        {
            key_columns[name] = key_columns.size();
            key_indices.push_back(key_index);
        }
        ++key_index;
    }

    if (!filter_dag)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    auto inverted_dag = cloneASTWithInversionPushDown({filter_dag->getOutputs().at(0)}, context);
    assert(inverted_dag->getOutputs().size() == 1);

    const auto * inverted_dag_filter_node = inverted_dag->getOutputs()[0];

    RPNBuilder<RPNElement> builder(inverted_dag_filter_node, context, [&](const RPNBuilderTreeNode & node, RPNElement & out)
    {
        return extractAtomFromTree(node, out);
    });

    rpn = std::move(builder).extractRPN();
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

/// The case when arguments may have types different than in the primary key.
static std::pair<Field, DataTypePtr> applyFunctionForFieldOfUnknownType(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    ColumnsWithTypeAndName arguments{{ arg_type->createColumnConst(1, arg_value), arg_type, "x" }};
    DataTypePtr return_type = func->getResultType();

    auto col = func->execute(arguments, return_type, 1);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}


/// Same as above but for binary operators
static std::pair<Field, DataTypePtr> applyBinaryFunctionForFieldOfUnknownType(
    const FunctionOverloadResolverPtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value,
    const DataTypePtr & arg_type2,
    const Field & arg_value2)
{
    ColumnsWithTypeAndName arguments{
        {arg_type->createColumnConst(1, arg_value), arg_type, "x"}, {arg_type2->createColumnConst(1, arg_value2), arg_type2, "y"}};

    FunctionBasePtr func_base = func->build(arguments);

    DataTypePtr return_type = func_base->getResultType();

    auto col = func_base->execute(arguments, return_type, 1);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}


static FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    String result_name = "_" + func->getName() + "_" + toString(field.column_idx);
    const auto & columns = field.columns;
    size_t result_idx = columns->size();

    for (size_t i = 0; i < result_idx; ++i)
    {
        if ((*columns)[i].name == result_name)
            result_idx = i;
    }

    if (result_idx == columns->size())
    {
        ColumnsWithTypeAndName args{(*columns)[field.column_idx]};
        field.columns->emplace_back(ColumnWithTypeAndName {nullptr, func->getResultType(), result_name});
        (*columns)[result_idx].column = func->execute(args, (*columns)[result_idx].type, columns->front().column->size());
    }

    return {field.columns, field.row_idx, result_idx};
}

/** When table's key has expression with these functions from a column,
  * and when a column in a query is compared with a constant, such as:
  * CREATE TABLE (x String) ORDER BY toDate(x)
  * SELECT ... WHERE x LIKE 'Hello%'
  * we want to apply the function to the constant for index analysis,
  * but should modify it to pass on unparsable values.
  */
static std::set<std::string_view> date_time_parsing_functions = {
    "toDate",
    "toDate32",
    "toDateTime",
    "toDateTime64",
    "ParseDateTimeBestEffort",
    "ParseDateTimeBestEffortUS",
    "ParseDateTime32BestEffort",
    "ParseDateTime64BestEffort",
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
bool KeyCondition::transformConstantWithValidFunctions(
    ContextPtr context,
    const String & expr_name,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type,
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
                out_type = removeLowCardinality(out_type);
                auto const_type = removeLowCardinality(cur_node->result_type);
                auto const_column = out_type->createColumnConst(1, out_value);
                auto const_value = (*castColumnAccurateOrNull({const_column, out_type, ""}, const_type))[0];

                if (const_value.isNull())
                    return false;

                while (!chain.empty())
                {
                    const auto * func = chain.top();
                    chain.pop();

                    if (func->type != ActionsDAG::ActionType::FUNCTION)
                        continue;

                    const auto & func_name = func->function_base->getName();
                    auto func_base = func->function_base;
                    const auto & arg_types = func_base->getArgumentTypes();
                    if (date_time_parsing_functions.contains(func_name) && !arg_types.empty() && isStringOrFixedString(arg_types[0]))
                    {
                        auto func_or_null = FunctionFactory::instance().get(func_name + "OrNull", context);
                        ColumnsWithTypeAndName arguments;
                        int i = 0;
                        for (const auto & type : func->function_base->getArgumentTypes())
                            arguments.push_back({nullptr, type, fmt::format("_{}", i++)});

                        func_base = func_or_null->build(arguments);
                    }

                    if (func->children.size() == 1)
                    {
                        std::tie(const_value, const_type)
                            = applyFunctionForFieldOfUnknownType(func_base, const_type, const_value);
                    }
                    else if (func->children.size() == 2)
                    {
                        const auto * left = func->children[0];
                        const auto * right = func->children[1];
                        if (left->column && isColumnConst(*left->column))
                        {
                            auto left_arg_type = left->result_type;
                            auto left_arg_value = (*left->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                FunctionFactory::instance().get(func_base->getName(), context),
                                left_arg_type, left_arg_value, const_type, const_value);
                        }
                        else
                        {
                            auto right_arg_type = right->result_type;
                            auto right_arg_value = (*right->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                FunctionFactory::instance().get(func_base->getName(), context),
                                const_type, const_value, right_arg_type, right_arg_value);
                        }
                    }

                    if (const_value.isNull())
                        return false;
                }

                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                out_value = const_value;
                out_type = const_type;
                return true;
            }
        }
    }

    return false;
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

    return transformConstantWithValidFunctions(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_column_type,
        out_value,
        out_type,
        [](const IFunctionBase & func, const IDataType & type)
        {
            if (!func.hasInformationAboutMonotonicity())
                return false;
            else
            {
                /// Range is irrelevant in this case.
                auto monotonicity = func.getMonotonicityForRange(type, Field(), Field());
                if (!monotonicity.is_always_monotonic)
                    return false;
            }
            return true;
        });
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

    return transformConstantWithValidFunctions(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_column_type,
        out_value,
        out_type,
        [](const IFunctionBase & func, const IDataType &)
        {
            return func.isDeterministic();
        });
}

bool KeyCondition::tryPrepareSetIndex(
    const RPNBuilderFunctionTreeNode & func,
    RPNElement & out,
    size_t & out_key_column_num)
{
    const auto & left_arg = func.getArgumentAt(0);

    out_key_column_num = 0;
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;
    DataTypes data_types;

    auto get_key_tuple_position_mapping = [&](const RPNBuilderTreeNode & node, size_t tuple_index)
    {
        MergeTreeSetIndex::KeyTuplePositionMapping index_mapping;
        index_mapping.tuple_index = tuple_index;
        DataTypePtr data_type;
        if (isKeyPossiblyWrappedByMonotonicFunctions(node, index_mapping.key_index, data_type, index_mapping.functions))
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            if (out_key_column_num < index_mapping.key_index)
                out_key_column_num = index_mapping.key_index;
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

    auto future_set = right_arg.tryGetPreparedSet(indexes_mapping, data_types);
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(right_arg.getTreeContext().getQueryContext());
    if (!prepared_set)
        return false;

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    prepared_set->checkColumnsNumber(left_args_count);
    for (size_t i = 0; i < indexes_mapping.size(); ++i)
        prepared_set->checkTypesEqual(indexes_mapping[i].tuple_index, data_types[i]);

    out.set_index = std::make_shared<MergeTreeSetIndex>(prepared_set->getSetElements(), std::move(indexes_mapping));
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
        else if (kind == Kind::RIGHT_CONST)
        {
            auto new_arguments = arguments;
            new_arguments.push_back(const_arg);
            new_arguments.back().column = new_arguments.back().column->cloneResized(input_rows_count);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        else
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
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain)
{
    std::vector<RPNBuilderFunctionTreeNode> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_key_column_num, key_column_type, chain_not_tested_for_monotonicity))
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

        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
        if (!func || (!single_point && !func->hasInformationAboutMonotonicity()))
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
    DataTypePtr & out_key_column_type,
    std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = key_expr->getSampleBlock();

    // Key columns should use canonical names for index analysis
    String name = node.getColumnName();

    if (array_joined_column_names.contains(name))
        return false;

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(it->first).type;
        return true;
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
                result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(function_node.getArgumentAt(1), out_key_column_num, out_key_column_type, out_functions_chain);
            }
            else if (function_node.getArgumentAt(1).isConstant())
            {
                result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(function_node.getArgumentAt(0), out_key_column_num, out_key_column_type, out_functions_chain);
            }
        }
        else
        {
            result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(function_node.getArgumentAt(0), out_key_column_num, out_key_column_type, out_functions_chain);
        }

        return result;
    }

    return false;
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
    /** Functions < > = != <= >= in `notIn` isNull isNotNull, where one argument is a constant, and the other is one of columns of key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  or constant expression - number.
      */
    Field const_value;
    DataTypePtr const_type;
    if (node.isFunction())
    {
        auto func = node.toFunctionNode();
        size_t num_args = func.getArgumentsSize();

        DataTypePtr key_expr_type;    /// Type of expression containing key column
        size_t key_column_num = -1;   /// Number of a key column (inside key_column_names array)
        MonotonicFunctionsChain chain;
        std::string func_name = func.getFunctionName();

        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        if (num_args == 1)
        {
            if (!(isKeyPossiblyWrappedByMonotonicFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, chain)))
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");
        }
        else if (num_args == 2)
        {
            size_t key_arg_pos;           /// Position of argument with key column (non-const argument)
            bool is_set_const = false;
            bool is_constant_transformed = false;

            /// We don't look for inversed key transformations when strict is true, which is required for trivial count().
            /// Consider the following test case:
            ///
            /// create table test1(p DateTime, k int) engine MergeTree partition by toDate(p) order by k;
            /// insert into test1 values ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);
            /// select count() from test1 where p > toDateTime('2020-09-01 10:00:00');
            ///
            /// toDate(DateTime) is always monotonic, but we cannot relax the predicates to be
            /// >= toDate(toDateTime('2020-09-01 10:00:00')), which returns 3 instead of the right count: 2.
            bool strict_condition = strict;

            /// If we use this key condition to prune partitions by single value, we cannot relax conditions for NOT.
            if (single_point
                && (func_name == "notLike" || func_name == "notIn" || func_name == "globalNotIn" || func_name == "notNullIn"
                    || func_name == "globalNotNullIn" || func_name == "notEquals" || func_name == "notEmpty"))
                strict_condition = true;

            if (functionIsInOrGlobalInOperator(func_name))
            {
                if (tryPrepareSetIndex(func, out, key_column_num))
                {
                    key_arg_pos = 0;
                    is_set_const = true;
                }
                else
                    return false;
            }
            else if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }

                if (isKeyPossiblyWrappedByMonotonicFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, chain))
                {
                    key_arg_pos = 0;
                }
                else if (
                    !strict_condition
                    && canConstantBeWrappedByMonotonicFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals" && !strict_condition
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

                if (isKeyPossiblyWrappedByMonotonicFunctions(func.getArgumentAt(1), key_column_num, key_expr_type, chain))
                {
                    key_arg_pos = 1;
                }
                else if (
                    !strict_condition
                    && canConstantBeWrappedByMonotonicFunctions(func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 1;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals" && !strict_condition
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
                         func_name == "ilike" || func_name == "notIlike" ||
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
                        ColumnsWithTypeAndName arguments{
                            {nullptr, key_expr_type, ""},
                            {DataTypeString().createColumnConst(1, common_type_maybe_nullable->getName()), common_type_maybe_nullable, ""}};
                        FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl();
                        auto func_cast = func_builder_cast->build(arguments);

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
            }

        }
        else
            return false;

        const auto atom_it = atom_map.find(func_name);

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);

        return atom_it->second(out, const_value);
    }
    else if (node.tryGetConstant(const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`

        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Float64)
        {
            out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
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
        enum class Type
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
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::checkInRange");

    std::vector<std::string_view> key_names(key_columns.size());
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
  * [x1]       x [y1 .. +inf)
  * (x1 .. x2) x (-inf .. +inf)
  * [x2]       x (-inf .. y2]
  *
  * Or, for example, the range [ x1 y1 .. +inf ] is equal to the union of the following two hyperrectangles:
  * [x1]         x [y1 .. +inf)
  * (x1 .. +inf) x (-inf .. +inf)
  * It's easy to see that this is a special case of the variant above.
  *
  * This is important because it is easy for us to check the feasibility of the condition over the hyperrectangle,
  *  and therefore, feasibility of condition on the range of tuples will be checked by feasibility of condition
  *  over at least one hyperrectangle from which this range consists.
  */

template <typename F>
static BoolMask forAnyHyperrectangle(
    size_t key_size,
    const FieldRef * left_keys,
    const FieldRef * right_keys,
    bool left_bounded,
    bool right_bounded,
    std::vector<Range> & hyperrectangle,
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
            hyperrectangle[prefix_size] = Range::createLeftBounded(left_keys[prefix_size], true, data_types[prefix_size]->isNullable());
        else if (right_bounded)
            hyperrectangle[prefix_size] = Range::createRightBounded(right_keys[prefix_size], true, data_types[prefix_size]->isNullable());

        return callback(hyperrectangle);
    }

    /// (x1 .. x2) x (-inf .. +inf)

    if (left_bounded && right_bounded)
        hyperrectangle[prefix_size] = Range(left_keys[prefix_size], false, right_keys[prefix_size], false);
    else if (left_bounded)
        hyperrectangle[prefix_size] = Range::createLeftBounded(left_keys[prefix_size], false, data_types[prefix_size]->isNullable());
    else if (right_bounded)
        hyperrectangle[prefix_size] = Range::createRightBounded(right_keys[prefix_size], false, data_types[prefix_size]->isNullable());

    for (size_t i = prefix_size + 1; i < key_size; ++i)
    {
        if (data_types[i]->isNullable())
            hyperrectangle[i] = Range::createWholeUniverse();
        else
            hyperrectangle[i] = Range::createWholeUniverseWithoutNull();
    }


    BoolMask result = initial_mask;
    result = result | callback(hyperrectangle);

    /// There are several early-exit conditions (like the one below) hereinafter.
    /// They are important; in particular, if initial_mask == BoolMask::consider_only_can_be_true
    /// (which happens when this routine is called from KeyCondition::mayBeTrueXXX),
    /// they provide significant speedup, which may be observed on merge_tree_huge_pk performance test.
    if (result.isComplete())
        return result;

    /// [x1]       x [y1 .. +inf)

    if (left_bounded)
    {
        hyperrectangle[prefix_size] = Range(left_keys[prefix_size]);
        result = result
            | forAnyHyperrectangle(
                     key_size, left_keys, right_keys, true, false, hyperrectangle, data_types, prefix_size + 1, initial_mask, callback);
        if (result.isComplete())
            return result;
    }

    /// [x2]       x (-inf .. y2]

    if (right_bounded)
    {
        hyperrectangle[prefix_size] = Range(right_keys[prefix_size]);
        result = result
            | forAnyHyperrectangle(
                     key_size, left_keys, right_keys, false, true, hyperrectangle, data_types, prefix_size + 1, initial_mask, callback);
        if (result.isComplete())
            return result;
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
    std::vector<Range> key_ranges;

    key_ranges.reserve(used_key_size);
    for (size_t i = 0; i < used_key_size; ++i)
    {
        if (data_types[i]->isNullable())
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
    // std::cerr << "]\n";

    return forAnyHyperrectangle(used_key_size, left_keys, right_keys, true, true, key_ranges, data_types, 0, initial_mask,
        [&] (const std::vector<Range> & key_ranges_hyperrectangle)
    {
        auto res = checkInHyperrectangle(key_ranges_hyperrectangle, data_types);

        // std::cerr << "Hyperrectangle: ";
        // for (size_t i = 0, size = key_ranges.size(); i != size; ++i)
        //     std::cerr << (i != 0 ? " x " : "") << key_ranges[i].toString();
        // std::cerr << ": " << res.can_be_true << "\n";

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

BoolMask KeyCondition::checkInHyperrectangle(
    const std::vector<Range> & hyperrectangle,
    const DataTypes & data_types) const
{
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::checkInRange");

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

String KeyCondition::RPNElement::toString() const { return toString("column " + std::to_string(key_column), false); }

String KeyCondition::RPNElement::toString(std::string_view column_name, bool print_constants) const
{
    auto print_wrapped_column = [this, &column_name, print_constants](WriteBuffer & buf)
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

    UNREACHABLE();
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
