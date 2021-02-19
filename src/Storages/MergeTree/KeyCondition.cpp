#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/BoolMask.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/misc.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/typeid_cast.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTIdentifier.h>

#include <cassert>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TYPE_OF_FIELD;
}


String Range::toString() const
{
    std::stringstream str;

    if (!left_bounded)
        str << "(-inf, ";
    else
        str << (left_included ? '[' : '(') << applyVisitor(FieldVisitorToString(), left) << ", ";

    if (!right_bounded)
        str << "+inf)";
    else
        str << applyVisitor(FieldVisitorToString(), right) << (right_included ? ']' : ')');

    return str.str();
}


/// Example: for `Hello\_World% ...` string it returns `Hello_World`, and for `%test%` returns an empty string.
static String extractFixedPrefixFromLikePattern(const String & like_pattern)
{
    String fixed_prefix;

    const char * pos = like_pattern.data();
    const char * end = pos + like_pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
                [[fallthrough]];
            case '_':
                return fixed_prefix;

            case '\\':
                ++pos;
                if (pos == end)
                    break;
                [[fallthrough]];
            default:
                fixed_prefix += *pos;
                break;
        }

        ++pos;
    }

    return fixed_prefix;
}


/** For a given string, get a minimum string that is strictly greater than all strings with this prefix,
  *  or return an empty string if there are no such strings.
  */
static String firstStringThatIsGreaterThanAllStringsWithPrefix(const String & prefix)
{
    /** Increment the last byte of the prefix by one. But if it is 255, then remove it and increase the previous one.
      * Example (for convenience, suppose that the maximum value of byte is `z`)
      * abcx -> abcy
      * abcz -> abd
      * zzz -> empty string
      * z -> empty string
      */

    String res = prefix;

    while (!res.empty() && static_cast<UInt8>(res.back()) == 255)
        res.pop_back();

    if (res.empty())
        return res;

    res.back() = static_cast<char>(1 + static_cast<UInt8>(res.back()));
    return res;
}


/// A dictionary containing actions to the corresponding functions to turn them into `RPNElement`
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

            String prefix = extractFixedPrefixFromLikePattern(value.get<const String &>());
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

            String prefix = extractFixedPrefixFromLikePattern(value.get<const String &>());
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
        {"like", "notLike"},
        {"notLike", "like"},
        {"empty", "notEmpty"},
        {"notEmpty", "empty"},
};


bool isLogicalOperator(const String & func_name)
{
    return (func_name == "and" || func_name == "or" || func_name == "not");
}

/// The node can be one of:
///   - Logical operator (AND, OR, NOT)
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

        if (need_inversion)
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


inline bool Range::equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }
inline bool Range::less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs); }


/** Calculate expressions, that depend only on constants.
  * For index to work when something like "WHERE Date = toDate(now())" is written.
  */
Block KeyCondition::getBlockWithConstants(
    const ASTPtr & query, const TreeRewriterResultPtr & syntax_analyzer_result, const Context & context)
{
    Block result
    {
        { DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }
    };

    const auto expr_for_constant_folding = ExpressionAnalyzer(query, syntax_analyzer_result, context).getConstActions();

    expr_for_constant_folding->execute(result);

    return result;
}


KeyCondition::KeyCondition(
    const SelectQueryInfo & query_info,
    const Context & context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_)
    : key_expr(key_expr_), prepared_sets(query_info.sets)
{
    for (size_t i = 0, size = key_column_names.size(); i < size; ++i)
    {
        std::string name = key_column_names[i];
        if (!key_columns.count(name))
            key_columns[name] = i;
    }

    /** Evaluation of expressions that depend only on constants.
      * For the index to be used, if it is written, for example `WHERE Date = toDate(now())`.
      */
    Block block_with_constants = getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context);

    const ASTSelectQuery & select = query_info.query->as<ASTSelectQuery &>();
    if (select.where() || select.prewhere())
    {
        ASTPtr filter_query;
        if (select.where() && select.prewhere())
            filter_query = makeASTFunction("and", select.where(), select.prewhere());
        else
            filter_query = select.where() ? select.where() : select.prewhere();

        /** When non-strictly monotonic functions are employed in functional index (e.g. ORDER BY toStartOfHour(dateTime)),
          * the use of NOT operator in predicate will result in the indexing algorithm leave out some data.
          * This is caused by rewriting in KeyCondition::tryParseAtomFromAST of relational operators to less strict
          * when parsing the AST into internal RPN representation.
          * To overcome the problem, before parsing the AST we transform it to its semantically equivalent form where all NOT's
          * are pushed down and applied (when possible) to leaf nodes.
          */
        traverseAST(cloneASTWithInversionPushDown(filter_query), context, block_with_constants);
    }
    else
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
    }
}

bool KeyCondition::addCondition(const String & column, const Range & range)
{
    if (!key_columns.count(column))
        return false;
    rpn.emplace_back(RPNElement::FUNCTION_IN_RANGE, key_columns[column], range);
    rpn.emplace_back(RPNElement::FUNCTION_AND);
    return true;
}

/** Computes value of constant expression and its data type.
  * Returns false, if expression isn't constant.
  */
bool KeyCondition::getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type)
{
    String column_name = expr->getColumnName();

    if (const auto * lit = expr->as<ASTLiteral>())
    {
        /// By default block_with_constants has only one column named "_dummy".
        /// If block contains only constants it's may not be preprocessed by
        //  ExpressionAnalyzer, so try to look up in the default column.
        if (!block_with_constants.has(column_name))
            column_name = "_dummy";

        /// Simple literal
        out_value = lit->value;
        out_type = block_with_constants.getByName(column_name).type;
        return true;
    }
    else if (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column))
    {
        /// An expression which is dependent on constants only
        const auto & expr_info = block_with_constants.getByName(column_name);
        out_value = (*expr_info.column)[0];
        out_type = expr_info.type;
        return true;
    }
    else
        return false;
}


static Field applyFunctionForField(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    Block block
    {
        { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
        { nullptr, func->getReturnType(), "y" }
    };

    func->execute(block, {0}, 1, 1);
    return (*block.safeGetByPosition(1).column)[0];
}

/// The case when arguments may have types different than in the primary key.
static std::pair<Field, DataTypePtr> applyFunctionForFieldOfUnknownType(
    const FunctionOverloadResolverPtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value)
{
    ColumnWithTypeAndName argument = { arg_type->createColumnConst(1, arg_value), arg_type, "x" };

    FunctionBasePtr func_base = func->build({argument});

    DataTypePtr return_type = func_base->getReturnType();

    Block block
    {
        std::move(argument),
        { nullptr, return_type, "result" }
    };

    func_base->execute(block, {0}, 1, 1);

    Field result = (*block.safeGetByPosition(1).column)[0];

    return {std::move(result), std::move(return_type)};
}


static FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field)
{
    /// Fallback for fields without block reference.
    if (field.isExplicit())
        return applyFunctionForField(func, current_type, field);

    String result_name = "_" + func->getName() + "_" + toString(field.column_idx);
    size_t result_idx;
    const auto & block = field.block;
    if (!block->has(result_name))
    {
        result_idx = block->columns();
        field.block->insert({nullptr, func->getReturnType(), result_name});
        func->execute(*block, {field.column_idx}, result_idx, block->rows());
    }
    else
        result_idx = block->getPositionByName(result_name);

    return {field.block, field.row_idx, result_idx};
}

void KeyCondition::traverseAST(const ASTPtr & node, const Context & context, Block & block_with_constants)
{
    RPNElement element;

    if (const auto * func = node->as<ASTFunction>())
    {
        if (tryParseLogicalOperatorFromAST(func, element))
        {
            auto & args = func->arguments->children;
            for (size_t i = 0, size = args.size(); i < size; ++i)
            {
                traverseAST(args[i], context, block_with_constants);

                /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                  * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                  */
                if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
                    rpn.emplace_back(element);
            }

            return;
        }
    }

    if (!tryParseAtomFromAST(node, context, block_with_constants, element))
    {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}


bool KeyCondition::canConstantBeWrappedByMonotonicFunctions(
    const ASTPtr & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node->getColumnName();
    const auto & sample_block = key_expr->getSampleBlock();
    if (!sample_block.has(expr_name))
        return false;

    bool found_transformation = false;
    for (const ExpressionAction & action : key_expr->getActions())
    {
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
        const auto & argument_names = action.argument_names;
        if (action.type == ExpressionAction::Type::APPLY_FUNCTION
            && argument_names.size() == 1
            && argument_names[0] == expr_name)
        {
            if (!action.function_base->hasInformationAboutMonotonicity())
                return false;

            /// Range is irrelevant in this case.
            IFunction::Monotonicity monotonicity = action.function_base->getMonotonicityForRange(*out_type, Field(), Field());
            if (!monotonicity.is_always_monotonic)
                return false;

            /// Apply the next transformation step.
            std::tie(out_value, out_type) = applyFunctionForFieldOfUnknownType(
                action.function_builder,
                out_type, out_value);

            expr_name = action.result_name;

            /// Transformation results in a key expression, accept.
            auto it = key_columns.find(expr_name);
            if (key_columns.end() != it)
            {
                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                found_transformation = true;
                break;
            }
        }
    }

    return found_transformation;
}

bool KeyCondition::tryPrepareSetIndex(
    const ASTs & args,
    const Context & context,
    RPNElement & out,
    size_t & out_key_column_num)
{
    const ASTPtr & left_arg = args[0];

    out_key_column_num = 0;
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;
    DataTypes data_types;

    auto get_key_tuple_position_mapping = [&](const ASTPtr & node, size_t tuple_index)
    {
        MergeTreeSetIndex::KeyTuplePositionMapping index_mapping;
        index_mapping.tuple_index = tuple_index;
        DataTypePtr data_type;
        if (isKeyPossiblyWrappedByMonotonicFunctions(
                node, context, index_mapping.key_index, data_type, index_mapping.functions))
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            if (out_key_column_num < index_mapping.key_index)
                out_key_column_num = index_mapping.key_index;
        }
    };

    size_t left_args_count = 1;
    const auto * left_arg_tuple = left_arg->as<ASTFunction>();
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        const auto & tuple_elements = left_arg_tuple->arguments->children;
        left_args_count = tuple_elements.size();
        for (size_t i = 0; i < left_args_count; ++i)
            get_key_tuple_position_mapping(tuple_elements[i], i);
    }
    else
        get_key_tuple_position_mapping(left_arg, 0);

    if (indexes_mapping.empty())
        return false;

    const ASTPtr & right_arg = args[1];

    SetPtr prepared_set;
    if (right_arg->as<ASTSubquery>() || right_arg->as<ASTIdentifier>())
    {
        auto set_it = prepared_sets.find(PreparedSetKey::forSubquery(*right_arg));
        if (set_it == prepared_sets.end())
            return false;

        prepared_set = set_it->second;
    }
    else
    {
        /// We have `PreparedSetKey::forLiteral` but it is useless here as we don't have enough information
        /// about types in left argument of the IN operator. Instead, we manually iterate through all the sets
        /// and find the one for the right arg based on the AST structure (getTreeHash), after that we check
        /// that the types it was prepared with are compatible with the types of the primary key.
        auto set_ast_hash = right_arg->getTreeHash();
        auto set_it = std::find_if(
            prepared_sets.begin(), prepared_sets.end(),
            [&](const auto & candidate_entry)
            {
                if (candidate_entry.first.ast_hash != set_ast_hash)
                    return false;

                for (size_t i = 0; i < indexes_mapping.size(); ++i)
                    if (!candidate_entry.second->areTypesEqual(indexes_mapping[i].tuple_index, data_types[i]))
                        return false;

                return true;
        });
        if (set_it == prepared_sets.end())
            return false;

        prepared_set = set_it->second;
    }

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    prepared_set->checkColumnsNumber(left_args_count);
    for (size_t i = 0; i < indexes_mapping.size(); ++i)
        prepared_set->checkTypesEqual(indexes_mapping[i].tuple_index, data_types[i]);

    out.set_index = std::make_shared<MergeTreeSetIndex>(prepared_set->getSetElements(), std::move(indexes_mapping));

    return true;
}


bool KeyCondition::isKeyPossiblyWrappedByMonotonicFunctions(
    const ASTPtr & node,
    const Context & context,
    size_t & out_key_column_num,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain)
{
    std::vector<const ASTFunction *> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_key_column_num, key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        auto func_builder = FunctionFactory::instance().tryGet((*it)->name, context);
        ColumnsWithTypeAndName arguments{{ nullptr, key_column_type, "" }};
        auto func = func_builder->build(arguments);

        if (!func || !func->hasInformationAboutMonotonicity())
            return false;

        key_column_type = func->getReturnType();
        out_functions_chain.push_back(func);
    }

    out_key_res_column_type = key_column_type;

    return true;
}

bool KeyCondition::isKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const ASTPtr & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    std::vector<const ASTFunction *> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = key_expr->getSampleBlock();
    String name = node->getColumnName();

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(it->first).type;
        return true;
    }

    if (const auto * func = node->as<ASTFunction>())
    {
        const auto & args = func->arguments->children;
        if (args.size() != 1)
            return false;

        out_functions_chain.push_back(func);

        return isKeyPossiblyWrappedByMonotonicFunctionsImpl(args[0], out_key_column_num, out_key_column_type, out_functions_chain);
    }

    return false;
}


static void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const ASTPtr & node)
{
    if (desired_type->equals(*src_type))
        return;

    try
    {
        src_value = convertFieldToType(src_value, *desired_type, src_type.get());
    }
    catch (...)
    {
        throw Exception("Key expression contains comparison between inconvertible types: " +
            desired_type->getName() + " and " + src_type->getName() +
            " inside " + queryToString(node),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


bool KeyCondition::tryParseAtomFromAST(const ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out)
{
    /** Functions < > = != <= >= in `notIn`, where one argument is a constant, and the other is one of columns of key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  or constant expression - number.
      */
    Field const_value;
    DataTypePtr const_type;
    if (const auto * func = node->as<ASTFunction>())
    {
        const ASTs & args = func->arguments->children;

        DataTypePtr key_expr_type;    /// Type of expression containing key column
        size_t key_column_num = -1;   /// Number of a key column (inside key_column_names array)
        MonotonicFunctionsChain chain;
        std::string func_name = func->name;

        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        if (args.size() == 1)
        {
            if (!(isKeyPossiblyWrappedByMonotonicFunctions(args[0], context, key_column_num, key_expr_type, chain)))
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception("`key_column_num` wasn't initialized. It is a bug.", ErrorCodes::LOGICAL_ERROR);
        }
        else if (args.size() == 2)
        {
            size_t key_arg_pos;           /// Position of argument with key column (non-const argument)
            bool is_set_const = false;
            bool is_constant_transformed = false;

            if (functionIsInOrGlobalInOperator(func_name)
                && tryPrepareSetIndex(args, context, out, key_column_num))
            {
                key_arg_pos = 0;
                is_set_const = true;
            }
            else if (getConstant(args[1], block_with_constants, const_value, const_type)
                && isKeyPossiblyWrappedByMonotonicFunctions(args[0], context, key_column_num, key_expr_type, chain))
            {
                key_arg_pos = 0;
            }
            else if (getConstant(args[1], block_with_constants, const_value, const_type)
                && canConstantBeWrappedByMonotonicFunctions(args[0], key_column_num, key_expr_type, const_value, const_type))
            {
                key_arg_pos = 0;
                is_constant_transformed = true;
            }
            else if (getConstant(args[0], block_with_constants, const_value, const_type)
                && isKeyPossiblyWrappedByMonotonicFunctions(args[1], context, key_column_num, key_expr_type, chain))
            {
                key_arg_pos = 1;
            }
            else if (getConstant(args[0], block_with_constants, const_value, const_type)
                && canConstantBeWrappedByMonotonicFunctions(args[1], key_column_num, key_expr_type, const_value, const_type))
            {
                key_arg_pos = 1;
                is_constant_transformed = true;
            }
            else
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception("`key_column_num` wasn't initialized. It is a bug.", ErrorCodes::LOGICAL_ERROR);

            /// Transformed constant must weaken the condition, for example "x > 5" must weaken to "round(x) >= 5"
            if (is_constant_transformed)
            {
                if (func_name == "less")
                    func_name = "lessOrEquals";
                else if (func_name == "greater")
                    func_name = "greaterOrEquals";
            }

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
                         func_name == "startsWith")
                {
                    /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                    return false;
                }
            }

            bool cast_not_needed =
                is_set_const /// Set args are already casted inside Set::createFromAST
                || (isNativeNumber(key_expr_type) && isNativeNumber(const_type)); /// Numbers are accurately compared without cast.

            if (!cast_not_needed)
                castValueToType(key_expr_type, const_value, const_type, node);
        }
        else
            return false;

        const auto atom_it = atom_map.find(func_name);

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);

        return atom_it->second(out, const_value);
    }
    else if (getConstant(node, block_with_constants, const_value, const_type))
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
            out.function = const_value.safeGet<Float64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }
    return false;
}

bool KeyCondition::tryParseLogicalOperatorFromAST(const ASTFunction * func, RPNElement & out)
{
    /// Functions AND, OR, NOT.
    const ASTs & args = func->arguments->children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;

        out.function = RPNElement::FUNCTION_NOT;
    }
    else
    {
        if (func->name == "and")
            out.function = RPNElement::FUNCTION_AND;
        else if (func->name == "or")
            out.function = RPNElement::FUNCTION_OR;
        else
            return false;
    }

    return true;
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
  * Hyperrectangles (you can also find the term "rail")
  *  will be the subrange of an n-dimensional space that is a direct product of one-dimensional ranges.
  * In this case, the one-dimensional range can be: a period, a segment, an interval, a half-interval, unlimited on the left, unlimited on the right ...
  *
  * The range of tuples can always be represented as a combination of hyperrectangles.
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
    const FieldRef * key_left,
    const FieldRef * key_right,
    bool left_bounded,
    bool right_bounded,
    std::vector<Range> & hyperrectangle,
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
            if (key_left[prefix_size] == key_right[prefix_size])
            {
                /// Point ranges.
                hyperrectangle[prefix_size] = Range(key_left[prefix_size]);
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
            hyperrectangle[prefix_size] = Range(key_left[prefix_size], true, key_right[prefix_size], true);
        else if (left_bounded)
            hyperrectangle[prefix_size] = Range::createLeftBounded(key_left[prefix_size], true);
        else if (right_bounded)
            hyperrectangle[prefix_size] = Range::createRightBounded(key_right[prefix_size], true);

        return callback(hyperrectangle);
    }

    /// (x1 .. x2) x (-inf .. +inf)

    if (left_bounded && right_bounded)
        hyperrectangle[prefix_size] = Range(key_left[prefix_size], false, key_right[prefix_size], false);
    else if (left_bounded)
        hyperrectangle[prefix_size] = Range::createLeftBounded(key_left[prefix_size], false);
    else if (right_bounded)
        hyperrectangle[prefix_size] = Range::createRightBounded(key_right[prefix_size], false);

    for (size_t i = prefix_size + 1; i < key_size; ++i)
        hyperrectangle[i] = Range();


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
        hyperrectangle[prefix_size] = Range(key_left[prefix_size]);
        result = result | forAnyHyperrectangle(key_size, key_left, key_right, true, false, hyperrectangle, prefix_size + 1, initial_mask, callback);
        if (result.isComplete())
            return result;
    }

    /// [x2]       x (-inf .. y2]

    if (right_bounded)
    {
        hyperrectangle[prefix_size] = Range(key_right[prefix_size]);
        result = result | forAnyHyperrectangle(key_size, key_left, key_right, false, true, hyperrectangle, prefix_size + 1, initial_mask, callback);
        if (result.isComplete())
            return result;
    }

    return result;
}


BoolMask KeyCondition::checkInRange(
    size_t used_key_size,
    const FieldRef * left_key,
    const FieldRef * right_key,
    const DataTypes & data_types,
    bool right_bounded,
    BoolMask initial_mask) const
{
    std::vector<Range> key_ranges(used_key_size, Range());

/*  std::cerr << "Checking for: [";
    for (size_t i = 0; i != used_key_size; ++i)
        std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), left_key[i]);
    std::cerr << " ... ";

    if (right_bounded)
    {
        for (size_t i = 0; i != used_key_size; ++i)
            std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), right_key[i]);
        std::cerr << "]\n";
    }
    else
        std::cerr << "+inf)\n";*/

    return forAnyHyperrectangle(used_key_size, left_key, right_key, true, right_bounded, key_ranges, 0, initial_mask,
        [&] (const std::vector<Range> & key_ranges_hyperrectangle)
    {
        auto res = checkInHyperrectangle(key_ranges_hyperrectangle, data_types);

/*      std::cerr << "Hyperrectangle: ";
        for (size_t i = 0, size = key_ranges.size(); i != size; ++i)
            std::cerr << (i != 0 ? " x " : "") << key_ranges[i].toString();
        std::cerr << ": " << res.can_be_true << "\n";*/

        return res;
    });
}


std::optional<Range> KeyCondition::applyMonotonicFunctionsChainToRange(
    Range key_range,
    const MonotonicFunctionsChain & functions,
    DataTypePtr current_type)
{
    for (const auto & func : functions)
    {
        /// We check the monotonicity of each function on a specific range.
        IFunction::Monotonicity monotonicity = func->getMonotonicityForRange(
            *current_type.get(), key_range.left, key_range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }

        /// If we apply function to open interval, we can get empty intervals in result.
        /// E.g. for ('2020-01-03', '2020-01-20') after applying 'toYYYYMM' we will get ('202001', '202001').
        /// To avoid this we make range left and right included.
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

        current_type = func->getReturnType();

        if (!monotonicity.is_positive)
            key_range.swapLeftAndRight();
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
            Range transformed_range;
            if (!element.monotonic_functions_chain.empty())
            {
                std::optional<Range> new_range = applyMonotonicFunctionsChainToRange(
                    *key_range,
                    element.monotonic_functions_chain,
                    data_types[element.key_column]
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
            element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            if (!element.set_index)
                throw Exception("Set for IN is not created yet", ErrorCodes::LOGICAL_ERROR);

            rpn_stack.emplace_back(element.set_index->checkInRange(hyperrectangle, data_types));
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
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::checkInRange", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0];
}


BoolMask KeyCondition::checkInRange(
    size_t used_key_size,
    const FieldRef * left_key,
    const FieldRef * right_key,
    const DataTypes & data_types,
    BoolMask initial_mask) const
{
    return checkInRange(used_key_size, left_key, right_key, data_types, true, initial_mask);
}


bool KeyCondition::mayBeTrueInRange(
    size_t used_key_size,
    const FieldRef * left_key,
    const FieldRef * right_key,
    const DataTypes & data_types) const
{
    return checkInRange(used_key_size, left_key, right_key, data_types, true, BoolMask::consider_only_can_be_true).can_be_true;
}


BoolMask KeyCondition::checkAfter(
    size_t used_key_size,
    const FieldRef * left_key,
    const DataTypes & data_types,
    BoolMask initial_mask) const
{
    return checkInRange(used_key_size, left_key, nullptr, data_types, false, initial_mask);
}


bool KeyCondition::mayBeTrueAfter(
    size_t used_key_size,
    const FieldRef * left_key,
    const DataTypes & data_types) const
{
    return checkInRange(used_key_size, left_key, nullptr, data_types, false, BoolMask::consider_only_can_be_true).can_be_true;
}


String KeyCondition::RPNElement::toString() const
{
    auto print_wrapped_column = [this](std::ostringstream & ss)
    {
        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
            ss << (*it)->getName() << "(";

        ss << "column " << key_column;

        for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
            ss << ")";
    };

    std::ostringstream ss;
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
            ss << "(";
            print_wrapped_column(ss);
            ss << (function == FUNCTION_IN_SET ? " in " : " notIn ");
            if (!set_index)
                ss << "unknown size set";
            else
                ss << set_index->size() << "-element set";
            ss << ")";
            return ss.str();
        }
        case FUNCTION_IN_RANGE:
        case FUNCTION_NOT_IN_RANGE:
        {
            ss << "(";
            print_wrapped_column(ss);
            ss << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString();
            ss << ")";
            return ss.str();
        }
        case ALWAYS_FALSE:
            return "false";
        case ALWAYS_TRUE:
            return "true";
    }

    __builtin_unreachable();
}


bool KeyCondition::alwaysUnknownOrTrue() const
{
    std::vector<UInt8> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET
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
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::alwaysUnknownOrTrue", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0];
}


size_t KeyCondition::getMaxKeyColumn() const
{
    size_t res = 0;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_IN_SET
            || element.function == RPNElement::FUNCTION_NOT_IN_SET)
        {
            if (element.key_column > res)
                res = element.key_column;
        }
    }
    return res;
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
