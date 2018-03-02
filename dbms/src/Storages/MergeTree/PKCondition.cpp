#include <Storages/MergeTree/PKCondition.h>
#include <Storages/MergeTree/BoolMask.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Set.h>
#include <Parsers/queryToString.h>


namespace DB
{

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
const PKCondition::AtomMap PKCondition::atom_map
{
    {
        "notEquals",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "equals",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range(value);
            return true;
        }
    },
    {
        "less",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, false);
            return true;
        }
    },
    {
        "greater",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, false);
            return true;
        }
    },
    {
        "lessOrEquals",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createRightBounded(value, true);
            return true;
        }
    },
    {
        "greaterOrEquals",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
        {
            out.function = RPNElement::FUNCTION_IN_RANGE;
            out.range = Range::createLeftBounded(value, true);
            return true;
        }
    },
    {
        "in",
        [] (RPNElement & out, const Field &, const ASTPtr & node)
        {
            out.function = RPNElement::FUNCTION_IN_SET;
            out.in_function = node;
            return true;
        }
    },
    {
        "notIn",
        [] (RPNElement & out, const Field &, const ASTPtr & node)
        {
            out.function = RPNElement::FUNCTION_NOT_IN_SET;
            out.in_function = node;
            return true;
        }
    },
    {
        "like",
        [] (RPNElement & out, const Field & value, const ASTPtr &)
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
    }
};


inline bool Range::equals(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs); }
inline bool Range::less(const Field & lhs, const Field & rhs) { return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs); }


FieldWithInfinity::FieldWithInfinity(const Field & field_)
    : field(field_),
    type(Type::NORMAL)
{
}

FieldWithInfinity::FieldWithInfinity(Field && field_)
    : field(std::move(field_)),
    type(Type::NORMAL)
{
}

FieldWithInfinity::FieldWithInfinity(const Type type_)
    : field(),
    type(type_)
{
}

FieldWithInfinity FieldWithInfinity::getMinusInfinity()
{
    return FieldWithInfinity(Type::MINUS_INFINITY);
}

FieldWithInfinity FieldWithInfinity::getPlusinfinity()
{
    return FieldWithInfinity(Type::PLUS_INFINITY);
}

bool FieldWithInfinity::operator<(const FieldWithInfinity & other) const
{
    return type < other.type || (type == other.type && type == Type::NORMAL && field < other.field);
}

bool FieldWithInfinity::operator==(const FieldWithInfinity & other) const
{
    return type == other.type && (type != Type::NORMAL || field == other.field);
}


/** Calculate expressions, that depend only on constants.
  * For index to work when something like "WHERE Date = toDate(now())" is written.
  */
Block PKCondition::getBlockWithConstants(
    const ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns)
{
    Block result
    {
        { DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }
    };

    const auto expr_for_constant_folding = ExpressionAnalyzer{query, context, nullptr, all_columns}.getConstActions();

    expr_for_constant_folding->execute(result);

    return result;
}


PKCondition::PKCondition(
    const SelectQueryInfo & query_info,
    const Context & context,
    const NamesAndTypesList & all_columns,
    const SortDescription & sort_descr_,
    const ExpressionActionsPtr & pk_expr_)
    : sort_descr(sort_descr_), pk_expr(pk_expr_), prepared_sets(query_info.sets)
{
    for (size_t i = 0; i < sort_descr.size(); ++i)
    {
        std::string name = sort_descr[i].column_name;
        if (!pk_columns.count(name))
            pk_columns[name] = i;
    }

    /** Evaluation of expressions that depend only on constants.
      * For the index to be used, if it is written, for example `WHERE Date = toDate(now())`.
      */
    Block block_with_constants = getBlockWithConstants(query_info.query, context, all_columns);

    /// Trasform WHERE section to Reverse Polish notation
    const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    if (select.where_expression)
    {
        traverseAST(select.where_expression, context, block_with_constants);

        if (select.prewhere_expression)
        {
            traverseAST(select.prewhere_expression, context, block_with_constants);
            rpn.emplace_back(RPNElement::FUNCTION_AND);
        }
    }
    else if (select.prewhere_expression)
    {
        traverseAST(select.prewhere_expression, context, block_with_constants);
    }
    else
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
    }
}

bool PKCondition::addCondition(const String & column, const Range & range)
{
    if (!pk_columns.count(column))
        return false;
    rpn.emplace_back(RPNElement::FUNCTION_IN_RANGE, pk_columns[column], range);
    rpn.emplace_back(RPNElement::FUNCTION_AND);
    return true;
}

/** Computes value of constant expression and it data type.
  * Returns false, if expression isn't constant.
  */
static bool getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type)
{
    String column_name = expr->getColumnName();

    if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(expr.get()))
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
    else if (block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isColumnConst())
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


static void applyFunction(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type, const Field & arg_value,
    DataTypePtr & res_type, Field & res_value)
{
    res_type = func->getReturnType();

    Block block
    {
        { arg_type->createColumnConst(1, arg_value), arg_type, "x" },
        { nullptr, res_type, "y" }
    };

    func->execute(block, {0}, 1);

    block.safeGetByPosition(1).column->get(0, res_value);
}


void PKCondition::traverseAST(const ASTPtr & node, const Context & context, Block & block_with_constants)
{
    RPNElement element;

    if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node))
    {
        if (operatorFromAST(func, element))
        {
            auto & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;
            for (size_t i = 0, size = args.size(); i < size; ++i)
            {
                traverseAST(args[i], context, block_with_constants);

                /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                  * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                  */
                if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
                    rpn.emplace_back(std::move(element));
            }

            return;
        }
    }

    if (!atomFromAST(node, context, block_with_constants, element))
    {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}


bool PKCondition::canConstantBeWrappedByMonotonicFunctions(
    const ASTPtr & node,
    size_t & out_primary_key_column_num,
    DataTypePtr & out_primary_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node->getColumnName();
    const auto & sample_block = pk_expr->getSampleBlock();
    if (!sample_block.has(expr_name))
        return false;

    bool found_transformation = false;
    for (const ExpressionAction & a : pk_expr->getActions())
    {
        /** The primary key functional expression constraint may be inferred from a plain column in the expression.
          * For example, if the primary key contains `toStartOfHour(Timestamp)` and query contains `WHERE Timestamp >= now()`,
          * it can be assumed that if `toStartOfHour()` is monotonic on [now(), inf), the `toStartOfHour(Timestamp) >= toStartOfHour(now())`
          * condition also holds, so the index may be used to select only parts satisfying this condition.
          *
          * To check the assumption, we'd need to assert that the inverse function to this transformation is also monotonic, however the
          * inversion isn't exported (or even viable for not strictly monotonic functions such as `toStartOfHour()`).
          * Instead, we can qualify only functions that do not transform the range (for example rounding),
          * which while not strictly monotonic, are monotonic everywhere on the input range.
          */
        const auto & action = a.argument_names;
        if (a.type == ExpressionAction::Type::APPLY_FUNCTION && action.size() == 1 && a.argument_names[0] == expr_name)
        {
            if (!a.function->hasInformationAboutMonotonicity())
                return false;

            // Range is irrelevant in this case
            IFunction::Monotonicity monotonicity = a.function->getMonotonicityForRange(*out_type, Field(), Field());
            if (!monotonicity.is_always_monotonic)
                return false;

            // Apply the next transformation step
            DataTypePtr new_type;
            applyFunction(a.function, out_type, out_value, new_type, out_value);
            if (!new_type)
                return false;

            out_type.swap(new_type);
            expr_name = a.result_name;

            // Transformation results in a primary key expression, accept
            auto it = pk_columns.find(expr_name);
            if (pk_columns.end() != it)
            {
                out_primary_key_column_num = it->second;
                out_primary_key_column_type = sample_block.getByName(it->first).type;
                found_transformation = true;
                break;
            }
        }
    }

    return found_transformation;
}

void PKCondition::getPKTuplePositionMapping(
    const ASTPtr & node,
    const Context & context,
    std::vector<MergeTreeSetIndex::PKTuplePositionMapping> & indexes_mapping,
    const size_t tuple_index,
    size_t & out_primary_key_column_num)
{
    MergeTreeSetIndex::PKTuplePositionMapping index_mapping;
    index_mapping.tuple_index = tuple_index;
    DataTypePtr data_type;
    if (isPrimaryKeyPossiblyWrappedByMonotonicFunctions(
            node, context, index_mapping.pk_index,
            data_type, index_mapping.functions))
    {
        indexes_mapping.push_back(index_mapping);
        if (out_primary_key_column_num < index_mapping.pk_index)
        {
            out_primary_key_column_num = index_mapping.pk_index;
        }
    }
}

// Try to prepare PKTuplePositionMapping for tuples from IN expression.
bool PKCondition::isTupleIndexable(
    const ASTPtr & node,
    const Context & context,
    RPNElement & out,
    const SetPtr & prepared_set,
    size_t & out_primary_key_column_num)
{
    out_primary_key_column_num = 0;
    const ASTFunction * node_tuple = typeid_cast<const ASTFunction *>(node.get());
    std::vector<MergeTreeSetIndex::PKTuplePositionMapping> indexes_mapping;
    if (node_tuple && node_tuple->name == "tuple")
    {
        size_t current_tuple_index = 0;
        for (const auto & arg : node_tuple->arguments->children)
        {
            getPKTuplePositionMapping(arg, context, indexes_mapping, current_tuple_index++, out_primary_key_column_num);
        }
    }
    else
    {
       getPKTuplePositionMapping(node, context, indexes_mapping, 0, out_primary_key_column_num);
    }

    if (indexes_mapping.empty())
    {
        return false;
    }

    out.set_index = std::make_shared<MergeTreeSetIndex>(
        prepared_set->getSetElements(), std::move(indexes_mapping));

    return true;
}

bool PKCondition::isPrimaryKeyPossiblyWrappedByMonotonicFunctions(
    const ASTPtr & node,
    const Context & context,
    size_t & out_primary_key_column_num,
    DataTypePtr & out_primary_key_res_column_type,
    RPNElement::MonotonicFunctionsChain & out_functions_chain)
{
    std::vector<const ASTFunction *> chain_not_tested_for_monotonicity;
    DataTypePtr primary_key_column_type;

    if (!isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_primary_key_column_num, primary_key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        auto func_builder = FunctionFactory::instance().tryGet((*it)->name, context);
        ColumnsWithTypeAndName arguments{{ nullptr, primary_key_column_type, "" }};
        auto func = func_builder->build(arguments);

        if (!func || !func->hasInformationAboutMonotonicity())
            return false;

        primary_key_column_type = func->getReturnType();
        out_functions_chain.push_back(func);
    }

    out_primary_key_res_column_type = primary_key_column_type;

    return true;
}

bool PKCondition::isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const ASTPtr & node,
    size_t & out_primary_key_column_num,
    DataTypePtr & out_primary_key_column_type,
    std::vector<const ASTFunction *> & out_functions_chain)
{
    /** By itself, the primary key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = pk_expr->getSampleBlock();
    String name = node->getColumnName();

    auto it = pk_columns.find(name);
    if (pk_columns.end() != it)
    {
        out_primary_key_column_num = it->second;
        out_primary_key_column_type = sample_block.getByName(it->first).type;
        return true;
    }

    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        const auto & args = func->arguments->children;
        if (args.size() != 1)
            return false;

        out_functions_chain.push_back(func);

        if (!isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(args[0], out_primary_key_column_num, out_primary_key_column_type,
                                                                 out_functions_chain))
            return false;

        return true;
    }

    return false;
}


static void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const ASTPtr & node)
{
    if (desired_type->equals(*src_type))
        return;

    try
    {
        /// NOTE: We don't need accurate info about src_type at this moment
        src_value = convertFieldToType(src_value, *desired_type);
    }
    catch (...)
    {
        throw Exception("Primary key expression contains comparison between inconvertible types: " +
            desired_type->getName() + " and " + src_type->getName() +
            " inside " + queryToString(node),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


bool PKCondition::atomFromAST(const ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out)
{
    /** Functions < > = != <= >= in `notIn`, where one argument is a constant, and the other is one of columns of primary key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  or constant expression - number.
      */
    Field const_value;
    DataTypePtr const_type;
    if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        if (args.size() != 2)
            return false;

        DataTypePtr key_expr_type;    /// Type of expression containing primary key column
        size_t key_arg_pos;           /// Position of argument with primary key column (non-const argument)
        size_t key_column_num;        /// Number of a primary key column (inside sort_descr array)
        RPNElement::MonotonicFunctionsChain chain;
        bool is_set_const = false;
        bool is_constant_transformed = false;

        if (prepared_sets.count(args[1].get())
            && isTupleIndexable(args[0], context, out, prepared_sets[args[1].get()], key_column_num))
        {
            key_arg_pos = 0;
            is_set_const = true;
        }
        else if (getConstant(args[1], block_with_constants, const_value, const_type)
            && isPrimaryKeyPossiblyWrappedByMonotonicFunctions(args[0], context, key_column_num, key_expr_type, chain))
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
            && isPrimaryKeyPossiblyWrappedByMonotonicFunctions(args[1], context, key_column_num, key_expr_type, chain))
        {
            key_arg_pos = 1;
        }
        else if (getConstant(args[0], block_with_constants, const_value, const_type)
            &&  canConstantBeWrappedByMonotonicFunctions(args[1], key_column_num, key_expr_type, const_value, const_type))
        {
            key_arg_pos = 1;
            is_constant_transformed = true;
        }
        else
            return false;

        std::string func_name = func->name;

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
            else if (func_name == "in" || func_name == "notIn" || func_name == "like")
            {
                /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                return false;
            }
        }

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);

        const auto atom_it = atom_map.find(func_name);
        if (atom_it == std::end(atom_map))
            return false;

        bool cast_not_needed =
            is_set_const /// Set args are already casted inside Set::createFromAST
            || (key_expr_type->isNumber() && const_type->isNumber()); /// Numbers are accurately compared without cast.

        if (!cast_not_needed)
            castValueToType(key_expr_type, const_value, const_type, node);

        return atom_it->second(out, const_value, node);
    }
    else if (getConstant(node, block_with_constants, const_value, const_type))    /// For cases where it says, for example, `WHERE 0 AND something`
    {
        if (const_value.getType() == Field::Types::UInt64
            || const_value.getType() == Field::Types::Int64
            || const_value.getType() == Field::Types::Float64)
        {
            /// Zero in all types is represented in memory the same way as in UInt64.
            out.function = const_value.get<UInt64>()
                ? RPNElement::ALWAYS_TRUE
                : RPNElement::ALWAYS_FALSE;

            return true;
        }
    }

    return false;
}

bool PKCondition::operatorFromAST(const ASTFunction * func, RPNElement & out)
{
    /// Functions AND, OR, NOT.
    /** Also a special function `indexHint` - works as if instead of calling a function there are just parentheses
      * (or, the same thing - calling the function `and` from one argument).
      */
    const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;

        out.function = RPNElement::FUNCTION_NOT;
    }
    else
    {
        if (func->name == "and" || func->name == "indexHint")
            out.function = RPNElement::FUNCTION_AND;
        else if (func->name == "or")
            out.function = RPNElement::FUNCTION_OR;
        else
            return false;
    }

    return true;
}

String PKCondition::toString() const
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


/** Index is the value of primary key every `index_granularity` rows.
  * This value is called a "mark". That is, the index consists of marks.
  *
  * The primary key is the tuple.
  * The data is sorted by primary key in the sense of lexicographic order over tuples.
  *
  * A pair of marks specifies a segment with respect to the order over the tuples.
  * Denote it like this: [ x1 y1 z1 .. x2 y2 z2 ],
  *  where x1 y1 z1 - tuple - value of primary key in left border of segment;
  *        x2 y2 z2 - tuple - value of primary key in right boundary of segment.
  * In this section there are data between these marks.
  *
  * Or, the last mark specifies the range open on the right: [ a b c .. + inf )
  *
  * The set of all possible tuples can be considered as an n-dimensional space, where n is the size of the tuple.
  * A range of tuples specifies some subset of this space.
  *
  * Parallelograms (you can also find the term "rail")
  *  will be the subrange of an n-dimensional space that is a direct product of one-dimensional ranges.
  * In this case, the one-dimensional range can be: a period, a segment, an interval, a half-interval, unlimited on the left, unlimited on the right ...
  *
  * The range of tuples can always be represented as a combination of parallelograms.
  * For example, the range [ x1 y1 .. x2 y2 ] given x1 != x2 is equal to the union of the following three parallelograms:
  * [x1]       x [y1 .. +inf)
  * (x1 .. x2) x (-inf .. +inf)
  * [x2]       x (-inf .. y2]
  *
  * Or, for example, the range [ x1 y1 .. +inf ] is equal to the union of the following two parallelograms:
  * [x1]         x [y1 .. +inf)
  * (x1 .. +inf) x (-inf .. +inf)
  * It's easy to see that this is a special case of the variant above.
  *
  * This is important because it is easy for us to check the feasibility of the condition over the parallelogram,
  *  and therefore, feasibility of condition on the range of tuples will be checked by feasibility of condition
  *  over at least one parallelogram from which this range consists.
  */

template <typename F>
static bool forAnyParallelogram(
    size_t key_size,
    const Field * key_left,
    const Field * key_right,
    bool left_bounded,
    bool right_bounded,
    std::vector<Range> & parallelogram,
    size_t prefix_size,
    F && callback)
{
    if (!left_bounded && !right_bounded)
        return callback(parallelogram);

    if (left_bounded && right_bounded)
    {
        /// Let's go through the matching elements of the key.
        while (prefix_size < key_size)
        {
            if (key_left[prefix_size] == key_right[prefix_size])
            {
                /// Point ranges.
                parallelogram[prefix_size] = Range(key_left[prefix_size]);
                ++prefix_size;
            }
            else
                break;
        }
    }

    if (prefix_size == key_size)
        return callback(parallelogram);

    if (prefix_size + 1 == key_size)
    {
        if (left_bounded && right_bounded)
            parallelogram[prefix_size] = Range(key_left[prefix_size], true, key_right[prefix_size], true);
        else if (left_bounded)
            parallelogram[prefix_size] = Range::createLeftBounded(key_left[prefix_size], true);
        else if (right_bounded)
            parallelogram[prefix_size] = Range::createRightBounded(key_right[prefix_size], true);

        return callback(parallelogram);
    }

    /// (x1 .. x2) x (-inf .. +inf)

    if (left_bounded && right_bounded)
        parallelogram[prefix_size] = Range(key_left[prefix_size], false, key_right[prefix_size], false);
    else if (left_bounded)
        parallelogram[prefix_size] = Range::createLeftBounded(key_left[prefix_size], false);
    else if (right_bounded)
        parallelogram[prefix_size] = Range::createRightBounded(key_right[prefix_size], false);

    for (size_t i = prefix_size + 1; i < key_size; ++i)
        parallelogram[i] = Range();

    if (callback(parallelogram))
        return true;

    /// [x1]       x [y1 .. +inf)

    if (left_bounded)
    {
        parallelogram[prefix_size] = Range(key_left[prefix_size]);
        if (forAnyParallelogram(key_size, key_left, key_right, true, false, parallelogram, prefix_size + 1, callback))
            return true;
    }

    /// [x2]       x (-inf .. y2]

    if (right_bounded)
    {
        parallelogram[prefix_size] = Range(key_right[prefix_size]);
        if (forAnyParallelogram(key_size, key_left, key_right, false, true, parallelogram, prefix_size + 1, callback))
            return true;
    }

    return false;
}


bool PKCondition::mayBeTrueInRange(
    size_t used_key_size,
    const Field * left_pk,
    const Field * right_pk,
    const DataTypes & data_types,
    bool right_bounded) const
{
    std::vector<Range> key_ranges(used_key_size, Range());

/*  std::cerr << "Checking for: [";
    for (size_t i = 0; i != used_key_size; ++i)
        std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), left_pk[i]);
    std::cerr << " ... ";

    if (right_bounded)
    {
        for (size_t i = 0; i != used_key_size; ++i)
            std::cerr << (i != 0 ? ", " : "") << applyVisitor(FieldVisitorToString(), right_pk[i]);
        std::cerr << "]\n";
    }
    else
        std::cerr << "+inf)\n";*/

    return forAnyParallelogram(used_key_size, left_pk, right_pk, true, right_bounded, key_ranges, 0,
        [&] (const std::vector<Range> & key_ranges)
    {
        auto res = mayBeTrueInRangeImpl(key_ranges, data_types);

/*      std::cerr << "Parallelogram: ";
        for (size_t i = 0, size = key_ranges.size(); i != size; ++i)
            std::cerr << (i != 0 ? " x " : "") << key_ranges[i].toString();
        std::cerr << ": " << res << "\n";*/

        return res;
    });
}

std::optional<Range> PKCondition::applyMonotonicFunctionsChainToRange(
    Range key_range,
    RPNElement::MonotonicFunctionsChain & functions,
    DataTypePtr current_type
)
{
    for (auto & func : functions)
    {
        /// We check the monotonicity of each function on a specific range.
        IFunction::Monotonicity monotonicity = func->getMonotonicityForRange(
            *current_type.get(), key_range.left, key_range.right);

        if (!monotonicity.is_monotonic)
        {
            return {};
        }

        /// Apply the function.
        DataTypePtr new_type;
        if (!key_range.left.isNull())
            applyFunction(func, current_type, key_range.left, new_type, key_range.left);
        if (!key_range.right.isNull())
            applyFunction(func, current_type, key_range.right, new_type, key_range.right);

        if (!new_type)
        {
            return {};
        }

        current_type.swap(new_type);

        if (!monotonicity.is_positive)
            key_range.swapLeftAndRight();
    }
    return key_range;
}

bool PKCondition::mayBeTrueInRangeImpl(const std::vector<Range> & key_ranges, const DataTypes & data_types) const
{
    std::vector<BoolMask> rpn_stack;
    for (size_t i = 0; i < rpn.size(); ++i)
    {
        const auto & element = rpn[i];
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN_RANGE
            || element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
        {
            const Range * key_range = &key_ranges[element.key_column];

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
            auto in_func = typeid_cast<const ASTFunction *>(element.in_function.get());
            const ASTs & args = typeid_cast<const ASTExpressionList &>(*in_func->arguments).children;
            PreparedSets::const_iterator it = prepared_sets.find(args[1].get());
            if (in_func && it != prepared_sets.end())
            {
                rpn_stack.emplace_back(element.set_index->mayBeTrueInRange(key_ranges, data_types));
                if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
                {
                    rpn_stack.back() = !rpn_stack.back();
                }
            }
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
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
            throw Exception("Unexpected function type in PKCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in PKCondition::mayBeTrueInRange", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}


bool PKCondition::mayBeTrueInRange(
    size_t used_key_size, const Field * left_pk, const Field * right_pk, const DataTypes & data_types) const
{
    return mayBeTrueInRange(used_key_size, left_pk, right_pk, data_types, true);
}

bool PKCondition::mayBeTrueAfter(
    size_t used_key_size, const Field * left_pk, const DataTypes & data_types) const
{
    return mayBeTrueInRange(used_key_size, left_pk, nullptr, data_types, false);
}


String PKCondition::RPNElement::toString() const
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
            ss << (function == FUNCTION_IN_SET ? " in set" : " notIn set");
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
        default:
            throw Exception("Unknown function in RPNElement", ErrorCodes::LOGICAL_ERROR);
    }
}


bool PKCondition::alwaysUnknownOrTrue() const
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
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else
            throw Exception("Unexpected function type in PKCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    return rpn_stack[0];
}


size_t PKCondition::getMaxKeyColumn() const
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

}
