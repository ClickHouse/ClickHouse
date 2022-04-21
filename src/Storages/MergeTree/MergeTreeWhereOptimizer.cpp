#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/misc.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <base/map.h>

namespace DB
{

namespace ErrorCodes
{
}

/// Conditions like "x = N" are considered good if abs(N) > threshold.
/// This is used to assume that condition is likely to have good selectivity.
static constexpr auto threshold = 2;


MergeTreeWhereOptimizer::MergeTreeWhereOptimizer(
    SelectQueryInfo & query_info,
    ContextPtr context,
    std::unordered_map<std::string, UInt64> column_sizes_,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & queried_columns_,
    Poco::Logger * log_)
    : table_columns{collections::map<std::unordered_set>(
        metadata_snapshot->getColumns().getAllPhysical(), [](const NameAndTypePair & col) { return col.name; })}
    , queried_columns{queried_columns_}
    , sorting_key_names{NameSet(
          metadata_snapshot->getSortingKey().column_names.begin(), metadata_snapshot->getSortingKey().column_names.end())}
    , block_with_constants{KeyCondition::getBlockWithConstants(query_info.query->clone(), query_info.syntax_analyzer_result, context)}
    , log{log_}
    , column_sizes{std::move(column_sizes_)}
{
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    if (!primary_key.column_names.empty())
        first_primary_key_column = primary_key.column_names[0];

    for (const auto & name : queried_columns)
    {
        auto it = column_sizes.find(name);
        if (it != column_sizes.end())
            total_size_of_queried_columns += it->second;
    }

    determineArrayJoinedNames(query_info.query->as<ASTSelectQuery &>());
    optimize(query_info.query->as<ASTSelectQuery &>());
}


static void collectIdentifiersNoSubqueries(const ASTPtr & ast, NameSet & set)
{
    if (auto opt_name = tryGetIdentifierName(ast))
        return (void)set.insert(*opt_name);

    if (ast->as<ASTSubquery>())
        return;

    for (const auto & child : ast->children)
        collectIdentifiersNoSubqueries(child, set);
}

static bool isConditionGood(const ASTPtr & condition)
{
    const auto * function = condition->as<ASTFunction>();
    if (!function)
        return false;

    /** we are only considering conditions of form `equals(one, another)` or `one = another`,
        * especially if either `one` or `another` is ASTIdentifier */
    if (function->name != "equals")
        return false;

    auto * left_arg = function->arguments->children.front().get();
    auto * right_arg = function->arguments->children.back().get();

    /// try to ensure left_arg points to ASTIdentifier
    if (!left_arg->as<ASTIdentifier>() && right_arg->as<ASTIdentifier>())
        std::swap(left_arg, right_arg);

    if (left_arg->as<ASTIdentifier>())
    {
        /// condition may be "good" if only right_arg is a constant and its value is outside the threshold
        if (const auto * literal = right_arg->as<ASTLiteral>())
        {
            const auto & field = literal->value;
            const auto type = field.getType();

            /// check the value with respect to threshold
            if (type == Field::Types::UInt64)
            {
                const auto value = field.get<UInt64>();
                return value > threshold;
            }
            else if (type == Field::Types::Int64)
            {
                const auto value = field.get<Int64>();
                return value < -threshold || threshold < value;
            }
            else if (type == Field::Types::Float64)
            {
                const auto value = field.get<Float64>();
                return value < threshold || threshold < value;
            }
        }
    }

    return false;
}

static const ASTFunction * getAsTuple(const ASTPtr & node)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "tuple")
        return func;
    return {};
};

static bool getAsTupleLiteral(const ASTPtr & node, Tuple & tuple)
{
    if (const auto * value_tuple = node->as<ASTLiteral>())
        return value_tuple && value_tuple->value.tryGet<Tuple>(tuple);
    return false;
};

bool MergeTreeWhereOptimizer::tryAnalyzeTuple(Conditions & res, const ASTFunction * func, bool is_final) const
{
    if (!func || func->name != "equals" || func->arguments->children.size() != 2)
        return false;

    Tuple tuple_lit;
    const ASTFunction * tuple_other = nullptr;
    if (getAsTupleLiteral(func->arguments->children[0], tuple_lit))
        tuple_other = getAsTuple(func->arguments->children[1]);
    else if (getAsTupleLiteral(func->arguments->children[1], tuple_lit))
        tuple_other = getAsTuple(func->arguments->children[0]);

    if (!tuple_other || tuple_lit.size() != tuple_other->arguments->children.size())
        return false;

    for (size_t i = 0; i < tuple_lit.size(); ++i)
    {
        const auto & child = tuple_other->arguments->children[i];
        std::shared_ptr<IAST> fetch_sign_column = nullptr;
        /// tuple in tuple like (a, (b, c)) = (1, (2, 3))
        if (const auto * child_func = getAsTuple(child))
            fetch_sign_column = std::make_shared<ASTFunction>(*child_func);
        else if (const auto * child_ident = child->as<ASTIdentifier>())
            fetch_sign_column = std::make_shared<ASTIdentifier>(child_ident->name());
        else
            return false;

        ASTPtr fetch_sign_value = std::make_shared<ASTLiteral>(tuple_lit.at(i));
        ASTPtr func_node = makeASTFunction("equals", fetch_sign_column, fetch_sign_value);
        analyzeImpl(res, func_node, is_final);
    }

    return true;
}

void MergeTreeWhereOptimizer::analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final) const
{
    const auto * func = node->as<ASTFunction>();

    if (func && func->name == "and")
    {
        for (const auto & elem : func->arguments->children)
            analyzeImpl(res, elem, is_final);
    }
    else if (tryAnalyzeTuple(res, func, is_final))
    {
        /// analyzed
    }
    else
    {
        Condition cond;
        cond.node = node;

        collectIdentifiersNoSubqueries(node, cond.identifiers);

        cond.columns_size = getIdentifiersColumnSize(cond.identifiers);

        cond.viable =
            /// Condition depend on some column. Constant expressions are not moved.
            !cond.identifiers.empty()
            && !cannotBeMoved(node, is_final)
            /// Do not take into consideration the conditions consisting only of the first primary key column
            && !hasPrimaryKeyAtoms(node)
            /// Only table columns are considered. Not array joined columns. NOTE We're assuming that aliases was expanded.
            && isSubsetOfTableColumns(cond.identifiers)
            /// Do not move conditions involving all queried columns.
            && cond.identifiers.size() < queried_columns.size();

        if (cond.viable)
            cond.good = isConditionGood(node);

        res.emplace_back(std::move(cond));
    }
}

/// Transform conjunctions chain in WHERE expression to Conditions list.
MergeTreeWhereOptimizer::Conditions MergeTreeWhereOptimizer::analyze(const ASTPtr & expression, bool is_final) const
{
    Conditions res;
    analyzeImpl(res, expression, is_final);
    return res;
}

/// Transform Conditions list to WHERE or PREWHERE expression.
ASTPtr MergeTreeWhereOptimizer::reconstruct(const Conditions & conditions)
{
    if (conditions.empty())
        return {};

    if (conditions.size() == 1)
        return conditions.front().node;

    const auto function = std::make_shared<ASTFunction>();

    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    for (const auto & elem : conditions)
        function->arguments->children.push_back(elem.node);

    return function;
}


void MergeTreeWhereOptimizer::optimize(ASTSelectQuery & select) const
{
    if (!select.where() || select.prewhere())
        return;

    Conditions where_conditions = analyze(select.where(), select.final());
    Conditions prewhere_conditions;

    UInt64 total_size_of_moved_conditions = 0;
    UInt64 total_number_of_moved_columns = 0;

    /// Move condition and all other conditions depend on the same set of columns.
    auto move_condition = [&](Conditions::iterator cond_it)
    {
        prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, cond_it);
        total_size_of_moved_conditions += cond_it->columns_size;
        total_number_of_moved_columns += cond_it->identifiers.size();

        /// Move all other viable conditions that depend on the same set of columns.
        for (auto jt = where_conditions.begin(); jt != where_conditions.end();)
        {
            if (jt->viable && jt->columns_size == cond_it->columns_size && jt->identifiers == cond_it->identifiers)
                prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, jt++);
            else
                ++jt;
        }
    };

    /// Move conditions unless the ratio of total_size_of_moved_conditions to the total_size_of_queried_columns is less than some threshold.
    while (!where_conditions.empty())
    {
        /// Move the best condition to PREWHERE if it is viable.

        auto it = std::min_element(where_conditions.begin(), where_conditions.end());

        if (!it->viable)
            break;

        bool moved_enough = false;
        if (total_size_of_queried_columns > 0)
        {
            /// If we know size of queried columns use it as threshold. 10% ratio is just a guess.
            moved_enough = total_size_of_moved_conditions > 0
                && (total_size_of_moved_conditions + it->columns_size) * 10 > total_size_of_queried_columns;
        }
        else
        {
            /// Otherwise, use number of moved columns as a fallback.
            /// It can happen, if table has only compact parts. 25% ratio is just a guess.
            moved_enough = total_number_of_moved_columns > 0
                && (total_number_of_moved_columns + it->identifiers.size()) * 4 > queried_columns.size();
        }

        if (moved_enough)
            break;

        move_condition(it);
    }

    /// Nothing was moved.
    if (prewhere_conditions.empty())
        return;

    /// Rewrite the SELECT query.

    select.setExpression(ASTSelectQuery::Expression::WHERE, reconstruct(where_conditions));
    select.setExpression(ASTSelectQuery::Expression::PREWHERE, reconstruct(prewhere_conditions));

    LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition \"{}\" moved to PREWHERE", select.prewhere());
}


UInt64 MergeTreeWhereOptimizer::getIdentifiersColumnSize(const NameSet & identifiers) const
{
    UInt64 size = 0;

    for (const auto & identifier : identifiers)
        if (column_sizes.contains(identifier))
            size += column_sizes.at(identifier);

    return size;
}


bool MergeTreeWhereOptimizer::hasPrimaryKeyAtoms(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        const auto & args = func->arguments->children;

        if ((func->name == "not" && 1 == args.size()) || func->name == "and" || func->name == "or")
        {
            for (const auto & arg : args)
                if (hasPrimaryKeyAtoms(arg))
                    return true;

            return false;
        }
    }

    return isPrimaryKeyAtom(ast);
}


bool MergeTreeWhereOptimizer::isPrimaryKeyAtom(const ASTPtr & ast) const
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (!KeyCondition::atom_map.contains(func->name))
            return false;

        const auto & args = func->arguments->children;
        if (args.size() != 2)
            return false;

        const auto & first_arg_name = args.front()->getColumnName();
        const auto & second_arg_name = args.back()->getColumnName();

        if ((first_primary_key_column == first_arg_name && isConstant(args[1]))
            || (first_primary_key_column == second_arg_name && isConstant(args[0]))
            || (first_primary_key_column == first_arg_name && functionIsInOrGlobalInOperator(func->name)))
            return true;
    }

    return false;
}


bool MergeTreeWhereOptimizer::isSortingKey(const String & column_name) const
{
    return sorting_key_names.contains(column_name);
}


bool MergeTreeWhereOptimizer::isConstant(const ASTPtr & expr) const
{
    const auto column_name = expr->getColumnName();

    return expr->as<ASTLiteral>()
        || (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column));
}


bool MergeTreeWhereOptimizer::isSubsetOfTableColumns(const NameSet & identifiers) const
{
    for (const auto & identifier : identifiers)
        if (!table_columns.contains(identifier))
            return false;

    return true;
}


bool MergeTreeWhereOptimizer::cannotBeMoved(const ASTPtr & ptr, bool is_final) const
{
    if (const auto * function_ptr = ptr->as<ASTFunction>())
    {
        /// disallow arrayJoin expressions to be moved to PREWHERE for now
        if ("arrayJoin" == function_ptr->name)
            return true;

        /// disallow GLOBAL IN, GLOBAL NOT IN
        /// TODO why?
        if ("globalIn" == function_ptr->name
            || "globalNotIn" == function_ptr->name)
            return true;

        /// indexHint is a special function that it does not make sense to transfer to PREWHERE
        if ("indexHint" == function_ptr->name)
            return true;
    }
    else if (auto opt_name = IdentifierSemantic::getColumnName(ptr))
    {
        /// disallow moving result of ARRAY JOIN to PREWHERE
        if (array_joined_names.contains(*opt_name) ||
            array_joined_names.contains(Nested::extractTableName(*opt_name)) ||
            (is_final && !isSortingKey(*opt_name)))
            return true;
    }

    for (const auto & child : ptr->children)
        if (cannotBeMoved(child, is_final))
            return true;

    return false;
}


void MergeTreeWhereOptimizer::determineArrayJoinedNames(ASTSelectQuery & select)
{
    auto [array_join_expression_list, _] = select.arrayJoinExpressionList();

    /// much simplified code from ExpressionAnalyzer::getArrayJoinedColumns()
    if (!array_join_expression_list)
        return;

    for (const auto & ast : array_join_expression_list->children)
        array_joined_names.emplace(ast->getAliasOrColumnName());
}

}
