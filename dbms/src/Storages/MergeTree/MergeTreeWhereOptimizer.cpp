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
#include <Interpreters/QueryNormalizer.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <ext/scope_guard.h>
#include <ext/collection_cast.h>
#include <ext/map.h>
#include <memory>
#include <unordered_map>
#include <tuple>
#include <cstddef>


namespace DB
{

static constexpr auto threshold = 2;


MergeTreeWhereOptimizer::MergeTreeWhereOptimizer(
    SelectQueryInfo & query_info,
    const Context & context,
    const MergeTreeData & data,
    const Names & column_names,
    Logger * log)
        : table_columns{ext::map<std::unordered_set>(data.getColumns().getAllPhysical(),
            [] (const NameAndTypePair & col) { return col.name; })},
        block_with_constants{KeyCondition::getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context)},
        log{log}
{
    if (!data.primary_key_columns.empty())
        first_primary_key_column = data.primary_key_columns[0];

    calculateColumnSizes(data, column_names);
    auto & select = typeid_cast<ASTSelectQuery &>(*query_info.query);
    determineArrayJoinedNames(select);
    optimize(select);
}


void MergeTreeWhereOptimizer::optimize(ASTSelectQuery & select) const
{
    if (!select.where_expression || select.prewhere_expression)
        return;

    const auto function = typeid_cast<ASTFunction *>(select.where_expression.get());
    if (function && function->name == "and")
        optimizeConjunction(select, function);
    else
        optimizeArbitrary(select);
}


void MergeTreeWhereOptimizer::calculateColumnSizes(const MergeTreeData & data, const Names & column_names)
{
    for (const auto & column_name : column_names)
        column_sizes[column_name] = data.getColumnCompressedSize(column_name);
}


namespace
{
struct ConditionCandidate
{
    size_t columns_size;
    int64_t position;
    IdentifierNameSet identifiers;
    bool is_good;

    auto tuple() const
    {
        /// We'll move conditions from back to keep "position".
        return std::forward_as_tuple(!is_good, columns_size, -position);
    }

    bool operator< (const ConditionCandidate & rhs) const
    {
        return tuple() < rhs.tuple();
    }
};
}


void MergeTreeWhereOptimizer::optimizeConjunction(ASTSelectQuery & select, ASTFunction * const fun) const
{
    std::vector<ConditionCandidate> condition_candidates;

    auto & conditions = fun->arguments->children;

    /// remove condition by swapping it with the last one and calling ::pop_back()
    const auto remove_condition_at_index = [&conditions] (const size_t idx)
    {
        if (idx < conditions.size() - 1)
            std::swap(conditions[idx], conditions.back());
        conditions.pop_back();
    };

    /// linearize conjunction and divide conditions into "good" and not-"good" ones
    for (size_t idx = 0; idx < conditions.size();)
    {
        const auto condition = conditions[idx].get();

        /// linearize sub-conjunctions
        if (const auto function = typeid_cast<ASTFunction *>(condition))
        {
            if (function->name == "and")
            {
                for (auto & child : function->arguments->children)
                    conditions.emplace_back(std::move(child));

                /// remove the condition corresponding to conjunction
                remove_condition_at_index(idx);

                /// continue iterating without increment to ensure the just added conditions are processed
                continue;
            }
        }

        SCOPE_EXIT(++idx);

        if (cannotBeMoved(conditions[idx]))
            continue;

        IdentifierNameSet identifiers{};
        collectIdentifiersNoSubqueries(condition, identifiers);

        /// do not take into consideration the conditions consisting only of the first primary key column
        if (!hasPrimaryKeyAtoms(condition) && isSubsetOfTableColumns(identifiers))
        {
            ConditionCandidate candidate;
            candidate.position = idx;
            candidate.columns_size = getIdentifiersColumnSize(identifiers);
            candidate.is_good = isConditionGood(condition);
            candidate.identifiers = identifiers;
            condition_candidates.emplace_back(std::move(candidate));
        }
    }

    if (condition_candidates.empty())
        return;

    const auto move_condition_to_prewhere = [&] (const size_t idx)
    {
        if (!select.prewhere_expression)
        {
            select.prewhere_expression = conditions[idx];
            select.children.push_back(select.prewhere_expression);
        }
        else if (auto func_and = typeid_cast<ASTFunction *>(select.prewhere_expression.get()); func_and && func_and->name == "and")
        {
            /// Add argument to AND chain

            func_and->arguments->children.emplace_back(conditions[idx]);
        }
        else
        {
            /// Make old_cond AND new_cond

            auto func = std::make_shared<ASTFunction>();
            func->name = "and";
            func->arguments->children = {select.prewhere_expression, conditions[idx]};

            select.children.clear();
            select.prewhere_expression = std::move(func);
            select.children.push_back(select.prewhere_expression);
        }

        /** Replace conjunction with the only remaining argument if only two conditions were present,
          *  remove selected condition from conjunction otherwise.
          */
        if (conditions.size() == 2)
        {
            /// find old where_expression in children of select
            const auto it = std::find(std::begin(select.children), std::end(select.children), select.where_expression);
            /// replace where_expression with the remaining argument
            select.where_expression = std::move(conditions[idx == 0 ? 1 : 0]);
            /// overwrite child entry with the new where_expression
            *it = select.where_expression;
        }
        else
            remove_condition_at_index(idx);
    };

    /// Lightest conditions first. NOTE The algorithm is suboptimal, replace with priority_queue if you want.
    std::sort(condition_candidates.begin(), condition_candidates.end());

    /// Pick the best condition and also all other conditions with the same set of columns.
    /// For example, if we take "EventTime >= '2014-03-20 00:00:00'", we will also take "EventTime < '2014-03-21 00:00:00'".

    IdentifierNameSet identifiers_of_moved_condition = condition_candidates[0].identifiers;
    move_condition_to_prewhere(condition_candidates[0].position);

    for (size_t i = 1, size = condition_candidates.size(); i < size; ++i)
    {
        if (identifiers_of_moved_condition == condition_candidates[i].identifiers)
            move_condition_to_prewhere(condition_candidates[i].position);
        else
            break;
    }

    if (select.prewhere_expression)
        LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition \"" << select.prewhere_expression << "\" moved to PREWHERE");
}


void MergeTreeWhereOptimizer::optimizeArbitrary(ASTSelectQuery & select) const
{
    auto & condition = select.where_expression;

    /// do not optimize restricted expressions
    if (cannotBeMoved(select.where_expression))
        return;

    IdentifierNameSet identifiers{};
    collectIdentifiersNoSubqueries(condition.get(), identifiers);

    if (hasPrimaryKeyAtoms(condition.get()) || !isSubsetOfTableColumns(identifiers))
        return;

    /// add the condition to PREWHERE, remove it from WHERE
    std::swap(select.prewhere_expression, condition);
    LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition `" << select.prewhere_expression << "` moved to PREWHERE");
}


size_t MergeTreeWhereOptimizer::getIdentifiersColumnSize(const IdentifierNameSet & identifiers) const
{
    /** for expressions containing no columns (or where columns could not be determined otherwise) assume maximum
        *    possible size so they do not have priority in eligibility over other expressions. */
    if (identifiers.empty())
        return std::numeric_limits<size_t>::max();

    size_t size{};

    for (const auto & identifier : identifiers)
        if (column_sizes.count(identifier))
            size += column_sizes.find(identifier)->second;

    return size;
}


bool MergeTreeWhereOptimizer::isConditionGood(const IAST * condition) const
{
    const auto function = typeid_cast<const ASTFunction *>(condition);
    if (!function)
        return false;

    /** we are only considering conditions of form `equals(one, another)` or `one = another`,
        * especially if either `one` or `another` is ASTIdentifier */
    if (function->name != "equals")
        return false;

    auto left_arg = function->arguments->children.front().get();
    auto right_arg = function->arguments->children.back().get();

    /// try to ensure left_arg points to ASTIdentifier
    if (!isIdentifier(left_arg) && isIdentifier(right_arg))
        std::swap(left_arg, right_arg);

    if (isIdentifier(left_arg))
    {
        /// condition may be "good" if only right_arg is a constant and its value is outside the threshold
        if (const auto literal = typeid_cast<const ASTLiteral *>(right_arg))
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


void MergeTreeWhereOptimizer::collectIdentifiersNoSubqueries(const IAST * const ast, IdentifierNameSet & set)
{
    if (auto opt_name = getIdentifierName(ast))
        return (void) set.insert(*opt_name);

    if (typeid_cast<const ASTSubquery *>(ast))
        return;

    for (const auto & child : ast->children)
        collectIdentifiersNoSubqueries(child.get(), set);
}


bool MergeTreeWhereOptimizer::hasPrimaryKeyAtoms(const IAST * ast) const
{
    if (const auto func = typeid_cast<const ASTFunction *>(ast))
    {
        const auto & args = func->arguments->children;

        if ((func->name == "not" && 1 == args.size()) || func->name == "and" || func->name == "or")
        {
            for (const auto & arg : args)
                if (hasPrimaryKeyAtoms(arg.get()))
                    return true;

            return false;
        }
    }

    return isPrimaryKeyAtom(ast);
}


bool MergeTreeWhereOptimizer::isPrimaryKeyAtom(const IAST * const ast) const
{
    if (const auto func = typeid_cast<const ASTFunction *>(ast))
    {
        if (!KeyCondition::atom_map.count(func->name))
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


bool MergeTreeWhereOptimizer::isConstant(const ASTPtr & expr) const
{
    const auto column_name = expr->getColumnName();

    if (typeid_cast<const ASTLiteral *>(expr.get()) ||
        (block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isColumnConst()))
        return true;

    return false;
}


bool MergeTreeWhereOptimizer::isSubsetOfTableColumns(const IdentifierNameSet & identifiers) const
{
    for (const auto & identifier : identifiers)
        if (table_columns.count(identifier) == 0)
            return false;

    return true;
}


bool MergeTreeWhereOptimizer::cannotBeMoved(const ASTPtr & ptr) const
{
    if (const auto function_ptr = typeid_cast<const ASTFunction *>(ptr.get()))
    {
        /// disallow arrayJoin expressions to be moved to PREWHERE for now
        if ("arrayJoin" == function_ptr->name)
            return true;

        /// disallow GLOBAL IN, GLOBAL NOT IN
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
        if (array_joined_names.count(*opt_name) ||
            array_joined_names.count(Nested::extractTableName(*opt_name)))
            return true;
    }

    for (const auto & child : ptr->children)
        if (cannotBeMoved(child))
            return true;

    return false;
}


void MergeTreeWhereOptimizer::determineArrayJoinedNames(ASTSelectQuery & select)
{
    auto array_join_expression_list = select.array_join_expression_list();

    /// much simplified code from ExpressionAnalyzer::getArrayJoinedColumns()
    if (!array_join_expression_list)
        return;

    for (const auto & ast : array_join_expression_list->children)
        array_joined_names.emplace(ast->getAliasOrColumnName());
}

}
