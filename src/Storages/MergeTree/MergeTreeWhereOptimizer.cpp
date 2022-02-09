#include <memory>
#include <unordered_map>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
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
#include "Common/Exception.h"
#include <Common/typeid_cast.h>
#include "Core/Field.h"
#include "Interpreters/StorageID.h"
#include "Storages/StorageMerge.h"
#include <DataTypes/NestedUtils.h>
#include <base/map.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
}

/// Conditions like "x = N" are considered good if abs(N) > threshold.
/// This is used to assume that condition is likely to have good selectivity.
static constexpr auto threshold = 2;
static constexpr double EPS = 1e-9;
static constexpr double RANK_CORRECTION = 1e9;


MergeTreeWhereOptimizer::MergeTreeWhereOptimizer(
    SelectQueryInfo & query_info,
    ContextPtr context,
    const Settings & settings,
    std::unordered_map<std::string, UInt64> column_sizes_,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePtr & storage_,
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
    , stats(storage_->getStatisticsByPartitionPredicate(query_info, context))
    , use_new_scoring(settings.allow_experimental_stats_for_prewhere_optimization && stats != nullptr)
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
                return value < -threshold || threshold < value;
            }
        }
    }

    return false;
}

static bool isConditionGoodNew(const ASTPtr & condition)
{
    const auto * function = condition->as<ASTFunction>();
    if (!function)
        return false;

    std::unordered_set<String> compare_funcs = {
        "equals",
        "notEquals",
        "less",
        "greater",
        "greaterOrEquals",
        "lessOrEquals",
    };
    if (!compare_funcs.contains(function->name))
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
            const auto type = literal->value.getType();
            return type == Field::Types::UInt64 || type == Field::Types::Int64 || type == Field::Types::Float64;
        }
    }
    return false;
}

std::optional<MergeTreeWhereOptimizer::ConditionDescription> MergeTreeWhereOptimizer::parseCondition(
    const ASTPtr & condition) const
{
    const auto * function = condition->as<ASTFunction>();
    if (!function)
        return std::nullopt;

    static const std::unordered_map<String, ConditionDescription::Type> compare_funcs = {
        {"equals", ConditionDescription::Type::EQUAL},
        {"notEquals", ConditionDescription::Type::NOT_EQUAL},
        {"less", ConditionDescription::Type::LESS_OR_EQUAL},
        {"greater", ConditionDescription::Type::GREATER_OR_EQUAL},
        {"greaterOrEquals", ConditionDescription::Type::GREATER_OR_EQUAL},
        {"lessOrEquals", ConditionDescription::Type::LESS_OR_EQUAL},
    };
    if (!compare_funcs.contains(function->name))
        return std::nullopt;

    ConditionDescription::Type compare_type = compare_funcs.at(function->name);

    auto * left_arg = function->arguments->children.front().get();
    auto * right_arg = function->arguments->children.back().get();

    /// try to ensure left_arg points to ASTIdentifier
    if (!left_arg->as<ASTIdentifier>() && right_arg->as<ASTIdentifier>()) {
        std::swap(left_arg, right_arg);
        static const std::unordered_map<ConditionDescription::Type, ConditionDescription::Type> compare_funcs_swap = {
            {ConditionDescription::Type::EQUAL, ConditionDescription::Type::EQUAL},
            {ConditionDescription::Type::NOT_EQUAL, ConditionDescription::Type::NOT_EQUAL},
            {ConditionDescription::Type::LESS_OR_EQUAL, ConditionDescription::Type::GREATER_OR_EQUAL},
            {ConditionDescription::Type::GREATER_OR_EQUAL, ConditionDescription::Type::LESS_OR_EQUAL},
        };
        compare_type = compare_funcs_swap.at(compare_type);
    }

    const auto * ident = left_arg->as<ASTIdentifier>();
    if (ident)
    {
        /// condition may be "good" if only right_arg is a constant
        if (const auto * literal = right_arg->as<ASTLiteral>())
        {
            const auto & field = literal->value;
            LOG_INFO(&Poco::Logger::get("TEST>>>"), "DESC column = {} type={} field={}", ident->getColumnName(), compare_type, field);
            return ConditionDescription{ident->getColumnName(), compare_type, field};
        }
    }

    return std::nullopt;
}

double MergeTreeWhereOptimizer::scoreSelectivity(const std::optional<MergeTreeWhereOptimizer::ConditionDescription> & condition_description) const
{
    if (!condition_description)
        return 1;

    switch  (condition_description->type) {
    case ConditionDescription::Type::EQUAL:
        return stats->getDistributionStatistics()->estimateProbability(
            condition_description->identifier,
            condition_description->constant,
            condition_description->constant).value_or(1);
    case ConditionDescription::Type::NOT_EQUAL:
        return 1 - stats->getDistributionStatistics()->estimateProbability(
            condition_description->identifier,
            condition_description->constant,
            condition_description->constant).value_or(0);
    case ConditionDescription::Type::LESS_OR_EQUAL:
        return stats->getDistributionStatistics()->estimateProbability(
            condition_description->identifier,
            {},
            condition_description->constant).value_or(1);
    case ConditionDescription::Type::GREATER_OR_EQUAL:
        return stats->getDistributionStatistics()->estimateProbability(
            condition_description->identifier,
            condition_description->constant,
            {}).value_or(1); 
    }
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

bool MergeTreeWhereOptimizer::tryAnalyzeTuple(Conditions & res, const ASTFunction * func, bool is_final, bool recurse_tuple) const
{
    // TODO: support not only equals
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
        analyzeImpl(res, func_node, is_final, recurse_tuple);
    }

    return true;
}

void MergeTreeWhereOptimizer::analyzeImpl(Conditions & res, const ASTPtr & node, bool is_final, bool recurse_tuple) const
{
    const auto * func = node->as<ASTFunction>();

    if (func && func->name == "and")
    {
        for (const auto & elem : func->arguments->children)
            analyzeImpl(res, elem, is_final, recurse_tuple);
    }
    else if (tryAnalyzeTuple(res, func, is_final, recurse_tuple))
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
        
        if (use_new_scoring) {
            cond.description = parseCondition(node);
            cond.selectivity = scoreSelectivity(cond.description);
        }

        if (cond.viable)
        {
            if (!use_new_scoring)
            {
                cond.good = isConditionGood(node);
                cond.selectivity = 1;
                cond.rank = 0;
            }
            else
            {
                cond.good = isConditionGoodNew(node);
                // See page 5 in https://dsf.berkeley.edu/jmh/miscpapers/sigmod93.pdf
                // Cost per tuple = mean size of tuple = columns_size / count.
                cond.rank = (1 - cond.selectivity) * RANK_CORRECTION / cond.columns_size;
            }
        }

        res.emplace_back(std::move(cond));
    }
}

/// Transform conjunctions chain in WHERE expression to Conditions list.
MergeTreeWhereOptimizer::Conditions MergeTreeWhereOptimizer::analyze(const ASTPtr & expression, bool is_final, bool recurse_tuple) const
{
    Conditions res;
    analyzeImpl(res, expression, is_final, recurse_tuple);
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

void MergeTreeWhereOptimizer::optimize(ASTSelectQuery & select) const {
    if (!select.where() || select.prewhere())
        return;
    if (use_new_scoring) {
        optimizeByRanks(select);
    } else {
        optimizeBySize(select);
    }
}

bool MergeTreeWhereOptimizer::ColumnWithRank::operator<(const ColumnWithRank & other) const {
    return rank < other.rank;
}

bool MergeTreeWhereOptimizer::ColumnWithRank::operator==(const ColumnWithRank & other) const {
    return rank == other.rank;
}

std::vector<MergeTreeWhereOptimizer::ColumnWithRank> MergeTreeWhereOptimizer::getSimpleColumns(
    const std::unordered_map<std::string, Conditions> & column_to_simple_conditions) const {
    std::vector<MergeTreeWhereOptimizer::ColumnWithRank> rank_to_column;
    for (const auto & [column, conditions] : column_to_simple_conditions) {
        double total_selectivity = 1;
        // Conditions are connected using AND
        // TODO: consider </> and =/!= separately
        LOG_INFO(&Poco::Logger::get("TEST>>>"), "column = {}", column);
        for (const auto & condition : conditions) {
            total_selectivity *= condition.selectivity;
            LOG_INFO(&Poco::Logger::get("TEST>>> selectivity"), "sel = {} cl = {}", condition.selectivity, condition.description->identifier);
        }

        // See page 5 in https://dsf.berkeley.edu/jmh/miscpapers/sigmod93.pdf
        // rank = (1 - selectivity) / cost per tuple;
        // Cost per tuple = mean size of tuple = columns_size / count.
        rank_to_column.emplace_back(
            -(1 - total_selectivity) * RANK_CORRECTION / getIdentifiersColumnSize({column}),
            total_selectivity,
            column);
    }
    std::sort(std::begin(rank_to_column), std::end(rank_to_column));
    return rank_to_column;
}

void MergeTreeWhereOptimizer::optimizeByRanks(ASTSelectQuery & select) const
{
    // TODO: better estimation for a < 10 OR b > 10 OR b < -100..
    // TODO: basic support for tuples (a, b, c) < (d, e, f) => estimate a <= d and add to prewhere
    Conditions where_conditions = analyze(select.where(), select.final(), false);
    std::unordered_map<std::string, Conditions> column_to_simple_conditions;
    Conditions complex_conditions;
    for (const auto & condition : where_conditions) {
        if (condition.viable && condition.description) {
            column_to_simple_conditions[condition.description->identifier].push_back(condition);
        } else {
            complex_conditions.push_back(condition);
        }
    }
    Conditions prewhere_conditions;
    std::unordered_set<std::string> columns_in_prewhere;

    UInt64 total_size_of_moved_conditions = 0;
    UInt64 total_number_of_moved_columns = 0;

    // First we'll move simple expressions (one column) while estimated amount of data get's lower. (only for conditions with selectivity estimates)
    std::vector<ColumnWithRank> rank_to_column = getSimpleColumns(column_to_simple_conditions);

    // Total data read = sum of prewhere columns + product of selectivities * sum of other columns.
    // We are trying to minimize total data read from disk.
    size_t prewhere_columns_size = 0;
    size_t where_columns_size = total_size_of_queried_columns;
    double prewhere_selectivity = 1;
    LOG_INFO(&Poco::Logger::get("TEST"), "START totalsz = {} rtc ={}", where_columns_size, rank_to_column.size());
    for (const auto & [rank, selectivity, column] : rank_to_column) {
        LOG_INFO(&Poco::Logger::get("TEST???"), "rnk={} sel={} clm={} sz={}", rank, selectivity, column, getIdentifiersColumnSize({column}));
    }
    for (const auto & [rank, selectivity, column] : rank_to_column) {
        const size_t column_size = getIdentifiersColumnSize({column});
        const double current_loss = prewhere_columns_size + prewhere_selectivity * where_columns_size;
        if (where_columns_size < column_size) {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "It's a bug: where_columns_size = {} < column_size = {}",
                where_columns_size, column_size);
        }
        // We think that columns are independent.
        const double predicted_loss = (prewhere_columns_size + column_size) + prewhere_selectivity * selectivity * (where_columns_size - column_size);
        
        LOG_INFO(&Poco::Logger::get("TEST"), "rnk={} sel={} clm={}", rank, selectivity, column);
        LOG_INFO(&Poco::Logger::get("TEST"), "currloss = {} predloss ={}", current_loss, predicted_loss);

        // don't stop if difference is small
        if (current_loss + EPS < predicted_loss) {
            break;   
        }

        prewhere_columns_size += column_size;
        where_columns_size -= column_size;
        prewhere_selectivity *= selectivity;

        columns_in_prewhere.insert(column);

        total_size_of_moved_conditions += column_size;
        total_number_of_moved_columns += 1;
    }

    // Let's collect conditions that can be calculated in prewhere using columns_in_prewhere.
    for (auto it = where_conditions.begin(); it != where_conditions.end();)
    {
        if (it->viable &&
            std::all_of(
                std::begin(it->identifiers),
                std::end(it->identifiers),
                [&columns_in_prewhere] (const auto & ident) {
                    return columns_in_prewhere.contains(ident);
                })) {
            prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, it++);
        } else {
            ++it;
        }
    }

    // Now let's move other columns from smaller to bigger while totals are less than threasholds.
    // Just repeat the old algorithm, but order conditions by ranks instead of only sizes.

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

    /// TODO: use columns in expr
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

void MergeTreeWhereOptimizer::optimizeBySize(ASTSelectQuery & select) const
{
    Conditions where_conditions = analyze(select.where(), select.final(), true);
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

    /// TODO: use columns in expr
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


bool MergeTreeWhereOptimizer::isSortingKey(const String & column_name) const
{
    return sorting_key_names.count(column_name);
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
        if (table_columns.count(identifier) == 0)
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
            array_joined_names.count(Nested::extractTableName(*opt_name)) ||
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
