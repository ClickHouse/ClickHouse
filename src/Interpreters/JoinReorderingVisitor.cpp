#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Interpreters/JoinReorderingVisitor.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/ExtractAsterisksVisitor.h>
#include <Interpreters/findGoodReordering.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/logger_useful.h>
#include <Storages/StorageJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool isSimpleEquiJoinExpression(const ASTPtr & ast,
                                       size_t & out_left_table_pos,
                                       size_t & out_right_table_pos,
                                       const TablesWithColumns & tables,
                                       const Aliases & aliases)
{
    if (const auto * func = ast->as<ASTFunction>(); func && func->name == NameEquals::name)
    {
        if (!func->arguments || func->arguments->children.size() != 2)
            return false;

        /// Check if the identifiers are from different joined tables.
        auto left_table_pos = IdentifierSemantic::getIdentsMembership(func->arguments->children[0], tables, aliases);
        auto right_table_pos = IdentifierSemantic::getIdentsMembership(func->arguments->children[1], tables, aliases);

        if (left_table_pos && right_table_pos && *left_table_pos != *right_table_pos)
        {
            out_left_table_pos = *left_table_pos;
            out_right_table_pos = *right_table_pos;
            return true;
        }
    }
    return false;
}

static bool isSimpleEquiJoinExpression(const ASTPtr & ast, const TablesWithColumns & tables, const Aliases & aliases)
{
    size_t left, right;
    return isSimpleEquiJoinExpression(ast, left, right, tables, aliases);
}

static bool isFunctionOfSingleTable(const ASTPtr & ast, size_t & table_pos, const TablesWithColumns & tables, const Aliases & aliases)
{
    if (const auto * func = ast->as<ASTFunction>(); func)
    {
        /// getIdentsMembership will not return a value if multiple tables are referred to in call to func.
        auto maybe_table_pos = IdentifierSemantic::getIdentsMembership(ast, tables, aliases);
        if (maybe_table_pos)
            table_pos = *maybe_table_pos;
        return maybe_table_pos.has_value();
    }
    return false;
}

static void removeChild(ASTPtr & parent, const ASTPtr & child)
{
    if (child)
    {
        const auto * child_it = std::find_if(parent->children.begin(), parent->children.end(), [&](const auto & p)
        {
           return p.get() == child.get();
        });
        if (child_it != parent->children.end())
            parent->children.erase(child_it);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Couldn't find child to remove.");
    }
}

static bool bfsAnd(const ASTPtr & root, std::function<bool(ASTFunction &, const ASTPtr &)> visitor)
{
    ASTs parents = { root };

    for (size_t idx = 0; idx < parents.size();)
    {
        ASTPtr cur_parent = parents.at(idx);

        if (auto * function = cur_parent->as<ASTFunction>(); function && function->name == "and")
        {
            parents.erase(parents.begin() + idx);

            for (auto & child : function->arguments->children)
            {
                if (visitor(*function, child))
                    return true;
                parents.emplace_back(child);
            }
            continue;
        }
        ++idx;
    }
    return false;
}

/// Removes node from the WHERE clause. Every parent of node, up to and including the WHERE clause, must be an AND function.
/// `AND(AND(a, node), b)` will be rewritten to `AND(a, b)`.
/// `AND(node, a)` will be rewritten to `a`. Note: `AND(node, a)` has type boolean (UInt8, that is 0 or 1), while `a` might not have.
/// This means this rewrite is valid for the WHERE clause, but generally not valid elsewhere,
/// because the WHERE clause is always interpreted as a boolean.
/// If either node or the AND function to be removed has an alias, it will not be removed.
static bool tryRemoveFromWhere(ASTSelectQuery & select, const ASTPtr & node)
{
    if (!node || !node->tryGetAlias().empty())
        return false;

    ASTFunction * rewritten_and = nullptr;
    /// First pass, convert AND(node, a) to AND(a)
    bfsAnd(select.refWhere(), [&](ASTFunction & parent, const ASTPtr & child)
    {
        if (child.get() == node.get())
        {
            if (!parent.tryGetAlias().empty())
                return true;
            removeChild(parent.arguments, node);
            rewritten_and = &parent;
            return true;
        }
        return false; //continue traversal
    });

    if (!rewritten_and)
        return false;

    /// Second pass, convert AND(a) to a
    bool found = bfsAnd(select.refWhere(), [&](ASTFunction & parent, const ASTPtr & child)
    {
        if (child.get() != rewritten_and)
            return false; //continue traversal
        if (const auto * child_and = child->as<ASTFunction>(); child_and && child_and->name == "and" && child_and->arguments->children.size() == 1)
        {
            auto * child_it = std::find_if(parent.children.begin(), parent.children.end(), [&](const auto & p)
            {
                return p.get() == child_and;
            });
            if (child_it != parent.children.end())
                *child_it = child_and->arguments->children.at(0);
        }
        return true;
    });
    if (!found)
    {
        /// If the AND(a) is not a child of the WHERE clause, it should *be* the WHERE clause. Rewrite to `WHERE a`.
        if (const auto * child = select.refWhere()->as<ASTFunction>(); child && child->name == "and")
        {
            if (child->arguments->children.size() == 1)
            {
                select.refWhere() = child->arguments->children.at(0);
            }
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Rewrote an AND clause, but then lost track of it. This is a bug.");
    }
    return true;
}

/// Extract expressions that are the same on all sides of an OR clause.
/// For an expression like `((a AND b) OR (a AND c) OR (a AND b))`, it extracts `a`.
static ASTs findRepeatedClausesInOr(const ASTPtr & root)
{
    const auto * function = root->as<ASTFunction>();
    if (!function || function->name != "or")
        return {};

    //For each part of the disjunction, build a set of all expressions per part (while splitting AND clauses into individual expressions)
    const auto parts = splitDisjunctionsAst(root);
    if (parts.size() < 2)
        return {};
    std::vector<std::unordered_set<UInt128>> seen(parts.size());
    for (size_t i = 0; i < parts.size(); ++i)
    {
        for (const auto & node : splitConjunctionsAst(parts[i]))
            seen[i].insert(UInt128(node->getTreeHash()));
    }

    ASTs results;
    //now for every expression in the first part, check that it also exists in all other parts
    for (const auto & node : splitConjunctionsAst(parts[0]))
    {
        UInt128 hash = node->getTreeHash();
        bool all_contains = true;
        for (size_t i = 1; i < parts.size(); ++i)
        {
            if (!seen[i].contains(hash))
            {
                all_contains = false;
                break;
            }
        }
        if (all_contains)
            results.push_back(node);
    }
    return results;
}

/// Return NxN matrix of selectivities, an N vector of self-selectivities, and NxN matrix of predicates, between each pair of table.
static std::tuple<Vector2D<Float64>, std::vector<Float64>, Vector2D<ASTs>> computeSelectivities(
    ASTSelectQuery & select,
    const std::vector<Float64> row_counts,
    const TablesWithColumns & tables,
    const Aliases & aliases)
{
    const size_t N = tables.size();
    Vector2D<Float64> selectivities(N, N); /// Selectivity between each pair of table
    selectivities.fill(1.0);
    std::vector<Float64> self_selectivities(N); /// The "self-selectivity", i.e. how much a table is filtered by non-join predicates.
    for (size_t i = 0; i < N; ++i)
        self_selectivities[i] = 1.0;
    Vector2D<ASTs> predicates(N, N); /// Predicates between each pair of table

    auto collectPredicates = [&] (const ASTPtr & expression, bool is_where)
    {
        for (const auto & node : splitConjunctionsAst(expression))
        {
            size_t table_pos_a, table_pos_b;
            if (isSimpleEquiJoinExpression(node, table_pos_a, table_pos_b, tables, aliases))
            {
                Float64 selectivity = 1.0 / std::min(row_counts[table_pos_a], row_counts[table_pos_b]);
                selectivities(table_pos_a, table_pos_b) *= selectivity;
                selectivities(table_pos_b, table_pos_a) *= selectivity;
                predicates(table_pos_a, table_pos_b).push_back(node);
                predicates(table_pos_b, table_pos_a).push_back(node);
                if (is_where)
                    tryRemoveFromWhere(select, node);
            }
            else if (isFunctionOfSingleTable(node, table_pos_a, tables, aliases))
            {
                if (node->as<ASTFunction>()->name == NameEquals::name)
                {
                    /// Found clause like t1.a = 10.
                    /// Could select 1 row (e.g. sel = 1/N) or a portion of rows (e.g. sel = 0.1). Don't know which, so use an average.
                    Float64 sel = ((1.0 / row_counts[table_pos_a]) + 0.1) * 0.5;
                    self_selectivities[table_pos_a] *= sel;
                }
                else
                {
                    /// Found non-equality clause, like t1.a > 10.
                    self_selectivities[table_pos_a] *= 0.1;
                }
            }
            else
            {
                for (const auto & repeated_node : findRepeatedClausesInOr(node))
                {
                    if (isSimpleEquiJoinExpression(repeated_node, table_pos_a, table_pos_b, tables, aliases))
                    {
                        Float64 selectivity = 1.0 / std::min(row_counts[table_pos_a], row_counts[table_pos_b]);
                        selectivities(table_pos_a, table_pos_b) *= selectivity;
                        selectivities(table_pos_b, table_pos_a) *= selectivity;
                        predicates(table_pos_a, table_pos_b).push_back(node);
                        predicates(table_pos_b, table_pos_a).push_back(node);
                    }
                }
            }
            /// FIXME: We could also accept OR(equi_join1, equi_join2), where both equi_join1 and equi_join2 refer to the same two tables.
            /// Just need to move the OR function as a unit to an ON clause. Currently disallow these in canReorderJoin().
            /// FIXME: If we have joins (t1.id = t2.id) AND (t2.id = t3.id), it implies that we can also join on (t1.id = t3.id).
            /// Using this can allow faster plans, but if we don't use the new predicate, it will be slower to compute it than to not compute it.
            /// So if we expand the list of predicates, we'll want to later remove predicates that are superfluous because they are implied by
            /// predicates we already used. This can get complicated.
        }
    };

    const auto * ast_tables = select.tables()->as<ASTTablesInSelectQuery>();
    for (size_t i = 1; i < ast_tables->children.size(); ++i)
    {
        auto * table = ast_tables->children[i]->as<ASTTablesInSelectQueryElement>();
        if (!table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected child of ASTTablesInSelectQuery to be ASTTablesInSelectQueryElement");
        if (!table->table_join || !table->table_join->as<ASTTableJoin>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table_join property to be set on all tables except the first.");
        auto * table_join = table->table_join->as<ASTTableJoin>();
        if (table_join->on_expression)
        {
            collectPredicates(table_join->on_expression, false);
        }
    }
    collectPredicates(select.where(), true);

    return { selectivities, self_selectivities, predicates };
}

static bool hasNoIdentifierAliases(const ASTPtr & node, const Aliases & aliases)
{
    /// CrossToInnerVisitor refuses to rewrite queries where there's a function with certain aliases. Let's do the same here.
    /// See isAllowedToRewriteCrossJoin() in CrossToInnerJoinVisitor.cpp
    /// FIXME: It's probably possible to allow aliases in WHERE without problems, as long we
    /// remove the alias when cloning the ASTPtr to ON, and then don't remove the ASTPtr from the WHERE clause.
    if (node->as<ASTFunction>())
    {
        auto idents = IdentifiersCollector::collect(node);
        for (const auto * ident : idents)
        {
            if (ident->isShort() && aliases.contains(ident->shortName()))
                return false;
        }
        return true;
    }
    return node->as<ASTIdentifier>() || node->as<ASTLiteral>();
}

static bool canReorderJoins(const ASTSelectQuery & select, const JoinReorderingMatcher::Data & data)
{
    if (!data.settings.reorder_joins)
        return false;

    /// cross_to_inner_join_rewrite = 2 means we should throw an error if we can't convert cross join to inner.
    /// But findGoodReordering might determine that a plan with a cross join is faster in some cases, even if
    /// a plan without cross join is possible. That means we have to turn off join reordering if cross_to_inner_join_rewrite=2,
    /// otherwise we would have to start throwing on queries that worked before we introduced join reordering.
    if (data.settings.cross_to_inner_join_rewrite >= 2)
        return false;

    if (!select.tables())
        return false;

    const auto * query_tables = select.tables()->as<ASTTablesInSelectQuery>();
    if (!query_tables || query_tables->children.size() <= 1)
        return false;

    auto * first_table = query_tables->children[0]->as<ASTTablesInSelectQueryElement>();
    if (!first_table || first_table->array_join)
        return false;

    for (const auto & storage : data.storages)
        if (storage && storage->as<StorageJoin>())
            return false;

    std::optional<JoinLocality> first_locality;
    for (size_t i = 1; i < query_tables->children.size(); ++i)
    {
        auto * joined_table = query_tables->children[i]->as<ASTTablesInSelectQueryElement>();
        if (!joined_table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected child of ASTTablesInSelectQuery to be ASTTablesInSelectQueryElement");

        if (joined_table->array_join)
            return false;

        const auto * table_join = joined_table->table_join->as<ASTTableJoin>();
        /// "JOIN .. USING id" can be rewritten to an ON expression, and then reordered. Need to resolve names first. FIXME: implement this.
        if (table_join->using_expression_list)
            return false;

        /// If we only have cross, inner and comma joins, they can be freely reordered.
        /// FIXME: In presence of other join types, it can be possible to reorder subsets of the tables.
        /// For now, we give up when other join types are present.
        auto join_kind = table_join->kind;
        if (join_kind != JoinKind::Cross && join_kind != JoinKind::Inner && join_kind != JoinKind::Comma)
            return false;

        /// For JoinStrictness::Any, it may be possible to reorder subsets of the tables (i.e. separately to the left and right of the rhs any-joined table).
        /// For now, require that all joins have ALL strictness, and give up otherwise.
        bool strictness_ok = table_join->strictness == JoinStrictness::All ||
                             (table_join->strictness == JoinStrictness::Unspecified && data.settings.join_default_strictness == JoinStrictness::All);
        if (!strictness_ok)
            return false;

        /// For distributed queries, require the join locality to be the same for all joins,
        /// otherwise reordering needs to take locality into account.
        if (!first_locality)
            first_locality = table_join->locality;
        else if (*first_locality != table_join->locality)
            return false;

        if (table_join->on_expression)
        {
            for (const auto & node : splitConjunctionsAst(table_join->on_expression))
            {
                /// "JOIN .. ON expression" can have a complex expression as ON clause, including OR expressions.
                /// Example 1) JOIN .. ON (t1.a = t2.b OR t1.c = t2.d)
                /// Example 2) JOIN .. ON (t1.a = t2.b AND t2.c > 6)
                /// OR can be handled by treating it as a unit, when both sides of OR refer to the same two tables. Case 2 can also be handled.
                /// It shouldn't be too hard to cut up and stich together such expressions, but we give up for now.
                /// FIXME: Handle more complex ON clauses.
                /// For now, accept only equi-join expressions (t1.a = t2.b), or conjunctions of these.
                if (!isSimpleEquiJoinExpression(node, data.tables, data.aliases))
                    return false;
                if (!hasNoIdentifierAliases(node, data.aliases))
                    return false;
            }
        }
    }

    for (const auto & node : splitConjunctionsAst(select.where()))
    {
        if (!hasNoIdentifierAliases(node, data.aliases))
            return false;
    }
    return true;
}

static void expandAsterisks(ASTSelectQuery & select, TablesWithColumns & tables)
{
    /// Need to expand asterisks in the query, otherwise a query like "SELECT * FROM t1, t2 WHERE t1.id = t2.id"
    /// can produce different column orderings, depending on how we order t1 and t2. (i.e. t1.*, t2.* vs t2.*, t1.*)
    /// Also, we need to expand asterisks to know which columns are selected, which is used in cost estimates.
    ExtractAsterisksVisitor::Data asterisks_data(tables);
    ExtractAsterisksVisitor(asterisks_data).visit(select.select());
    if (asterisks_data.new_select_expression_list)
        select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(asterisks_data.new_select_expression_list));
}

static void storeColumnNamesBeforeReordering(ASTSelectQuery & select, JoinReorderingMatcher::Data & data)
{
    if (data.tables.size() != 2)
        return;
    for (const auto & ast : select.select()->children)
    {
        const auto * ident = ast->as<ASTIdentifier>();
        if (!ident)
        {
            data.pre_reorder_names.emplace_back(ast->getAliasOrColumnName());
            continue;
        }

        /// Qualify names for right table, if they clash with names in the left table
        const auto pos = IdentifierSemantic::getIdentMembership(*ident, data.tables);
        if (!pos.has_value())
            data.pre_reorder_names.emplace_back(ast->getAliasOrColumnName());
        else if ((*pos == 0 || !data.tables[0].hasColumn(ident->shortName())) && isValidIdentifierBegin(ident->shortName().at(0)))
        {
            String shortname_or_alias = ident->tryGetAlias().empty() ? ident->shortName() : ident->tryGetAlias();
            data.pre_reorder_names.emplace_back(shortname_or_alias);
        }
        else
        {
            String longname_or_alias = ident->tryGetAlias().empty() ? data.tables[*pos].table.getQualifiedNamePrefix() + ident->shortName() : ident->tryGetAlias();
            data.pre_reorder_names.emplace_back(longname_or_alias);
        }
    }
}

/// Gets row counts where possible. Missing row counts are replaced with the average of the row counts we have.
/// Need row counts for at least 2 tables for this to be useful, so returns false if we don't have enough row counts to work with.
/// Also count the number of used columns from each table.
static bool getCounts(ASTSelectQuery & select, const ASTPtr & ast, JoinReorderingMatcher::Data & data, std::vector<Float64> & row_counts, std::vector<Float64> & col_counts)
{
    if (data.storages.size() != data.tables.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of storages doesn't match size of tables.");

    std::vector<Float64> sizes(data.tables.size());
    for (size_t i = 0; i < data.tables.size(); ++i)
    {
        if (data.storages[i])
        {
            auto rowCount = data.storages[i]->totalRows(data.settings);
            if (rowCount)
                sizes[i] = *rowCount;
        }
    }
    /// We need a fallback for subqueries, or where underlying storage doesn't return a value for totalRows().
    /// Can just pick average of counts from other tables.
    Float64 sum = 0;
    size_t non_empty = 0;
    for (Float64 size : sizes)
    {
        if (size != 0.0)
        {
            sum += size;
            non_empty += 1;
        }
    }
    if (non_empty < 2)
    {
        /// If we only have 0 or 1 tables with row counts, we have no information to go on to do a reordering.
        return false;
    }
    Float64 average = sum/non_empty;
    for (Float64 & size : sizes)
        if (size == 0.0)
            size = average;
    row_counts = sizes;

    expandAsterisks(select, data.tables);
    storeColumnNamesBeforeReordering(select, data);

    /// Collect number of used columns for each table in the query
    std::vector<Float64> cols(data.tables.size());
    std::vector<IdentifierNameSet> ident_names(data.tables.size());
    auto identifiers = IdentifiersCollector::collect(ast);
    for (auto & identifier : identifiers)
    {
        const auto pos = IdentifierSemantic::getIdentMembership(*identifier, data.tables);
        if (pos.has_value())
            identifier->collectIdentifierNames(ident_names[*pos]);
    }
    for (size_t i = 0; i < cols.size(); ++i)
        cols[i] = ident_names[i].size();
    col_counts = cols;
    return true;
}

static ASTPtr makeConjunction(const ASTs & nodes)
{
    if (nodes.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a non-zero number of nodes.");

    if (nodes.size() == 1)
        return nodes[0]->clone();

    ASTs arguments;
    arguments.reserve(nodes.size());
    for (const auto & ast : nodes)
        arguments.emplace_back(ast->clone());

    return makeASTFunction(NameAnd::name, std::move(arguments));
}

static void reorderJoins(const std::vector<size_t> & ordering, const Vector2D<ASTs> & predicates, ASTSelectQuery & select, TablesWithColumns & tables)
{
    if (!select.tables())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No tables to reorder.");

    auto * query_tables = select.tables()->as<ASTTablesInSelectQuery>();
    if (!query_tables)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected tables to be of type ASTTablesInSelectQuery");
    if (query_tables->children.size() != ordering.size() || ordering.size() <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of tables to reorder. {} and {}", query_tables->children.size(), ordering.size());

    ASTs new_children;
    TablesWithColumns new_tables;
    for (size_t new_idx = 0; new_idx < ordering.size(); ++new_idx) // NOLINT
    {
        size_t old_idx = ordering[new_idx];
        new_children.push_back(query_tables->children[old_idx]);
        new_tables.push_back(tables[old_idx]);
    }
    query_tables->children = new_children;
    tables = new_tables;

    /// Determine the join locality to use. canReorderJoins() has already ensured that all joins have the same locality.
    JoinLocality locality = JoinLocality::Unspecified;
    for (auto & child : query_tables->children)
    {
        auto * table = child->as<ASTTablesInSelectQueryElement>();
        if (!table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected child of ASTTablesInSelectQuery to be ASTTablesInSelectQueryElement");
        if (table->table_join)
        {
            locality = table->table_join->as<ASTTableJoin>()->locality;
            break;
        }
    }

    /// Rules for query_tables->children (see ASTTablesInSelectQueryElement)
    ///   First element either has el.table_expression or el.array_join
    ///   The next elements either have both (table_join && table_expression) or (array_join)
    /// Here, canReorderJoins() guarantees that we don't have array_join.
    /// This implies that every element in children currently already has table_expression set.
    /// To follow these rules, we must make sure the 1st element doesn't have table_join, while every following element does.
    auto * first_table = query_tables->children[0]->as<ASTTablesInSelectQueryElement>();
    removeChild(query_tables->children[0], first_table->table_join);
    first_table->table_join = nullptr;

    /// Reconstruct predicates:
    /// Walk through the tables for n from 1 to N, and collect predicates from all tables [0..n-1] that relates to n,
    /// and add these to the ON expression for n. If there are no predicates, make it a CROSS join, otherwise an INNER join.
    for (size_t n = 1; n < query_tables->children.size(); ++n)
    {
        ASTs relevant_predicates;
        for (size_t i = 0; i < n; ++i)
        {
            size_t old_idx_n = ordering[n];
            size_t old_idx_i = ordering[i];
            /// This will in the end have considered all pairs (i,n), so we can't miss any predicates.
            for (ASTPtr & predicate : predicates(old_idx_i, old_idx_n))
                relevant_predicates.push_back(predicate);
        }
        auto join_ast = std::make_shared<ASTTableJoin>();
        join_ast->locality = locality;
        join_ast->strictness = JoinStrictness::All;
        if (relevant_predicates.empty())
        {
            join_ast->kind = JoinKind::Cross;
        }
        else
        {
            join_ast->kind = JoinKind::Inner;
            join_ast->on_expression = makeConjunction(relevant_predicates);
            join_ast->children.emplace_back(join_ast->on_expression);
        }
        auto * table = query_tables->children[n]->as<ASTTablesInSelectQueryElement>();
        if (!table)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected child of ASTTablesInSelectQuery to be ASTTablesInSelectQueryElement");
        removeChild(query_tables->children[n], table->table_join);
        table->table_join = join_ast;
        table->children.emplace_back(join_ast);
    }
}

bool JoinReorderingMatcher::needChildVisit(ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>();
}

void JoinReorderingMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<ASTSelectQuery>())
        visit(*t, ast, data);
}

void JoinReorderingMatcher::visit(ASTSelectQuery & select, ASTPtr & ast, Data & data)
{
    std::vector<Float64> row_counts, col_counts;
    if (canReorderJoins(select, data) && getCounts(select, ast, data, row_counts, col_counts))
    {
        auto [selectivities, self_selectivities, predicates] = computeSelectivities(select, row_counts, data.tables, data.aliases);
        auto good_reordering = findGoodReordering(row_counts, col_counts, selectivities, self_selectivities);
        reorderJoins(good_reordering, predicates, select, data.tables);
        /// When there's 2 joined tables, and columns have the same name,
        /// the execution machinery will use a short name for columns from left table,
        /// and a qualified name for columns from the right (if there's name clashes).
        /// In this case, if we reordered, we need to keep track of what the names would have been
        /// if there was no reordering, and rename the columns back later in the execution.
        bool reordered_two = data.tables.size() == 2 && good_reordering[0] == 1;
        if (!reordered_two)
            data.pre_reorder_names.clear();
    }
    else
    {
        /// Fall back to CrossToInner
        CrossToInnerJoinVisitor::Data cross_to_inner{data.tables, data.aliases, data.current_database};
        cross_to_inner.cross_to_inner_join_rewrite = static_cast<UInt8>(std::min<UInt64>(data.settings.cross_to_inner_join_rewrite, 2));
        CrossToInnerJoinVisitor(cross_to_inner).visit(ast);
    }
}

}
