#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Core/Settings.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/OptimizeIfWithConstantConditionVisitor.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <DataTypes/NestedUtils.h>

#include <Core/NamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>

#include <functional>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_NESTED_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

NameSet removeDuplicateColumns(NamesAndTypesList & columns)
{
    NameSet names;
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (names.emplace(it->name).second)
            ++it;
        else
            columns.erase(it++);
    }
    return names;
}

namespace
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


/// Add columns from storage to source_columns list.
void collectSourceColumns(const ASTSelectQuery * select_query, StoragePtr storage, NamesAndTypesList & source_columns)
{
    if (storage)
    {
        auto physical_columns = storage->getColumns().getAllPhysical();
        if (source_columns.empty())
            source_columns.swap(physical_columns);
        else
            source_columns.insert(source_columns.end(), physical_columns.begin(), physical_columns.end());

        if (select_query)
        {
            const auto & storage_aliases = storage->getColumns().getAliases();
            source_columns.insert(source_columns.end(), storage_aliases.begin(), storage_aliases.end());
        }
    }
}

/// Translate qualified names such as db.table.column, table.column, table_alias.column to names' normal form.
/// Expand asterisks and qualified asterisks with column names.
/// There would be columns in normal form & column aliases after translation. Column & column alias would be normalized in QueryNormalizer.
void translateQualifiedNames(ASTPtr & query, const ASTSelectQuery & select_query, const Context & context,
                             const Names & source_columns_list, const NameSet & source_columns_set,
                             const JoinedColumnsList & columns_from_joined_table)
{
    std::vector<TableWithColumnNames> tables_with_columns = getDatabaseAndTablesWithColumnNames(select_query, context);

    if (tables_with_columns.empty())
    {
        Names all_columns_name = source_columns_list;

        /// TODO: asterisk_left_columns_only probably does not work in some cases
        if (!context.getSettingsRef().asterisk_left_columns_only)
        {
            for (auto & column : columns_from_joined_table)
                all_columns_name.emplace_back(column.name_and_type.name);
        }

        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, std::move(all_columns_name));
    }

    LogAST log;
    TranslateQualifiedNamesVisitor::Data visitor_data(source_columns_set, tables_with_columns);
    TranslateQualifiedNamesVisitor visitor(visitor_data, log.stream());
    visitor.visit(query);
}

bool hasArrayJoin(const ASTPtr & ast)
{
    if (const ASTFunction * function = ast->as<ASTFunction>())
        if (function->name == "arrayJoin")
            return true;

    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && hasArrayJoin(child))
            return true;

    return false;
}

/// Keep number of columns for 'GLOBAL IN (SELECT 1 AS a, a)'
void renameDuplicatedColumns(const ASTSelectQuery * select_query)
{
    ASTs & elements = select_query->select()->children;

    std::set<String> all_column_names;
    std::set<String> assigned_column_names;

    for (auto & expr : elements)
        all_column_names.insert(expr->getAliasOrColumnName());

    for (auto & expr : elements)
    {
        auto name = expr->getAliasOrColumnName();

        if (!assigned_column_names.insert(name).second)
        {
            size_t i = 1;
            while (all_column_names.end() != all_column_names.find(name + "_" + toString(i)))
                ++i;

            name = name + "_" + toString(i);
            expr = expr->clone();   /// Cancels fuse of the same expressions in the tree.
            expr->setAlias(name);

            all_column_names.insert(name);
            assigned_column_names.insert(name);
        }
    }
}

/// Sometimes we have to calculate more columns in SELECT clause than will be returned from query.
/// This is the case when we have DISTINCT or arrayJoin: we require more columns in SELECT even if we need less columns in result.
/// Also we have to remove duplicates in case of GLOBAL subqueries. Their results are placed into tables so duplicates are inpossible.
void removeUnneededColumnsFromSelectClause(const ASTSelectQuery * select_query, const Names & required_result_columns, bool remove_dups)
{
    ASTs & elements = select_query->select()->children;

    std::map<String, size_t> required_columns_with_duplicate_count;

    if (!required_result_columns.empty())
    {
        /// Some columns may be queried multiple times, like SELECT x, y, y FROM table.
        for (const auto & name : required_result_columns)
        {
            if (remove_dups)
                required_columns_with_duplicate_count[name] = 1;
            else
                ++required_columns_with_duplicate_count[name];
        }
    }
    else if (remove_dups)
    {
        /// Even if we have no requirements there could be duplicates cause of asterisks. SELECT *, t.*
        for (const auto & elem : elements)
            required_columns_with_duplicate_count.emplace(elem->getAliasOrColumnName(), 1);
    }
    else
        return;

    ASTs new_elements;
    new_elements.reserve(elements.size());

    for (const auto & elem : elements)
    {
        String name = elem->getAliasOrColumnName();

        auto it = required_columns_with_duplicate_count.find(name);
        if (required_columns_with_duplicate_count.end() != it && it->second)
        {
            new_elements.push_back(elem);
            --it->second;
        }
        else if (select_query->distinct || hasArrayJoin(elem))
        {
            new_elements.push_back(elem);
        }
    }

    elements = std::move(new_elements);
}

/// Replacing scalar subqueries with constant values.
void executeScalarSubqueries(ASTPtr & query, const Context & context, size_t subquery_depth)
{
    LogAST log;
    ExecuteScalarSubqueriesVisitor::Data visitor_data{context, subquery_depth};
    ExecuteScalarSubqueriesVisitor(visitor_data, log.stream()).visit(query);
}

/** Calls to these functions in the GROUP BY statement would be
  * replaced by their immediate argument.
  */
const std::unordered_set<String> injective_function_names
{
        "negate",
        "bitNot",
        "reverse",
        "reverseUTF8",
        "toString",
        "toFixedString",
        "IPv4NumToString",
        "IPv4StringToNum",
        "hex",
        "unhex",
        "bitmaskToList",
        "bitmaskToArray",
        "tuple",
        "regionToName",
        "concatAssumeInjective",
};

const std::unordered_set<String> possibly_injective_function_names
{
        "dictGetString",
        "dictGetUInt8",
        "dictGetUInt16",
        "dictGetUInt32",
        "dictGetUInt64",
        "dictGetInt8",
        "dictGetInt16",
        "dictGetInt32",
        "dictGetInt64",
        "dictGetFloat32",
        "dictGetFloat64",
        "dictGetDate",
        "dictGetDateTime"
};

/// Eliminates injective function calls and constant expressions from group by statement.
void optimizeGroupBy(ASTSelectQuery * select_query, const NameSet & source_columns, const Context & context)
{
    if (!select_query->groupBy())
        return;

    const auto is_literal = [] (const ASTPtr & ast) -> bool
    {
        return ast->as<ASTLiteral>();
    };

    auto & group_exprs = select_query->groupBy()->children;

    /// removes expression at index idx by making it last one and calling .pop_back()
    const auto remove_expr_at_index = [&group_exprs] (const size_t idx)
    {
        if (idx < group_exprs.size() - 1)
            std::swap(group_exprs[idx], group_exprs.back());

        group_exprs.pop_back();
    };

    /// iterate over each GROUP BY expression, eliminate injective function calls and literals
    for (size_t i = 0; i < group_exprs.size();)
    {
        if (const auto * function = group_exprs[i]->as<ASTFunction>())
        {
            /// assert function is injective
            if (possibly_injective_function_names.count(function->name))
            {
                /// do not handle semantic errors here
                if (function->arguments->children.size() < 2)
                {
                    ++i;
                    continue;
                }

                const auto & dict_name = function->arguments->children[0]->as<ASTLiteral &>().value.safeGet<String>();
                const auto & dict_ptr = context.getExternalDictionaries().getDictionary(dict_name);
                const auto & attr_name = function->arguments->children[1]->as<ASTLiteral &>().value.safeGet<String>();

                if (!dict_ptr->isInjective(attr_name))
                {
                    ++i;
                    continue;
                }
            }
            else if (!injective_function_names.count(function->name))
            {
                ++i;
                continue;
            }

            /// copy shared pointer to args in order to ensure lifetime
            auto args_ast = function->arguments;

            /** remove function call and take a step back to ensure
              * next iteration does not skip not yet processed data
              */
            remove_expr_at_index(i);

            /// copy non-literal arguments
            std::remove_copy_if(
                    std::begin(args_ast->children), std::end(args_ast->children),
                    std::back_inserter(group_exprs), is_literal
            );
        }
        else if (is_literal(group_exprs[i]))
        {
            remove_expr_at_index(i);
        }
        else
        {
            /// if neither a function nor literal - advance to next expression
            ++i;
        }
    }

    if (group_exprs.empty())
    {
        /** You can not completely remove GROUP BY. Because if there were no aggregate functions, then it turns out that there will be no aggregation.
          * Instead, leave `GROUP BY const`.
          * Next, see deleting the constants in the analyzeAggregation method.
          */

        /// You must insert a constant that is not the name of the column in the table. Such a case is rare, but it happens.
        UInt64 unused_column = 0;
        String unused_column_name = toString(unused_column);

        while (source_columns.count(unused_column_name))
        {
            ++unused_column;
            unused_column_name = toString(unused_column);
        }

        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::make_shared<ASTExpressionList>());
        select_query->groupBy()->children.emplace_back(std::make_shared<ASTLiteral>(UInt64(unused_column)));
    }
}

/// Remove duplicate items from ORDER BY.
void optimizeOrderBy(const ASTSelectQuery * select_query)
{
    if (!select_query->orderBy())
        return;

    /// Make unique sorting conditions.
    using NameAndLocale = std::pair<String, String>;
    std::set<NameAndLocale> elems_set;

    ASTs & elems = select_query->orderBy()->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        String name = elem->children.front()->getColumnName();
        const auto & order_by_elem = elem->as<ASTOrderByElement &>();

        if (elems_set.emplace(name, order_by_elem.collation ? order_by_elem.collation->getColumnName() : "").second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = std::move(unique_elems);
}

/// Remove duplicate items from LIMIT BY.
void optimizeLimitBy(const ASTSelectQuery * select_query)
{
    if (!select_query->limitBy())
        return;

    std::set<String> elems_set;

    ASTs & elems = select_query->limitBy()->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        if (elems_set.emplace(elem->getColumnName()).second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = std::move(unique_elems);
}

/// Remove duplicated columns from USING(...).
void optimizeUsing(const ASTSelectQuery * select_query)
{
    if (!select_query->join())
        return;

    const auto * table_join = select_query->join()->table_join->as<ASTTableJoin>();
    if (!(table_join && table_join->using_expression_list))
        return;

    ASTs & expression_list = table_join->using_expression_list->children;
    ASTs uniq_expressions_list;

    std::set<String> expressions_names;

    for (const auto & expression : expression_list)
    {
        auto expression_name = expression->getAliasOrColumnName();
        if (expressions_names.find(expression_name) == expressions_names.end())
        {
            uniq_expressions_list.push_back(expression);
            expressions_names.insert(expression_name);
        }
    }

    if (uniq_expressions_list.size() < expression_list.size())
        expression_list = uniq_expressions_list;
}

void getArrayJoinedColumns(ASTPtr & query, SyntaxAnalyzerResult & result, const ASTSelectQuery * select_query,
                           const Names & source_columns, const NameSet & source_columns_set)
{
    if (ASTPtr array_join_expression_list = select_query->array_join_expression_list())
    {
        ArrayJoinedColumnsVisitor::Data visitor_data{result.aliases,
                                                    result.array_join_name_to_alias,
                                                    result.array_join_alias_to_name,
                                                    result.array_join_result_to_source};
        ArrayJoinedColumnsVisitor(visitor_data).visit(query);

        /// If the result of ARRAY JOIN is not used, it is necessary to ARRAY-JOIN any column,
        /// to get the correct number of rows.
        if (result.array_join_result_to_source.empty())
        {
            ASTPtr expr = select_query->array_join_expression_list()->children.at(0);
            String source_name = expr->getColumnName();
            String result_name = expr->getAliasOrColumnName();

            /// This is an array.
            if (!expr->as<ASTIdentifier>() || source_columns_set.count(source_name))
            {
                result.array_join_result_to_source[result_name] = source_name;
            }
            else /// This is a nested table.
            {
                bool found = false;
                for (const auto & column_name : source_columns)
                {
                    auto splitted = Nested::splitName(column_name);
                    if (splitted.first == source_name && !splitted.second.empty())
                    {
                        result.array_join_result_to_source[Nested::concatenateName(result_name, splitted.second)] = column_name;
                        found = true;
                        break;
                    }
                }
                if (!found)
                    throw Exception("No columns in nested table " + source_name, ErrorCodes::EMPTY_NESTED_TABLE);
            }
        }
    }
}

/// Parse JOIN ON expression and collect ASTs for joined columns.
void collectJoinedColumnsFromJoinOnExpr(AnalyzedJoin & analyzed_join, const ASTTableJoin & table_join)
{
    if (!table_join.on_expression)
        return;

    /// Stores examples of columns which are only from one table.
    struct TableBelonging
    {
        const ASTIdentifier * example_only_from_left = nullptr;
        const ASTIdentifier * example_only_from_right = nullptr;
    };

    /// Check all identifiers in ast and decide their possible table belonging.
    /// Throws if there are two identifiers definitely from different tables.
    std::function<TableBelonging(const ASTPtr &)> get_table_belonging;
    get_table_belonging = [&](const ASTPtr & ast) -> TableBelonging
    {
        if (IdentifierSemantic::getColumnName(ast))
        {
            const auto * identifier = ast->as<ASTIdentifier>();

            /// It's set in TranslateQualifiedNamesVisitor
            size_t membership = IdentifierSemantic::getMembership(*identifier);
            switch (membership)
            {
                case 1: return {identifier, nullptr};
                case 2: return {nullptr, identifier};
                default:
                    break;
            }

            return {};
        }

        TableBelonging table_belonging;
        for (const auto & child : ast->children)
        {
            auto children_belonging = get_table_belonging(child);
            if (!table_belonging.example_only_from_left)
                table_belonging.example_only_from_left = children_belonging.example_only_from_left;
            if (!table_belonging.example_only_from_right)
                table_belonging.example_only_from_right = children_belonging.example_only_from_right;
        }

        if (table_belonging.example_only_from_left && table_belonging.example_only_from_right)
            throw Exception("Invalid columns in JOIN ON section. Columns "
                            + table_belonging.example_only_from_left->getAliasOrColumnName() + " and "
                            + table_belonging.example_only_from_right->getAliasOrColumnName()
                            + " are from different tables.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);

        return table_belonging;
    };

    const auto supported_syntax = " Supported syntax: JOIN ON Expr([table.]column, ...) = Expr([table.]column, ...) "
                                  "[AND Expr([table.]column, ...) = Expr([table.]column, ...) ...]";
    auto throwSyntaxException = [&](const String & msg)
    {
        throw Exception("Invalid expression for JOIN ON. " + msg + supported_syntax, ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    };

    /// For equal expression find out corresponding table for each part, translate qualified names and add asts to join keys.
    auto add_columns_from_equals_expr = [&](const ASTPtr & expr)
    {
        const auto * func_equals = expr->as<ASTFunction>();
        if (!func_equals || func_equals->name != "equals")
            throwSyntaxException("Expected equals expression, got " + queryToString(expr) + ".");

        ASTPtr left_ast = func_equals->arguments->children.at(0)->clone();
        ASTPtr right_ast = func_equals->arguments->children.at(1)->clone();

        auto left_table_belonging = get_table_belonging(left_ast);
        auto right_table_belonging = get_table_belonging(right_ast);

        bool can_be_left_part_from_left_table = left_table_belonging.example_only_from_right == nullptr;
        bool can_be_left_part_from_right_table = left_table_belonging.example_only_from_left == nullptr;
        bool can_be_right_part_from_left_table = right_table_belonging.example_only_from_right == nullptr;
        bool can_be_right_part_from_right_table = right_table_belonging.example_only_from_left == nullptr;

        /// Default variant when all identifiers may be from any table.
        if (can_be_left_part_from_left_table && can_be_right_part_from_right_table)
            analyzed_join.addOnKeys(left_ast, right_ast);
        else if (can_be_left_part_from_right_table && can_be_right_part_from_left_table)
            analyzed_join.addOnKeys(right_ast, left_ast);
        else
        {
            auto * left_example = left_table_belonging.example_only_from_left ?
                                  left_table_belonging.example_only_from_left :
                                  left_table_belonging.example_only_from_right;

            auto * right_example = right_table_belonging.example_only_from_left ?
                                   right_table_belonging.example_only_from_left :
                                   right_table_belonging.example_only_from_right;

            auto left_name = queryToString(*left_example);
            auto right_name = queryToString(*right_example);
            auto expr_name = queryToString(expr);

            throwSyntaxException("In expression " + expr_name + " columns " + left_name + " and " + right_name
                                 + " are from the same table but from different arguments of equal function.");
        }
    };

    const auto * func = table_join.on_expression->as<ASTFunction>();
    if (func && func->name == "and")
    {
        for (const auto & expr : func->arguments->children)
            add_columns_from_equals_expr(expr);
    }
    else
        add_columns_from_equals_expr(table_join.on_expression);
}

/// Find the columns that are obtained by JOIN.
void collectJoinedColumns(AnalyzedJoin & analyzed_join, const ASTSelectQuery & select_query,
                          const NameSet & source_columns, const String & current_database, bool join_use_nulls)
{
    const ASTTablesInSelectQueryElement * node = select_query.join();

    if (!node)
        return;

    const auto & table_join = node->table_join->as<ASTTableJoin &>();
    const auto & table_expression = node->table_expression->as<ASTTableExpression &>();
    DatabaseAndTableWithAlias joined_table_name(table_expression, current_database);

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();
        for (const auto & key : keys.children)
            analyzed_join.addUsingKey(key);

        for (auto & name : analyzed_join.key_names_right)
            if (source_columns.count(name))
                name = joined_table_name.getQualifiedNamePrefix() + name;
    }
    else if (table_join.on_expression)
        collectJoinedColumnsFromJoinOnExpr(analyzed_join, table_join);

    bool make_nullable = join_use_nulls && isLeftOrFull(table_join.kind);

    analyzed_join.calculateAvailableJoinedColumns(make_nullable);
}

Names qualifyOccupiedNames(NamesAndTypesList & columns, const NameSet & source_columns, const DatabaseAndTableWithAlias& table)
{
    Names originals;
    originals.reserve(columns.size());

    for (auto & column : columns)
    {
        originals.push_back(column.name);
        if (source_columns.count(column.name))
            column.name = table.getQualifiedNamePrefix() + column.name;
    }

    return originals;
}

void replaceJoinedTable(const ASTTablesInSelectQueryElement* join)
{
    if (!join || !join->table_expression)
        return;

    auto & table_expr = join->table_expression->as<ASTTableExpression &>();
    if (table_expr.database_and_table_name)
    {
        const auto & table_id = table_expr.database_and_table_name->as<ASTIdentifier &>();
        String expr = "(select * from " + table_id.name + ") as " + table_id.shortName();

        // FIXME: since the expression "a as b" exposes both "a" and "b" names, which is not equivalent to "(select * from a) as b",
        //        we can't replace aliased tables.
        // FIXME: long table names include database name, which we can't save within alias.
        if (table_id.alias.empty() && table_id.isShort())
        {
            ParserTableExpression parser;
            table_expr = parseQuery(parser, expr, 0)->as<ASTTableExpression &>();
        }
    }
}

} // namespace


SyntaxAnalyzerResultPtr SyntaxAnalyzer::analyze(
    ASTPtr & query,
    const NamesAndTypesList & source_columns_,
    const Names & required_result_columns,
    StoragePtr storage) const
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!storage && select_query)
    {
        if (auto db_and_table = getDatabaseAndTable(*select_query, 0))
            storage = context.tryGetTable(db_and_table->database, db_and_table->table);
    }

    SyntaxAnalyzerResult result;
    result.storage = storage;
    result.source_columns = source_columns_;

    collectSourceColumns(select_query, result.storage, result.source_columns);
    NameSet source_columns_set = removeDuplicateColumns(result.source_columns);

    const auto & settings = context.getSettingsRef();

    Names source_columns_list;
    source_columns_list.reserve(result.source_columns.size());
    for (const auto & type_name : result.source_columns)
        source_columns_list.emplace_back(type_name.name);

    if (source_columns_set.size() != source_columns_list.size())
        throw Exception("Unexpected duplicates in source columns list.", ErrorCodes::LOGICAL_ERROR);

    if (select_query)
    {
        if (remove_duplicates)
            renameDuplicatedColumns(select_query);

        if (const ASTTablesInSelectQueryElement * node = select_query->join())
        {
            if (settings.enable_optimize_predicate_expression)
                replaceJoinedTable(node);

            const auto & joined_expression = node->table_expression->as<ASTTableExpression &>();
            DatabaseAndTableWithAlias table(joined_expression, context.getCurrentDatabase());

            NamesAndTypesList joined_columns = getNamesAndTypeListFromTableExpression(joined_expression, context);
            Names original_names = qualifyOccupiedNames(joined_columns, source_columns_set, table);
            result.analyzed_join.calculateColumnsFromJoinedTable(joined_columns, original_names);
        }

        translateQualifiedNames(query, *select_query, context,
                                (storage ? storage->getColumns().getOrdinary().getNames() : source_columns_list), source_columns_set,
                                result.analyzed_join.columns_from_joined_table);

        /// Rewrite IN and/or JOIN for distributed tables according to distributed_product_mode setting.
        InJoinSubqueriesPreprocessor(context).visit(query);

        /// Optimizes logical expressions.
        LogicalExpressionsOptimizer(select_query, settings.optimize_min_equality_disjunction_chain_length.value).perform();
    }

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    {
        LogAST log;
        QueryAliasesVisitor::Data query_aliases_data{result.aliases};
        QueryAliasesVisitor(query_aliases_data, log.stream()).visit(query);
    }

    /// Common subexpression elimination. Rewrite rules.
    {
        QueryNormalizer::Data normalizer_data(result.aliases, context.getSettingsRef());
        QueryNormalizer(normalizer_data).visit(query);
    }

    /// Remove unneeded columns according to 'required_result_columns'.
    /// Leave all selected columns in case of DISTINCT; columns that contain arrayJoin function inside.
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    if (select_query)
        removeUnneededColumnsFromSelectClause(select_query, required_result_columns, remove_duplicates);

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries(query, context, subquery_depth);

    /// Optimize if with constant condition after constants was substituted instead of scalar subqueries.
    OptimizeIfWithConstantConditionVisitor(result.aliases).visit(query);

    if (select_query)
    {
        /// GROUP BY injective function elimination.
        optimizeGroupBy(select_query, source_columns_set, context);

        /// Remove duplicate items from ORDER BY.
        optimizeOrderBy(select_query);

        /// Remove duplicated elements from LIMIT BY clause.
        optimizeLimitBy(select_query);

        /// Remove duplicated columns from USING(...).
        optimizeUsing(select_query);

        /// array_join_alias_to_name, array_join_result_to_source.
        getArrayJoinedColumns(query, result, select_query, source_columns_list, source_columns_set);

        /// Push the predicate expression down to the subqueries.
        result.rewrite_subqueries = PredicateExpressionsOptimizer(select_query, settings, context).optimize();

        collectJoinedColumns(result.analyzed_join, *select_query, source_columns_set, context.getCurrentDatabase(), settings.join_use_nulls);
    }

    return std::make_shared<const SyntaxAnalyzerResult>(result);
}

}
