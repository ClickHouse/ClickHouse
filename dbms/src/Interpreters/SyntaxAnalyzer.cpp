#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/Settings.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/ExternalDictionaries.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/queryToString.h>

#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>

#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <IO/WriteHelpers.h>

#include <functional>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ALIAS_REQUIRED;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int EMPTY_NESTED_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

namespace
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs
using Aliases = std::unordered_map<String, ASTPtr>;

/// Add columns from storage to source_columns list.
void collectSourceColumns(ASTSelectQuery * select_query, const Context & context,
                          StoragePtr & storage, NamesAndTypesList & source_columns);

/// Translate qualified names such as db.table.column, table.column, table_alias.column to unqualified names.
void translateQualifiedNames(ASTPtr & query, ASTSelectQuery * select_query,
                             const NameSet & source_columns, const Context & context);

/// For star nodes(`*`), expand them to a list of all columns. For literal nodes, substitute aliases.
void normalizeTree(
    ASTPtr & query,
    SyntaxAnalyzerResult & result,
    const Names & source_columns,
    const NameSet & source_columns_set,
    const StoragePtr & storage,
    const Context & context,
    const ASTSelectQuery * select_query,
    bool asterisk_left_columns_only);

/// Sometimes we have to calculate more columns in SELECT clause than will be returned from query.
/// This is the case when we have DISTINCT or arrayJoin: we require more columns in SELECT even if we need less columns in result.
void removeUnneededColumnsFromSelectClause(const ASTSelectQuery * select_query, const Names & required_result_columns);

/// Replacing scalar subqueries with constant values.
void executeScalarSubqueries(ASTPtr & query, const ASTSelectQuery * select_query,
                             const Context & context, size_t subquery_depth);

/// Remove Function_if AST if condition is constant.
void optimizeIfWithConstantCondition(ASTPtr & current_ast, Aliases & aliases);

/// Eliminates injective function calls and constant expressions from group by statement.
void optimizeGroupBy(ASTSelectQuery * select_query, const NameSet & source_columns, const Context & context);

/// Remove duplicate items from ORDER BY.
void optimizeOrderBy(const ASTSelectQuery * select_query);

/// Remove duplicate items from LIMIT BY.
void optimizeLimitBy(const ASTSelectQuery * select_query);

/// Remove duplicated columns from USING(...).
void optimizeUsing(const ASTSelectQuery * select_query);

void getArrayJoinedColumns(ASTPtr & query, SyntaxAnalyzerResult & result, const ASTSelectQuery * select_query,
                           const Names & source_columns, const NameSet & source_columns_set);

/// Parse JOIN ON expression and collect ASTs for joined columns.
void collectJoinedColumnsFromJoinOnExpr(AnalyzedJoin & analyzed_join, const ASTSelectQuery * select_query,
                                        const NameSet & source_columns, const Context & context);

/// Find the columns that are obtained by JOIN.
void collectJoinedColumns(AnalyzedJoin & analyzed_join, const ASTSelectQuery * select_query,
                          const NameSet & source_columns, const Context & context);
}

SyntaxAnalyzerResultPtr SyntaxAnalyzer::analyze(
    ASTPtr & query,
    const NamesAndTypesList & source_columns_,
    const Names & required_result_columns,
    size_t subquery_depth) const
{
    SyntaxAnalyzerResult result;
    result.storage = storage;
    result.source_columns = source_columns_;
    auto * select_query = typeid_cast<ASTSelectQuery *>(query.get());
    collectSourceColumns(select_query, context, result.storage, result.source_columns);

    const auto & settings = context.getSettingsRef();

    Names source_columns_list;
    source_columns_list.reserve(result.source_columns.size());
    for (const auto & type_name : result.source_columns)
        source_columns_list.emplace_back(type_name.name);
    NameSet source_columns_set(source_columns_list.begin(), source_columns_list.end());

    translateQualifiedNames(query, select_query, source_columns_set, context);

    /// Depending on the user's profile, check for the execution rights
    /// distributed subqueries inside the IN or JOIN sections and process these subqueries.
    InJoinSubqueriesPreprocessor(context).process(select_query);

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, settings.optimize_min_equality_disjunction_chain_length.value).perform();

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    {
        LogAST log;
        QueryAliasesVisitor query_aliases_visitor(result.aliases, log.stream());
        query_aliases_visitor.visit(query);
    }

    /// Common subexpression elimination. Rewrite rules.
    normalizeTree(query, result, source_columns_list, source_columns_set, result.storage,
                  context, select_query, settings.asterisk_left_columns_only != 0);

    /// Remove unneeded columns according to 'required_result_columns'.
    /// Leave all selected columns in case of DISTINCT; columns that contain arrayJoin function inside.
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    removeUnneededColumnsFromSelectClause(select_query, required_result_columns);

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries(query, select_query, context, subquery_depth);

    /// Optimize if with constant condition after constants was substituted instead of sclalar subqueries.
    optimizeIfWithConstantCondition(query, result.aliases);

    /// GROUP BY injective function elimination.
    optimizeGroupBy(select_query, source_columns_set, context);

    /// Remove duplicate items from ORDER BY.
    optimizeOrderBy(select_query);

    // Remove duplicated elements from LIMIT BY clause.
    optimizeLimitBy(select_query);

    /// Remove duplicated columns from USING(...).
    optimizeUsing(select_query);

    /// array_join_alias_to_name, array_join_result_to_source.
    getArrayJoinedColumns(query, result, select_query, source_columns_list, source_columns_set);

    /// Push the predicate expression down to the subqueries.
    result.rewrite_subqueries = PredicateExpressionsOptimizer(select_query, settings, context).optimize();

    collectJoinedColumns(result.analyzed_join, select_query, source_columns_set, context);

    return std::make_shared<const SyntaxAnalyzerResult>(result);
}

void removeDuplicateColumns(NamesAndTypesList & columns)
{
    std::set<String> names;
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (names.emplace(it->name).second)
            ++it;
        else
            columns.erase(it++);
    }
}

namespace
{

void collectSourceColumns(ASTSelectQuery * select_query, const Context & context,
                          StoragePtr & storage, NamesAndTypesList & source_columns)
{
    if (!storage && select_query)
    {
        if (auto db_and_table = getDatabaseAndTable(*select_query, 0))
            storage = context.tryGetTable(db_and_table->database, db_and_table->table);
    }

    if (storage)
    {
        auto physical_columns = storage->getColumns().getAllPhysical();
        if (source_columns.empty())
            source_columns.swap(physical_columns);
        else
            source_columns.insert(source_columns.end(), physical_columns.begin(), physical_columns.end());

        if (select_query)
        {
            const auto & storage_aliases = storage->getColumns().aliases;
            source_columns.insert(source_columns.end(), storage_aliases.begin(), storage_aliases.end());
        }
    }

    removeDuplicateColumns(source_columns);
}

void translateQualifiedNames(ASTPtr & query, ASTSelectQuery * select_query,
                             const NameSet & source_columns, const Context & context)
{
    if (!select_query || !select_query->tables || select_query->tables->children.empty())
        return;

    std::vector<DatabaseAndTableWithAlias> tables = getDatabaseAndTables(*select_query, context.getCurrentDatabase());

    LogAST log;
    TranslateQualifiedNamesMatcher::Data visitor_data{source_columns, tables};
    TranslateQualifiedNamesVisitor visitor(visitor_data, log.stream());
    visitor.visit(query);
}

void normalizeTree(
    ASTPtr & query,
    SyntaxAnalyzerResult & result,
    const Names & source_columns,
    const NameSet & source_columns_set,
    const StoragePtr & storage,
    const Context & context,
    const ASTSelectQuery * select_query,
    bool asterisk_left_columns_only)
{
    Names all_columns_name = storage ? storage->getColumns().ordinary.getNames() : source_columns;

    if (!asterisk_left_columns_only)
    {
        auto columns_from_joined_table = result.analyzed_join.getColumnsFromJoinedTable(source_columns_set, context, select_query);
        for (auto & column : columns_from_joined_table)
            all_columns_name.emplace_back(column.name_and_type.name);
    }

    if (all_columns_name.empty())
        throw Exception("An asterisk cannot be replaced with empty columns.", ErrorCodes::LOGICAL_ERROR);

    TableNamesAndColumnNames table_names_and_column_names;
    if (select_query && select_query->tables && !select_query->tables->children.empty())
    {
        std::vector<const ASTTableExpression *> tables_expression = getSelectTablesExpression(*select_query);

        bool first = true;
        for (const auto * table_expression : tables_expression)
        {
            DatabaseAndTableWithAlias table_name(*table_expression, context.getCurrentDatabase());
            NamesAndTypesList names_and_types = getNamesAndTypeListFromTableExpression(*table_expression, context);

            if (!first)
            {
                /// For joined tables qualify duplicating names.
                for (auto & name_and_type : names_and_types)
                    if (source_columns_set.count(name_and_type.name))
                        name_and_type.name = table_name.getQualifiedNamePrefix() + name_and_type.name;
            }

            first = false;

            table_names_and_column_names.emplace_back(std::pair(table_name, names_and_types.getNames()));
        }
    }

    auto & settings = context.getSettingsRef();
    QueryNormalizer(query, result.aliases, settings, all_columns_name, table_names_and_column_names).perform();
}

bool hasArrayJoin(const ASTPtr & ast)
{
    if (const ASTFunction * function = typeid_cast<const ASTFunction *>(&*ast))
        if (function->name == "arrayJoin")
            return true;

    for (const auto & child : ast->children)
        if (!typeid_cast<ASTSelectQuery *>(child.get()) && hasArrayJoin(child))
            return true;

    return false;
}

void removeUnneededColumnsFromSelectClause(const ASTSelectQuery * select_query, const Names & required_result_columns)
{
    if (!select_query)
        return;

    if (required_result_columns.empty())
        return;

    ASTs & elements = select_query->select_expression_list->children;

    ASTs new_elements;
    new_elements.reserve(elements.size());

    /// Some columns may be queried multiple times, like SELECT x, y, y FROM table.
    /// In that case we keep them exactly same number of times.
    std::map<String, size_t> required_columns_with_duplicate_count;
    for (const auto & name : required_result_columns)
        ++required_columns_with_duplicate_count[name];

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

void executeScalarSubqueries(ASTPtr & query, const ASTSelectQuery * select_query,
                             const Context & context, size_t subquery_depth)
{
    LogAST log;

    if (!select_query)
    {
        ExecuteScalarSubqueriesVisitor visitor(context, subquery_depth, log.stream());
        visitor.visit(query);
    }
    else
    {
        for (auto & child : query->children)
        {
            /// Do not go to FROM, JOIN, UNION.
            if (!typeid_cast<const ASTTableExpression *>(child.get())
                && !typeid_cast<const ASTSelectQuery *>(child.get()))
            {
                ExecuteScalarSubqueriesVisitor visitor(context, subquery_depth, log.stream());
                visitor.visit(child);
            }
        }
    }
}

bool tryExtractConstValueFromCondition(const ASTPtr & condition, bool & value)
{
    /// numeric constant in condition
    if (const ASTLiteral * literal = typeid_cast<ASTLiteral *>(condition.get()))
    {
        if (literal->value.getType() == Field::Types::Int64 ||
            literal->value.getType() == Field::Types::UInt64)
        {
            value = literal->value.get<Int64>();
            return true;
        }
    }

    /// cast of numeric constant in condition to UInt8
    if (const ASTFunction * function = typeid_cast<ASTFunction * >(condition.get()))
    {
        if (function->name == "CAST")
        {
            if (ASTExpressionList * expr_list = typeid_cast<ASTExpressionList *>(function->arguments.get()))
            {
                const ASTPtr & type_ast = expr_list->children.at(1);
                if (const ASTLiteral * type_literal = typeid_cast<ASTLiteral *>(type_ast.get()))
                {
                    if (type_literal->value.getType() == Field::Types::String &&
                        type_literal->value.get<std::string>() == "UInt8")
                        return tryExtractConstValueFromCondition(expr_list->children.at(0), value);
                }
            }
        }
    }

    return false;
}

void optimizeIfWithConstantCondition(ASTPtr & current_ast, Aliases & aliases)
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        auto * function_node = typeid_cast<ASTFunction *>(child.get());
        if (!function_node || function_node->name != "if")
        {
            optimizeIfWithConstantCondition(child, aliases);
            continue;
        }

        optimizeIfWithConstantCondition(function_node->arguments, aliases);
        auto * args = typeid_cast<ASTExpressionList *>(function_node->arguments.get());

        if (args->children.size() != 3)
            throw Exception("Wrong number of arguments for function 'if' (" + toString(args->children.size()) + " instead of 3)",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTPtr condition_expr = args->children[0];
        ASTPtr then_expr = args->children[1];
        ASTPtr else_expr = args->children[2];

        bool condition;
        if (tryExtractConstValueFromCondition(condition_expr, condition))
        {
            ASTPtr replace_ast = condition ? then_expr : else_expr;
            ASTPtr child_copy = child;
            String replace_alias = replace_ast->tryGetAlias();
            String if_alias = child->tryGetAlias();

            if (replace_alias.empty())
            {
                replace_ast->setAlias(if_alias);
                child = replace_ast;
            }
            else
            {
                /// Only copy of one node is required here.
                /// But IAST has only method for deep copy of subtree.
                /// This can be a reason of performance degradation in case of deep queries.
                ASTPtr replace_ast_deep_copy = replace_ast->clone();
                replace_ast_deep_copy->setAlias(if_alias);
                child = replace_ast_deep_copy;
            }

            if (!if_alias.empty())
            {
                auto alias_it = aliases.find(if_alias);
                if (alias_it != aliases.end() && alias_it->second.get() == child_copy.get())
                    alias_it->second = child;
            }
        }
    }
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

void optimizeGroupBy(ASTSelectQuery * select_query, const NameSet & source_columns, const Context & context)
{
    if (!(select_query && select_query->group_expression_list))
        return;

    const auto is_literal = [] (const ASTPtr & ast)
    {
        return typeid_cast<const ASTLiteral *>(ast.get());
    };

    auto & group_exprs = select_query->group_expression_list->children;

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
        if (const auto function = typeid_cast<ASTFunction *>(group_exprs[i].get()))
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

                const auto & dict_name = typeid_cast<const ASTLiteral &>(*function->arguments->children[0])
                        .value.safeGet<String>();

                const auto & dict_ptr = context.getExternalDictionaries().getDictionary(dict_name);

                const auto & attr_name = typeid_cast<const ASTLiteral &>(*function->arguments->children[1])
                        .value.safeGet<String>();

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

        select_query->group_expression_list = std::make_shared<ASTExpressionList>();
        select_query->group_expression_list->children.emplace_back(std::make_shared<ASTLiteral>(UInt64(unused_column)));
    }
}

void optimizeOrderBy(const ASTSelectQuery * select_query)
{
    if (!(select_query && select_query->order_expression_list))
        return;

    /// Make unique sorting conditions.
    using NameAndLocale = std::pair<String, String>;
    std::set<NameAndLocale> elems_set;

    ASTs & elems = select_query->order_expression_list->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        String name = elem->children.front()->getColumnName();
        const ASTOrderByElement & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

        if (elems_set.emplace(name, order_by_elem.collation ? order_by_elem.collation->getColumnName() : "").second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = unique_elems;
}

void optimizeLimitBy(const ASTSelectQuery * select_query)
{
    if (!(select_query && select_query->limit_by_expression_list))
        return;

    std::set<String> elems_set;

    ASTs & elems = select_query->limit_by_expression_list->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        if (elems_set.emplace(elem->getColumnName()).second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = unique_elems;
}

void optimizeUsing(const ASTSelectQuery * select_query)
{
    if (!select_query)
        return;

    auto node = const_cast<ASTTablesInSelectQueryElement *>(select_query->join());
    if (!node)
        return;

    auto table_join = static_cast<ASTTableJoin *>(&*node->table_join);
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
    if (select_query && select_query->array_join_expression_list())
    {
        ASTs & array_join_asts = select_query->array_join_expression_list()->children;
        for (const auto & ast : array_join_asts)
        {
            const String nested_table_name = ast->getColumnName();
            const String nested_table_alias = ast->getAliasOrColumnName();

            if (nested_table_alias == nested_table_name && !typeid_cast<const ASTIdentifier *>(ast.get()))
                throw Exception("No alias for non-trivial value in ARRAY JOIN: " + nested_table_name,
                                ErrorCodes::ALIAS_REQUIRED);

            if (result.array_join_alias_to_name.count(nested_table_alias) || result.aliases.count(nested_table_alias))
                throw Exception("Duplicate alias in ARRAY JOIN: " + nested_table_alias,
                                ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

            result.array_join_alias_to_name[nested_table_alias] = nested_table_name;
            result.array_join_name_to_alias[nested_table_name] = nested_table_alias;
        }

        {
            ArrayJoinedColumnsVisitor visitor(result.array_join_name_to_alias,
                                              result.array_join_alias_to_name,
                                              result.array_join_result_to_source);
            visitor.visit(query);
        }

        /// If the result of ARRAY JOIN is not used, it is necessary to ARRAY-JOIN any column,
        /// to get the correct number of rows.
        if (result.array_join_result_to_source.empty())
        {
            ASTPtr expr = select_query->array_join_expression_list()->children.at(0);
            String source_name = expr->getColumnName();
            String result_name = expr->getAliasOrColumnName();

            /// This is an array.
            if (!typeid_cast<ASTIdentifier *>(expr.get()) || source_columns_set.count(source_name))
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

void collectJoinedColumnsFromJoinOnExpr(AnalyzedJoin & analyzed_join, const ASTSelectQuery * select_query,
                                        const NameSet & source_columns, const Context & context)
{
    const auto & tables = static_cast<const ASTTablesInSelectQuery &>(*select_query->tables);
    const auto * left_tables_element = static_cast<const ASTTablesInSelectQueryElement *>(tables.children.at(0).get());
    const auto * right_tables_element = select_query->join();

    if (!left_tables_element || !right_tables_element)
        return;

    const auto & table_join = static_cast<const ASTTableJoin &>(*right_tables_element->table_join);
    if (!table_join.on_expression)
        return;

    const auto & left_table_expression = static_cast<const ASTTableExpression &>(*left_tables_element->table_expression);
    const auto & right_table_expression = static_cast<const ASTTableExpression &>(*right_tables_element->table_expression);

    DatabaseAndTableWithAlias left_source_names(left_table_expression, context.getCurrentDatabase());
    DatabaseAndTableWithAlias right_source_names(right_table_expression, context.getCurrentDatabase());

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
        auto * identifier = typeid_cast<const ASTIdentifier *>(ast.get());
        if (identifier)
        {
            if (identifier->general())
            {
                auto left_num_components = getNumComponentsToStripInOrderToTranslateQualifiedName(*identifier, left_source_names);
                auto right_num_components = getNumComponentsToStripInOrderToTranslateQualifiedName(*identifier, right_source_names);

                /// Assume that component from definite table if num_components is greater than for the other table.
                if (left_num_components > right_num_components)
                    return {identifier, nullptr};
                if (left_num_components < right_num_components)
                    return {nullptr, identifier};
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

    std::function<void(ASTPtr &, const DatabaseAndTableWithAlias &, bool)> translate_qualified_names;
    translate_qualified_names = [&](ASTPtr & ast, const DatabaseAndTableWithAlias & source_names, bool right_table)
    {
        if (auto * identifier = typeid_cast<const ASTIdentifier *>(ast.get()))
        {
            if (identifier->general())
            {
                auto num_components = getNumComponentsToStripInOrderToTranslateQualifiedName(*identifier, source_names);
                stripIdentifier(ast, num_components);

                if (right_table && source_columns.count(ast->getColumnName()))
                    source_names.makeQualifiedName(ast);

            }
            return;
        }

        for (auto & child : ast->children)
            translate_qualified_names(child, source_names, right_table);
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
        auto * func_equals = typeid_cast<const ASTFunction *>(expr.get());
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

        auto add_join_keys = [&](ASTPtr & ast_to_left_table, ASTPtr & ast_to_right_table)
        {
            translate_qualified_names(ast_to_left_table, left_source_names, false);
            translate_qualified_names(ast_to_right_table, right_source_names, true);

            analyzed_join.key_asts_left.push_back(ast_to_left_table);
            analyzed_join.key_names_left.push_back(ast_to_left_table->getColumnName());
            analyzed_join.key_asts_right.push_back(ast_to_right_table);
            analyzed_join.key_names_right.push_back(ast_to_right_table->getAliasOrColumnName());
        };

        /// Default variant when all identifiers may be from any table.
        if (can_be_left_part_from_left_table && can_be_right_part_from_right_table)
            add_join_keys(left_ast, right_ast);
        else if (can_be_left_part_from_right_table && can_be_right_part_from_left_table)
            add_join_keys(right_ast, left_ast);
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

    auto * func = typeid_cast<const ASTFunction *>(table_join.on_expression.get());
    if (func && func->name == "and")
    {
        for (const auto & expr : func->arguments->children)
            add_columns_from_equals_expr(expr);
    }
    else
        add_columns_from_equals_expr(table_join.on_expression);
}

void collectJoinedColumns(AnalyzedJoin & analyzed_join, const ASTSelectQuery * select_query,
                          const NameSet & source_columns, const Context & context)
{
    if (!select_query)
        return;

    const ASTTablesInSelectQueryElement * node = select_query->join();

    if (!node)
        return;

    const auto & table_join = static_cast<const ASTTableJoin &>(*node->table_join);
    const auto & table_expression = static_cast<const ASTTableExpression &>(*node->table_expression);
    DatabaseAndTableWithAlias joined_table_name(table_expression, context.getCurrentDatabase());

    auto add_name_to_join_keys = [&](Names & join_keys, ASTs & join_asts, const ASTPtr & ast, bool right_table)
    {
        String name;
        if (right_table)
        {
            name = ast->getAliasOrColumnName();
            if (source_columns.count(name))
                name = joined_table_name.getQualifiedNamePrefix() + name;
        }
        else
            name = ast->getColumnName();

        join_keys.push_back(name);
        join_asts.push_back(ast);
    };

    if (table_join.using_expression_list)
    {
        auto & keys = typeid_cast<ASTExpressionList &>(*table_join.using_expression_list);
        for (const auto & key : keys.children)
        {
            add_name_to_join_keys(analyzed_join.key_names_left, analyzed_join.key_asts_left, key, false);
            add_name_to_join_keys(analyzed_join.key_names_right, analyzed_join.key_asts_right, key, true);
        }
    }
    else if (table_join.on_expression)
        collectJoinedColumnsFromJoinOnExpr(analyzed_join, select_query, source_columns, context);

    auto & columns_from_joined_table = analyzed_join.getColumnsFromJoinedTable(source_columns, context, select_query);

    NameSet joined_columns;

    auto & settings = context.getSettingsRef();

    for (auto & column : columns_from_joined_table)
    {
        auto & column_name = column.name_and_type.name;
        auto & column_type = column.name_and_type.type;
        auto & original_name = column.original_name;
        {
            if (joined_columns.count(column_name)) /// Duplicate columns in the subquery for JOIN do not make sense.
                continue;

            joined_columns.insert(column_name);

            bool make_nullable = settings.join_use_nulls && (table_join.kind == ASTTableJoin::Kind::Left ||
                                                             table_join.kind == ASTTableJoin::Kind::Full);
            auto type = make_nullable ? makeNullable(column_type) : column_type;
            analyzed_join.available_joined_columns.emplace_back(NameAndTypePair(column_name, std::move(type)), original_name);
        }
    }
}

}

}
