#include <Poco/Util/Application.h>
#include <Poco/String.h>

#include <Core/Block.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/formatAST.h>
#include <Parsers/DumpASTNode.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/IColumn.h>

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Set.h>
#include <Interpreters/Join.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Dictionaries/IDictionary.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <Parsers/formatAST.h>

#include <ext/range.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Parsers/queryToString.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/evaluateQualified.h>
#include <Interpreters/QueryNormalizer.h>

#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/ExternalTablesVisitor.h>
#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int CYCLIC_ALIASES;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_ELEMENT_OF_SET;
    extern const int ALIAS_REQUIRED;
    extern const int EMPTY_NESTED_TABLE;
    extern const int DUPLICATE_COLUMN;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int ILLEGAL_AGGREGATION;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CONDITIONAL_TREE_PARENT_NOT_FOUND;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int EXPECTED_ALL_OR_ANY;
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

namespace
{

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

}


ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const Context & context_,
    const StoragePtr & storage_,
    const NamesAndTypesList & source_columns_,
    const Names & required_result_columns_,
    size_t subquery_depth_,
    bool do_global_,
    const SubqueriesForSets & subqueries_for_sets_)
    : ExpressionAnalyzerData(source_columns_, required_result_columns_, subqueries_for_sets_),
    query(query_), context(context_), settings(context.getSettings()), storage(storage_),
    subquery_depth(subquery_depth_), do_global(do_global_)
{
    select_query = typeid_cast<ASTSelectQuery *>(query.get());

    if (!storage && select_query)
    {
        auto select_database = select_query->database();
        auto select_table = select_query->table();

        if (select_table
            && !typeid_cast<const ASTSelectWithUnionQuery *>(select_table.get())
            && !typeid_cast<const ASTFunction *>(select_table.get()))
        {
            String database = select_database
                ? typeid_cast<const ASTIdentifier &>(*select_database).name
                : "";
            const String & table = typeid_cast<const ASTIdentifier &>(*select_table).name;
            storage = context.tryGetTable(database, table);
        }
    }

    if (storage && source_columns.empty())
    {
        auto physical_columns = storage->getColumns().getAllPhysical();
        if (source_columns.empty())
            source_columns.swap(physical_columns);
        else
        {
            source_columns.insert(source_columns.end(), physical_columns.begin(), physical_columns.end());
            removeDuplicateColumns(source_columns);
        }
    }
    else
        removeDuplicateColumns(source_columns);

    addAliasColumns();

    translateQualifiedNames();

    /// Depending on the user's profile, check for the execution rights
    /// distributed subqueries inside the IN or JOIN sections and process these subqueries.
    InJoinSubqueriesPreprocessor(context).process(select_query);

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, settings.min_equality_disjunction_chain_length).perform();

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    {
        LogAST log;
        QueryAliasesVisitor query_aliases_visitor(log.stream());
        query_aliases_visitor.visit(query, aliases);
    }

    /// Common subexpression elimination. Rewrite rules.
    normalizeTree();

    /// Remove unneeded columns according to 'required_result_columns'.
    /// Leave all selected columns in case of DISTINCT; columns that contain arrayJoin function inside.
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    removeUnneededColumnsFromSelectClause();

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries();

    /// Optimize if with constant condition after constants was substituted instead of sclalar subqueries.
    optimizeIfWithConstantCondition();

    /// GROUP BY injective function elimination.
    optimizeGroupBy();

    /// Remove duplicate items from ORDER BY.
    optimizeOrderBy();

    // Remove duplicated elements from LIMIT BY clause.
    optimizeLimitBy();

    /// Remove duplicated columns from USING(...).
    optimizeUsing();

    /// array_join_alias_to_name, array_join_result_to_source.
    getArrayJoinedColumns();

    /// Push the predicate expression down to the subqueries.
    rewrite_subqueries = PredicateExpressionsOptimizer(select_query, settings, context).optimize();

    /// Delete the unnecessary from `source_columns` list. Create `unknown_required_source_columns`. Form `columns_added_by_join`.
    collectUsedColumns();

    /// external_tables, subqueries_for_sets for global subqueries.
    /// Replaces global subqueries with the generated names of temporary tables that will be sent to remote servers.
    initGlobalSubqueriesAndExternalTables();

    /// has_aggregation, aggregation_keys, aggregate_descriptions, aggregated_columns.
    /// This analysis should be performed after processing global subqueries, because otherwise,
    /// if the aggregate function contains a global subquery, then `analyzeAggregation` method will save
    /// in `aggregate_descriptions` the information about the parameters of this aggregate function, among which
    /// global subquery. Then, when you call `initGlobalSubqueriesAndExternalTables` method, this
    /// the global subquery will be replaced with a temporary table, resulting in aggregate_descriptions
    /// will contain out-of-date information, which will lead to an error when the query is executed.
    analyzeAggregation();
}

bool ExpressionAnalyzer::isRemoteStorage() const
{
    return storage && storage->isRemote();
}


void ExpressionAnalyzer::translateQualifiedNames()
{
    if (!select_query || !select_query->tables || select_query->tables->children.empty())
        return;

    std::vector<DatabaseAndTableWithAlias> tables = getDatabaseAndTableWithAliases(select_query, context.getCurrentDatabase());

    LogAST log;
    TranslateQualifiedNamesVisitor visitor(source_columns, tables, log.stream());
    visitor.visit(query);
}

void ExpressionAnalyzer::optimizeIfWithConstantCondition()
{
    optimizeIfWithConstantConditionImpl(query);
}

bool ExpressionAnalyzer::tryExtractConstValueFromCondition(const ASTPtr & condition, bool & value) const
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

void ExpressionAnalyzer::optimizeIfWithConstantConditionImpl(ASTPtr & current_ast)
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        ASTFunction * function_node = typeid_cast<ASTFunction *>(child.get());
        if (!function_node || function_node->name != "if")
        {
            optimizeIfWithConstantConditionImpl(child);
            continue;
        }

        optimizeIfWithConstantConditionImpl(function_node->arguments);
        ASTExpressionList * args = typeid_cast<ASTExpressionList *>(function_node->arguments.get());

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

void ExpressionAnalyzer::analyzeAggregation()
{
    /** Find aggregation keys (aggregation_keys), information about aggregate functions (aggregate_descriptions),
     *  as well as a set of columns obtained after the aggregation, if any,
     *  or after all the actions that are usually performed before aggregation (aggregated_columns).
     *
     * Everything below (compiling temporary ExpressionActions) - only for the purpose of query analysis (type output).
     */

    if (select_query && (select_query->group_expression_list || select_query->having_expression))
        has_aggregation = true;

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(source_columns, context);

    if (select_query && select_query->array_join_expression_list())
    {
        getRootActions(select_query->array_join_expression_list(), true, temp_actions);
        addMultipleArrayJoinAction(temp_actions);
        array_join_columns = temp_actions->getSampleBlock().getNamesAndTypesList();
    }

    if (select_query)
    {
        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            const auto table_join = static_cast<const ASTTableJoin &>(*join->table_join);
            if (table_join.using_expression_list)
                getRootActions(table_join.using_expression_list, true, temp_actions);
            if (table_join.on_expression)
                for (const auto & key_ast : analyzed_join.key_asts_left)
                    getRootActions(key_ast, true, temp_actions);

            addJoinAction(temp_actions, true);
        }
    }

    getAggregates(query, temp_actions);

    if (has_aggregation)
    {
        assertSelect();

        /// Find out aggregation keys.
        if (select_query->group_expression_list)
        {
            NameSet unique_keys;
            ASTs & group_asts = select_query->group_expression_list->children;
            for (ssize_t i = 0; i < ssize_t(group_asts.size()); ++i)
            {
                ssize_t size = group_asts.size();
                getRootActions(group_asts[i], true, temp_actions);

                const auto & column_name = group_asts[i]->getColumnName();
                const auto & block = temp_actions->getSampleBlock();

                if (!block.has(column_name))
                    throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                const auto & col = block.getByName(column_name);

                /// Constant expressions have non-null column pointer at this stage.
                if (col.column && col.column->isColumnConst())
                {
                    /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                    if (!aggregate_descriptions.empty() || size > 1)
                    {
                        if (i + 1 < static_cast<ssize_t>(size))
                            group_asts[i] = std::move(group_asts.back());

                        group_asts.pop_back();

                        --i;
                        continue;
                    }
                }

                NameAndTypePair key{column_name, col.type};

                /// Aggregation keys are uniqued.
                if (!unique_keys.count(key.name))
                {
                    unique_keys.insert(key.name);
                    aggregation_keys.push_back(key);

                    /// Key is no longer needed, therefore we can save a little by moving it.
                    aggregated_columns.push_back(std::move(key));
                }
            }

            if (group_asts.empty())
            {
                select_query->group_expression_list = nullptr;
                has_aggregation = select_query->having_expression || aggregate_descriptions.size();
            }
        }

        for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
        {
            AggregateDescription & desc = aggregate_descriptions[i];
            aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
        }
    }
    else
    {
        aggregated_columns = temp_actions->getSampleBlock().getNamesAndTypesList();
    }
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables()
{
    /// Adds existing external tables (not subqueries) to the external_tables dictionary.
    ExternalTablesVisitor tables_visitor(context, external_tables);
    tables_visitor.visit(query);

    if (do_global)
    {
        GlobalSubqueriesVisitor subqueries_visitor(context, subquery_depth, isRemoteStorage(),
                                                   external_tables, subqueries_for_sets, has_global_subqueries);
        subqueries_visitor.visit(query);
    }
}


NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols)
{
    return std::find_if(cols.begin(), cols.end(),
        [&](const NamesAndTypesList::value_type & val) { return val.name == name; });
}

static NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context)
{
    NamesAndTypesList names_and_type_list;
    if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        names_and_type_list = InterpreterSelectWithUnionQuery::getSampleBlock(subquery, context).getNamesAndTypesList();
    }
    else if (table_expression.table_function)
    {
        const auto table_function = table_expression.table_function;
        auto query_context = const_cast<Context *>(&context.getQueryContext());
        const auto & function_storage = query_context->executeTableFunction(table_function);
        names_and_type_list = function_storage->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }
    else if (table_expression.database_and_table_name)
    {
        const auto & identifier = static_cast<const ASTIdentifier &>(*table_expression.database_and_table_name);
        auto database_table = getDatabaseAndTableNameFromIdentifier(identifier);
        const auto & table = context.getTable(database_table.first, database_table.second);
        names_and_type_list = table->getSampleBlockNonMaterialized().getNamesAndTypesList();
    }

    return names_and_type_list;
}

void ExpressionAnalyzer::normalizeTree()
{
    Names all_columns_name;

    auto columns_name = storage ? storage->getColumns().ordinary.getNames() : source_columns.getNames();
    all_columns_name.insert(all_columns_name.begin(), columns_name.begin(), columns_name.end());

    if (!settings.asterisk_left_columns_only)
    {
        auto columns_from_joined_table = analyzed_join.getColumnsFromJoinedTable(source_columns, context, select_query);
        for (auto & column : columns_from_joined_table)
            all_columns_name.emplace_back(column.name_and_type.name);
    }

    if (all_columns_name.empty())
        throw Exception("An asterisk cannot be replaced with empty columns.", ErrorCodes::LOGICAL_ERROR);

    TableNamesAndColumnNames table_names_and_column_names;
    if (select_query && select_query->tables && !select_query->tables->children.empty())
    {
        std::vector<const ASTTableExpression *> tables_expression = getSelectTablesExpression(select_query);

        bool first = true;
        for (const auto * table_expression : tables_expression)
        {
            const auto table_name = getTableNameWithAliasFromTableExpression(*table_expression, context.getCurrentDatabase());
            NamesAndTypesList names_and_types = getNamesAndTypeListFromTableExpression(*table_expression, context);

            if (!first)
            {
                /// For joined tables qualify duplicating names.
                for (auto & name_and_type : names_and_types)
                    if (source_columns.contains(name_and_type.name))
                        name_and_type.name = table_name.getQualifiedNamePrefix() + name_and_type.name;
            }

            first = false;

            table_names_and_column_names.emplace_back(std::pair(table_name, names_and_types.getNames()));
        }
    }

    QueryNormalizer(query, aliases, settings, all_columns_name, table_names_and_column_names).perform();
}


void ExpressionAnalyzer::addAliasColumns()
{
    if (!select_query)
        return;

    if (!storage)
        return;

    const auto & storage_aliases = storage->getColumns().aliases;
    source_columns.insert(std::end(source_columns), std::begin(storage_aliases), std::end(storage_aliases));
}


void ExpressionAnalyzer::executeScalarSubqueries()
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


void ExpressionAnalyzer::optimizeGroupBy()
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

        while (source_columns.end() != std::find_if(source_columns.begin(), source_columns.end(),
            [&unused_column_name](const NameAndTypePair & name_type) { return name_type.name == unused_column_name; }))
        {
            ++unused_column;
            unused_column_name = toString(unused_column);
        }

        select_query->group_expression_list = std::make_shared<ASTExpressionList>();
        select_query->group_expression_list->children.emplace_back(std::make_shared<ASTLiteral>(UInt64(unused_column)));
    }
}


void ExpressionAnalyzer::optimizeOrderBy()
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


void ExpressionAnalyzer::optimizeLimitBy()
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

void ExpressionAnalyzer::optimizeUsing()
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


void ExpressionAnalyzer::makeSetsForIndex()
{
    if (storage && select_query && storage->supportsIndexForIn())
    {
        if (select_query->where_expression)
            makeSetsForIndexImpl(select_query->where_expression, storage->getSampleBlock());
        if (select_query->prewhere_expression)
            makeSetsForIndexImpl(select_query->prewhere_expression, storage->getSampleBlock());
    }
}


void ExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name)
{
    BlockIO res = interpretSubquery(subquery_or_table_name, context, subquery_depth + 1, {})->execute();

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true);

    set->setHeader(res.in->getHeader());
    while (Block block = res.in->read())
    {
        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }

    prepared_sets[subquery_or_table_name->range] = std::move(set);
}


void ExpressionAnalyzer::makeSetsForIndexImpl(const ASTPtr & node, const Block & sample_block)
{
    for (auto & child : node->children)
    {
        /// Don't descent into subqueries.
        if (typeid_cast<ASTSubquery *>(child.get()))
            continue;

        /// Don't dive into lambda functions
        const ASTFunction * func = typeid_cast<const ASTFunction *>(child.get());
        if (func && func->name == "lambda")
            continue;

        makeSetsForIndexImpl(child, sample_block);
    }

    const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get());
    if (func && functionIsInOperator(func->name))
    {
        const IAST & args = *func->arguments;

        if (storage && storage->mayBenefitFromIndexForIn(args.children.at(0)))
        {
            const ASTPtr & arg = args.children.at(1);

            if (!prepared_sets.count(arg->range)) /// Not already prepared.
            {
                if (typeid_cast<ASTSubquery *>(arg.get()) || typeid_cast<ASTIdentifier *>(arg.get()))
                {
                    if (settings.use_index_for_in_with_subqueries)
                        tryMakeSetForIndexFromSubquery(arg);
                }
                else
                {
                    NamesAndTypesList temp_columns = source_columns;
                    temp_columns.insert(temp_columns.end(), array_join_columns.begin(), array_join_columns.end());
                    for (const auto & joined_column : analyzed_join.columns_added_by_join)
                        temp_columns.push_back(joined_column.name_and_type);
                    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(temp_columns, context);
                    getRootActions(func->arguments->children.at(0), true, temp_actions);

                    Block sample_block_with_calculated_columns = temp_actions->getSampleBlock();
                    if (sample_block_with_calculated_columns.has(args.children.at(0)->getColumnName()))
                        makeExplicitSet(func, sample_block_with_calculated_columns, true, context,
                                        settings.size_limits_for_set, prepared_sets);
                }
            }
        }
    }
}


void ExpressionAnalyzer::getArrayJoinedColumns()
{
    if (select_query && select_query->array_join_expression_list())
    {
        ASTs & array_join_asts = select_query->array_join_expression_list()->children;
        for (const auto & ast : array_join_asts)
        {
            const String nested_table_name = ast->getColumnName();
            const String nested_table_alias = ast->getAliasOrColumnName();

            if (nested_table_alias == nested_table_name && !typeid_cast<const ASTIdentifier *>(ast.get()))
                throw Exception("No alias for non-trivial value in ARRAY JOIN: " + nested_table_name, ErrorCodes::ALIAS_REQUIRED);

            if (array_join_alias_to_name.count(nested_table_alias) || aliases.count(nested_table_alias))
                throw Exception("Duplicate alias in ARRAY JOIN: " + nested_table_alias, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

            array_join_alias_to_name[nested_table_alias] = nested_table_name;
            array_join_name_to_alias[nested_table_name] = nested_table_alias;
        }

        {
            ArrayJoinedColumnsVisitor visitor(array_join_name_to_alias, array_join_alias_to_name, array_join_result_to_source);
            visitor.visit(query);
        }

        /// If the result of ARRAY JOIN is not used, it is necessary to ARRAY-JOIN any column,
        /// to get the correct number of rows.
        if (array_join_result_to_source.empty())
        {
            ASTPtr expr = select_query->array_join_expression_list()->children.at(0);
            String source_name = expr->getColumnName();
            String result_name = expr->getAliasOrColumnName();

            /// This is an array.
            if (!typeid_cast<ASTIdentifier *>(expr.get()) || findColumn(source_name, source_columns) != source_columns.end())
            {
                array_join_result_to_source[result_name] = source_name;
            }
            else /// This is a nested table.
            {
                bool found = false;
                for (const auto & column_name_type : source_columns)
                {
                    auto splitted = Nested::splitName(column_name_type.name);
                    if (splitted.first == source_name && !splitted.second.empty())
                    {
                        array_join_result_to_source[Nested::concatenateName(result_name, splitted.second)] = column_name_type.name;
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


bool ExpressionAnalyzer::isThereArrayJoin(const ASTPtr & ast)
{
    if (typeid_cast<ASTIdentifier *>(ast.get()))
    {
        return false;
    }
    else if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->name == "arrayJoin")
        {
            return true;
        }
        if (functionIsInOrGlobalInOperator(node->name))
        {
            return isThereArrayJoin(node->arguments->children.at(0));
        }
        if (node->name == "indexHint")
        {
            return false;
        }
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        {
            return false;
        }
        for (auto & child : node->arguments->children)
        {
            if (isThereArrayJoin(child))
            {
                return true;
            }
        }
        return false;
    }
    else if (typeid_cast<ASTLiteral *>(ast.get()))
    {
        return false;
    }
    else
    {
        for (auto & child : ast->children)
        {
            if (isThereArrayJoin(child))
            {
                return true;
            }
        }
        return false;
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_subqueries, ExpressionActionsPtr & actions, bool only_consts)
{
    bool is_conditional_tree = !isThereArrayJoin(ast) && settings.enable_conditional_computation && !only_consts;

    LogAST log;
    ActionsVisitor actions_visitor(context, settings.size_limits_for_set, is_conditional_tree, subquery_depth,
                                   source_columns, actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage(), log.stream());
    actions_visitor.visit(ast);
    actions = actions_visitor.popActionsLevel();
}


void ExpressionAnalyzer::getActionsFromJoinKeys(const ASTTableJoin & table_join, bool no_subqueries, ExpressionActionsPtr & actions)
{
    bool only_consts = false;
    bool is_conditional_tree = !isThereArrayJoin(query) && settings.enable_conditional_computation && !only_consts;

    LogAST log;
    ActionsVisitor actions_visitor(context, settings.size_limits_for_set, is_conditional_tree, subquery_depth,
                                   source_columns, actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage(), log.stream());

    if (table_join.using_expression_list)
        actions_visitor.visit(table_join.using_expression_list);
    else if (table_join.on_expression)
    {
        for (const auto & ast : analyzed_join.key_asts_left)
            actions_visitor.visit(ast);
    }

    actions = actions_visitor.popActionsLevel();
}


void ExpressionAnalyzer::getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions)
{
    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query && (ast.get() == select_query->where_expression.get() || ast.get() == select_query->prewhere_expression.get()))
    {
        assertNoAggregates(ast, "in WHERE or PREWHERE");
        return;
    }

    /// If we are not analyzing a SELECT query, but a separate expression, then there can not be aggregate functions in it.
    if (!select_query)
    {
        assertNoAggregates(ast, "in wrong place");
        return;
    }

    const ASTFunction * node = typeid_cast<const ASTFunction *>(ast.get());
    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
    {
        has_aggregation = true;
        AggregateDescription aggregate;
        aggregate.column_name = node->getColumnName();

        /// Make unique aggregate functions.
        for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
            if (aggregate_descriptions[i].column_name == aggregate.column_name)
                return;

        const ASTs & arguments = node->arguments->children;
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            /// There can not be other aggregate functions within the aggregate functions.
            assertNoAggregates(arguments[i], "inside another aggregate function");

            getRootActions(arguments[i], true, actions);
            const std::string & name = arguments[i]->getColumnName();
            types[i] = actions->getSampleBlock().getByName(name).type;
            aggregate.argument_names[i] = name;
        }

        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters);

        aggregate_descriptions.push_back(aggregate);
    }
    else
    {
        for (const auto & child : ast->children)
            if (!typeid_cast<const ASTSubquery *>(child.get())
                && !typeid_cast<const ASTSelectQuery *>(child.get()))
                getAggregates(child, actions);
    }
}


void ExpressionAnalyzer::assertNoAggregates(const ASTPtr & ast, const char * description)
{
    const ASTFunction * node = typeid_cast<const ASTFunction *>(ast.get());

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        throw Exception("Aggregate function " + node->getColumnName()
            + " is found " + String(description) + " in query", ErrorCodes::ILLEGAL_AGGREGATION);

    for (const auto & child : ast->children)
        if (!typeid_cast<const ASTSubquery *>(child.get())
            && !typeid_cast<const ASTSelectQuery *>(child.get()))
            assertNoAggregates(child, description);
}


void ExpressionAnalyzer::assertSelect() const
{
    if (!select_query)
        throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::assertAggregation() const
{
    if (!has_aggregation)
        throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const
{
    if (chain.steps.empty())
    {
        chain.steps.emplace_back(std::make_shared<ExpressionActions>(columns, context));
    }
}

/// "Big" ARRAY JOIN.
void ExpressionAnalyzer::addMultipleArrayJoinAction(ExpressionActionsPtr & actions) const
{
    NameSet result_columns;
    for (const auto & result_source : array_join_result_to_source)
    {
        /// Assign new names to columns, if needed.
        if (result_source.first != result_source.second)
            actions->add(ExpressionAction::copyColumn(result_source.second, result_source.first));

        /// Make ARRAY JOIN (replace arrays with their insides) for the columns in these new names.
        result_columns.insert(result_source.first);
    }

    actions->add(ExpressionAction::arrayJoin(result_columns, select_query->array_join_is_left(), context));
}

bool ExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->array_join_expression_list())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->array_join_expression_list(), only_types, step.actions);

    addMultipleArrayJoinAction(step.actions);

    return true;
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions, bool only_types) const
{
    if (only_types)
        actions->add(ExpressionAction::ordinaryJoin(nullptr, analyzed_join.key_names_left,
                                                    analyzed_join.getColumnsAddedByJoin()));
    else
        for (auto & subquery_for_set : subqueries_for_sets)
            if (subquery_for_set.second.join)
                actions->add(ExpressionAction::ordinaryJoin(subquery_for_set.second.join, analyzed_join.key_names_left,
                                                            analyzed_join.getColumnsAddedByJoin()));
}


void ExpressionAnalyzer::AnalyzedJoin::createJoinedBlockActions(const NamesAndTypesList & source_columns,
                                                                const ASTSelectQuery * select_query_with_join,
                                                                const Context & context)
{
    if (!select_query_with_join)
        return;

    const ASTTablesInSelectQueryElement * join = select_query_with_join->join();

    if (!join)
        return;

    const auto & join_params = static_cast<const ASTTableJoin &>(*join->table_join);

    /// Create custom expression list with join keys from right table.
    auto expression_list = std::make_shared<ASTExpressionList>();
    ASTs & children = expression_list->children;

    if (join_params.on_expression)
        for (const auto & join_right_key : key_asts_right)
            children.emplace_back(join_right_key);

    NameSet required_columns_set(key_names_right.begin(), key_names_right.end());
    for (const auto & joined_column : columns_added_by_join)
        required_columns_set.insert(joined_column.name_and_type.name);
    Names required_columns(required_columns_set.begin(), required_columns_set.end());

    const auto & columns_from_joined_table = getColumnsFromJoinedTable(source_columns, context, select_query_with_join);
    NamesAndTypesList source_column_names;
    for (auto & column : columns_from_joined_table)
        source_column_names.emplace_back(column.name_and_type);

    ExpressionAnalyzer analyzer(expression_list, context, nullptr, source_column_names, required_columns);
    joined_block_actions = analyzer.getActions(false);

    auto required_action_columns = joined_block_actions->getRequiredColumns();
    required_columns_from_joined_table.insert(required_action_columns.begin(), required_action_columns.end());
    auto sample = joined_block_actions->getSampleBlock();

    for (auto & column : key_names_right)
        if (!sample.has(column))
            required_columns_from_joined_table.insert(column);

    for (auto & column : columns_added_by_join)
        if (!sample.has(column.name_and_type.name))
            required_columns_from_joined_table.insert(column.name_and_type.name);
}


NamesAndTypesList ExpressionAnalyzer::AnalyzedJoin::getColumnsAddedByJoin() const
{
    NamesAndTypesList result;
    for (const auto & joined_column : columns_added_by_join)
        result.push_back(joined_column.name_and_type);

    return result;
}

const ExpressionAnalyzer::AnalyzedJoin::JoinedColumnsList & ExpressionAnalyzer::AnalyzedJoin::getColumnsFromJoinedTable(
    const NamesAndTypesList & source_columns, const Context & context, const ASTSelectQuery * select_query_with_join)
{
    if (select_query_with_join && columns_from_joined_table.empty())
    {
        if (const ASTTablesInSelectQueryElement * node = select_query_with_join->join())
        {
            const auto & table_expression = static_cast<const ASTTableExpression &>(*node->table_expression);
            auto table_name_with_alias = getTableNameWithAliasFromTableExpression(table_expression, context.getCurrentDatabase());

            auto columns = getNamesAndTypeListFromTableExpression(table_expression, context);

            for (auto & column : columns)
            {
                columns_from_joined_table.emplace_back(column, column.name);

                if (source_columns.contains(column.name))
                {
                    auto qualified_name = table_name_with_alias.getQualifiedNamePrefix() + column.name;
                    columns_from_joined_table.back().name_and_type.name = qualified_name;
                }
            }
        }
    }

    return columns_from_joined_table;
}


bool ExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->join())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    const auto & join_element = static_cast<const ASTTablesInSelectQueryElement &>(*select_query->join());
    auto & join_params = static_cast<ASTTableJoin &>(*join_element.table_join);

    if (join_params.strictness == ASTTableJoin::Strictness::Unspecified && join_params.kind != ASTTableJoin::Kind::Cross)
    {
        if (settings.join_default_strictness == "ANY")
            join_params.strictness = ASTTableJoin::Strictness::Any;
        else if (settings.join_default_strictness == "ALL")
            join_params.strictness = ASTTableJoin::Strictness::All;
        else
            throw Exception("Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty", DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
    }

    const auto & table_to_join = static_cast<const ASTTableExpression &>(*join_element.table_expression);

    getActionsFromJoinKeys(join_params, only_types, step.actions);

    /// Two JOINs are not supported with the same subquery, but different USINGs.
    auto join_hash = join_element.getTreeHash();

    SubqueryForSet & subquery_for_set = subqueries_for_sets[toString(join_hash.first) + "_" + toString(join_hash.second)];

    /// Special case - if table name is specified on the right of JOIN, then the table has the type Join (the previously prepared mapping).
    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        const auto & identifier = static_cast<const ASTIdentifier &>(*table_to_join.database_and_table_name);
        auto database_table = getDatabaseAndTableNameFromIdentifier(identifier);
        StoragePtr table = context.tryGetTable(database_table.first, database_table.second);

        if (table)
        {
            StorageJoin * storage_join = dynamic_cast<StorageJoin *>(table.get());

            if (storage_join)
            {
                storage_join->assertCompatible(join_params.kind, join_params.strictness);
                /// TODO Check the set of keys.

                JoinPtr & join = storage_join->getJoin();
                subquery_for_set.join = join;
            }
        }
    }

    if (!subquery_for_set.join)
    {
        JoinPtr join = std::make_shared<Join>(
            analyzed_join.key_names_left, analyzed_join.key_names_right, analyzed_join.columns_added_by_join_from_right_keys,
            settings.join_use_nulls, settings.size_limits_for_join,
            join_params.kind, join_params.strictness);

        /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
          * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
          *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
          * - this function shows the expression JOIN _data1.
          */
        if (!subquery_for_set.source)
        {
            ASTPtr table;

            if (table_to_join.subquery)
                table = table_to_join.subquery;
            else if (table_to_join.table_function)
                table = table_to_join.table_function;
            else if (table_to_join.database_and_table_name)
                table = table_to_join.database_and_table_name;

            Names original_columns;
            for (const auto & column : analyzed_join.getColumnsFromJoinedTable(source_columns, context, select_query))
                if (analyzed_join.required_columns_from_joined_table.count(column.name_and_type.name))
                    original_columns.emplace_back(column.original_name);

            auto interpreter = interpretSubquery(table, context, subquery_depth, original_columns);
            subquery_for_set.source = std::make_shared<LazyBlockInputStream>(
                interpreter->getSampleBlock(),
                [interpreter]() mutable { return interpreter->execute().in; });
        }

        /// Alias duplicating columns as qualified.
        for (const auto & column : analyzed_join.getColumnsFromJoinedTable(source_columns, context, select_query))
            if (analyzed_join.required_columns_from_joined_table.count(column.name_and_type.name))
                subquery_for_set.joined_block_aliases.emplace_back(column.original_name, column.name_and_type.name);

        auto sample_block = subquery_for_set.source->getHeader();
        for (const auto & name_with_alias : subquery_for_set.joined_block_aliases)
        {
            if (sample_block.has(name_with_alias.first))
            {
                auto pos = sample_block.getPositionByName(name_with_alias.first);
                auto column = sample_block.getByPosition(pos);
                sample_block.erase(pos);
                column.name = name_with_alias.second;
                sample_block.insert(std::move(column));
            }
        }

        analyzed_join.joined_block_actions->execute(sample_block);

        /// TODO You do not need to set this up when JOIN is only needed on remote servers.
        subquery_for_set.join = join;
        subquery_for_set.join->setSampleBlock(sample_block);
        subquery_for_set.joined_block_actions = analyzed_join.joined_block_actions;
    }

    addJoinAction(step.actions, false);

    return true;
}

bool ExpressionAnalyzer::appendPrewhere(ExpressionActionsChain & chain, bool only_types,
                                        const ASTPtr & sampling_expression, const ASTPtr & primary_expression)
{
    assertSelect();

    if (!select_query->prewhere_expression)
        return false;

    Names additional_required_mergetree_columns;
    if (sampling_expression)
        additional_required_mergetree_columns = ExpressionAnalyzer(sampling_expression, context, storage).getRequiredSourceColumns();
    if (primary_expression)
    {
        auto required_primary_columns = ExpressionAnalyzer(primary_expression, context, storage).getRequiredSourceColumns();
        additional_required_mergetree_columns.insert(additional_required_mergetree_columns.end(),
                                                     required_primary_columns.begin(), required_primary_columns.end());
    }

    initChain(chain, source_columns);
    auto & step = chain.getLastStep();
    getRootActions(select_query->prewhere_expression, only_types, step.actions);
    String prewhere_column_name = select_query->prewhere_expression->getColumnName();
    step.required_output.push_back(prewhere_column_name);
    step.can_remove_required_output.push_back(true);

    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions = std::make_shared<ExpressionActions>(source_columns, context);
        getRootActions(select_query->prewhere_expression, only_types, tmp_actions);
        tmp_actions->finalize({prewhere_column_name});
        auto required_columns = tmp_actions->getRequiredColumns();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());

        /// Add required columns to required output in order not to remove them after prewhere execution.
        /// TODO: add sampling and final execution to common chain.
        for (const auto & column : additional_required_mergetree_columns)
        {
            if (required_source_columns.count(column))
            {
                step.required_output.push_back(column);
                step.can_remove_required_output.push_back(true);
            }
        }

        auto names = step.actions->getSampleBlock().getNames();
        NameSet name_set(names.begin(), names.end());

        for (const auto & column : source_columns)
            if (required_source_columns.count(column.name) == 0)
                name_set.erase(column.name);

        Names required_output(name_set.begin(), name_set.end());
        step.actions->finalize(required_output);
    }

    {
        /// Add empty action with input = {prewhere actions output} + {unused source columns}
        /// Reasons:
        /// 1. Remove remove source columns which are used only in prewhere actions during prewhere actions execution.
        ///    Example: select A prewhere B > 0. B can be removed at prewhere step.
        /// 2. Store side columns which were calculated during prewhere actions execution if they are used.
        ///    Example: select F(A) prewhere F(A) > 0. F(A) can be saved from prewhere step.
        /// 3. Check if we can remove filter column at prewhere step. If we can, action will store single REMOVE_COLUMN.
        ColumnsWithTypeAndName columns = step.actions->getSampleBlock().getColumnsWithTypeAndName();
        auto required_columns = step.actions->getRequiredColumns();
        NameSet prewhere_input_names(required_columns.begin(), required_columns.end());
        NameSet unused_source_columns;

        for (const auto & column : source_columns)
        {
            if (prewhere_input_names.count(column.name) == 0)
            {
                columns.emplace_back(column.type, column.name);
                unused_source_columns.emplace(column.name);
            }
        }

        chain.steps.emplace_back(std::make_shared<ExpressionActions>(std::move(columns), context));
        chain.steps.back().additional_input = std::move(unused_source_columns);
    }

    return true;
}

bool ExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->where_expression)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->where_expression->getColumnName());
    step.can_remove_required_output = {true};

    getRootActions(select_query->where_expression, only_types, step.actions);

    return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->group_expression_list)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    ASTs asts = select_query->group_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        step.required_output.push_back(asts[i]->getColumnName());
        getRootActions(asts[i], only_types, step.actions);
    }

    return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
    {
        for (size_t j = 0; j < aggregate_descriptions[i].argument_names.size(); ++j)
        {
            step.required_output.push_back(aggregate_descriptions[i].argument_names[j]);
        }
    }

    getActionsBeforeAggregation(select_query->select_expression_list, step.actions, only_types);

    if (select_query->having_expression)
        getActionsBeforeAggregation(select_query->having_expression, step.actions, only_types);

    if (select_query->order_expression_list)
        getActionsBeforeAggregation(select_query->order_expression_list, step.actions, only_types);
}

bool ExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->having_expression)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->having_expression->getColumnName());
    getRootActions(select_query->having_expression, only_types, step.actions);

    return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->select_expression_list, only_types, step.actions);

    for (const auto & child : select_query->select_expression_list->children)
        step.required_output.push_back(child->getColumnName());
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->order_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->order_expression_list, only_types, step.actions);

    ASTs asts = select_query->order_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        ASTOrderByElement * ast = typeid_cast<ASTOrderByElement *>(asts[i].get());
        if (!ast || ast->children.size() < 1)
            throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
        ASTPtr order_expression = ast->children.at(0);
        step.required_output.push_back(order_expression->getColumnName());
    }

    return true;
}

bool ExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->limit_by_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->limit_by_expression_list, only_types, step.actions);

    for (const auto & child : select_query->limit_by_expression_list->children)
        step.required_output.push_back(child->getColumnName());

    return true;
}

void ExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    NamesWithAliases result_columns;

    ASTs asts = select_query->select_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        String result_name = asts[i]->getAliasOrColumnName();
        if (required_result_columns.empty()
            || std::find(required_result_columns.begin(), required_result_columns.end(), result_name) !=  required_result_columns.end())
        {
            result_columns.emplace_back(asts[i]->getColumnName(), result_name);
            step.required_output.push_back(result_columns.back().second);
        }
    }

    step.actions->add(ExpressionAction::project(result_columns));
}


void ExpressionAnalyzer::appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types)
{
    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();
    getRootActions(expr, only_types, step.actions);
    step.required_output.push_back(expr->getColumnName());
}


void ExpressionAnalyzer::getActionsBeforeAggregation(const ASTPtr & ast, ExpressionActionsPtr & actions, bool no_subqueries)
{
    ASTFunction * node = typeid_cast<ASTFunction *>(ast.get());

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        for (auto & argument : node->arguments->children)
            getRootActions(argument, no_subqueries, actions);
    else
        for (auto & child : ast->children)
            getActionsBeforeAggregation(child, actions, no_subqueries);
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result)
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(source_columns, context);
    NamesWithAliases result_columns;
    Names result_names;

    ASTs asts;

    if (auto node = typeid_cast<const ASTExpressionList *>(query.get()))
        asts = node->children;
    else
        asts = ASTs(1, query);

    for (size_t i = 0; i < asts.size(); ++i)
    {
        std::string name = asts[i]->getColumnName();
        std::string alias;
        if (add_aliases)
            alias = asts[i]->getAliasOrColumnName();
        else
            alias = name;
        result_columns.emplace_back(name, alias);
        result_names.push_back(alias);
        getRootActions(asts[i], false, actions);
    }

    if (add_aliases)
    {
        if (project_result)
            actions->add(ExpressionAction::project(result_columns));
        else
            actions->add(ExpressionAction::addAliases(result_columns));
    }

    if (!(add_aliases && project_result))
    {
        /// We will not delete the original columns.
        for (const auto & column_name_type : source_columns)
            result_names.push_back(column_name_type.name);
    }

    actions->finalize(result_names);

    return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(NamesAndTypesList(), context);

    getRootActions(query, true, actions, true);
    return actions;
}

void ExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const
{
    for (const auto & name_and_type : aggregation_keys)
        key_names.emplace_back(name_and_type.name);

    aggregates = aggregate_descriptions;
}

void ExpressionAnalyzer::collectUsedColumns()
{
    /** Calculate which columns are required to execute the expression.
      * Then, delete all other columns from the list of available columns.
      * After execution, columns will only contain the list of columns needed to read from the table.
      */

    NameSet required;
    NameSet ignored;

    NameSet available_columns;
    for (const auto & column : source_columns)
        available_columns.insert(column.name);

    if (select_query && select_query->array_join_expression_list())
    {
        ASTs & expressions = select_query->array_join_expression_list()->children;
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            /// Ignore the top-level identifiers from the ARRAY JOIN section.
            /// Then add them separately.
            if (typeid_cast<ASTIdentifier *>(expressions[i].get()))
            {
                ignored.insert(expressions[i]->getColumnName());
            }
            else
            {
                /// Nothing needs to be ignored for expressions in ARRAY JOIN.
                NameSet empty;
                RequiredSourceColumnsVisitor visitor(available_columns, required, empty, empty, empty);
                visitor.visit(expressions[i]);
            }

            ignored.insert(expressions[i]->getAliasOrColumnName());
        }
    }

    /** You also need to ignore the identifiers of the columns that are obtained by JOIN.
      * (Do not assume that they are required for reading from the "left" table).
      */
    NameSet available_joined_columns;
    collectJoinedColumns(available_joined_columns);

    NameSet required_joined_columns;

    for (const auto & left_key_ast : analyzed_join.key_asts_left)
    {
        NameSet empty;
        RequiredSourceColumnsVisitor columns_visitor(available_columns, required, ignored, empty, required_joined_columns);
        columns_visitor.visit(left_key_ast);
    }

    RequiredSourceColumnsVisitor columns_visitor(available_columns, required, ignored, available_joined_columns, required_joined_columns);
    columns_visitor.visit(query);

    for (auto it = analyzed_join.columns_added_by_join.begin(); it != analyzed_join.columns_added_by_join.end();)
    {
        if (required_joined_columns.count(it->name_and_type.name))
            ++it;
        else
            analyzed_join.columns_added_by_join.erase(it++);
    }

    analyzed_join.createJoinedBlockActions(source_columns, select_query, context);

    /// Some columns from right join key may be used in query. This columns will be appended to block during join.
    for (const auto & right_key_name : analyzed_join.key_names_right)
        if (required_joined_columns.count(right_key_name))
            analyzed_join.columns_added_by_join_from_right_keys.insert(right_key_name);

    /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
    NameSet array_join_sources;
    for (const auto & result_source : array_join_result_to_source)
        array_join_sources.insert(result_source.second);

    for (const auto & column_name_type : source_columns)
        if (array_join_sources.count(column_name_type.name))
            required.insert(column_name_type.name);

    /// You need to read at least one column to find the number of rows.
    if (select_query && required.empty())
        required.insert(ExpressionActions::getSmallestColumn(source_columns));

    NameSet unknown_required_source_columns = required;

    for (NamesAndTypesList::iterator it = source_columns.begin(); it != source_columns.end();)
    {
        unknown_required_source_columns.erase(it->name);

        if (!required.count(it->name))
            source_columns.erase(it++);
        else
            ++it;
    }

    /// If there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing they are also considered.
    if (storage)
    {
        for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
        {
            if (storage->hasColumn(*it))
            {
                source_columns.push_back(storage->getColumn(*it));
                unknown_required_source_columns.erase(it++);
            }
            else
                ++it;
        }
    }

    if (!unknown_required_source_columns.empty())
        throw Exception("Unknown identifier: " + *unknown_required_source_columns.begin(), ErrorCodes::UNKNOWN_IDENTIFIER);
}


void ExpressionAnalyzer::collectJoinedColumnsFromJoinOnExpr()
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

    auto left_source_names = getTableNameWithAliasFromTableExpression(left_table_expression, context.getCurrentDatabase());
    auto right_source_names = getTableNameWithAliasFromTableExpression(right_table_expression, context.getCurrentDatabase());

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

                if (right_table && source_columns.contains(ast->getColumnName()))
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

void ExpressionAnalyzer::collectJoinedColumns(NameSet & joined_columns)
{
    if (!select_query)
        return;

    const ASTTablesInSelectQueryElement * node = select_query->join();

    if (!node)
        return;

    const auto & table_join = static_cast<const ASTTableJoin &>(*node->table_join);
    const auto & table_expression = static_cast<const ASTTableExpression &>(*node->table_expression);
    auto joined_table_name = getTableNameWithAliasFromTableExpression(table_expression, context.getCurrentDatabase());

    auto add_name_to_join_keys = [&](Names & join_keys, ASTs & join_asts, const ASTPtr & ast, bool right_table)
    {
        String name;
        if (right_table)
        {
            name = ast->getAliasOrColumnName();
            if (source_columns.contains(name))
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
        collectJoinedColumnsFromJoinOnExpr();

    /// When we use JOIN ON syntax, non_joined_columns are columns from join_key_names_left,
    ///     because even if a column from join_key_names_right, we may need to join it if it has different name.
    /// If we use USING syntax, join_key_names_left and join_key_names_right are almost the same, but we need to use
    ///     join_key_names_right in order to support aliases in USING list. Example:
    ///     SELECT x FROM tab1 ANY LEFT JOIN tab2 USING (x as y) - will join column x from tab1 with column y from tab2.
    //auto & not_joined_columns = table_join.using_expression_list ? analyzed_join.key_names_right : analyzed_join.key_names_left;
    auto & columns_from_joined_table = analyzed_join.getColumnsFromJoinedTable(source_columns, context, select_query);

    for (auto & column : columns_from_joined_table)
    {
        auto & column_name = column.name_and_type.name;
        auto & column_type = column.name_and_type.type;
        auto & original_name = column.original_name;
        //if (table_join.on_expression
        //    || not_joined_columns.end() == std::find(not_joined_columns.begin(), not_joined_columns.end(), column_name))
        {
            if (joined_columns.count(column_name)) /// Duplicate columns in the subquery for JOIN do not make sense.
                continue;

            joined_columns.insert(column_name);

            bool make_nullable = settings.join_use_nulls && (table_join.kind == ASTTableJoin::Kind::Left ||
                                                             table_join.kind == ASTTableJoin::Kind::Full);
            auto type = make_nullable ? makeNullable(column_type) : column_type;
            analyzed_join.columns_added_by_join.emplace_back(NameAndTypePair(column_name, std::move(type)), original_name);
        }
    }
}


Names ExpressionAnalyzer::getRequiredSourceColumns() const
{
    return source_columns.getNames();
}


static bool hasArrayJoin(const ASTPtr & ast)
{
    if (const ASTFunction * function = typeid_cast<const ASTFunction *>(&*ast))
        if (function->name == "arrayJoin")
            return true;

    for (const auto & child : ast->children)
        if (!typeid_cast<ASTSelectQuery *>(child.get()) && hasArrayJoin(child))
            return true;

    return false;
}


void ExpressionAnalyzer::removeUnneededColumnsFromSelectClause()
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

}
