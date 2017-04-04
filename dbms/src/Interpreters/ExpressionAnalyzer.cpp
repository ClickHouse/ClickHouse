#include <Poco/Util/Application.h>
#include <Poco/String.h>

#include <DataTypes/FieldToDataType.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSet.h>
#include <Parsers/ASTOrderByElement.h>

#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeExpression.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnExpression.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Set.h>
#include <Interpreters/Join.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageSet.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Dictionaries/IDictionary.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils.h>

#include <Parsers/formatAST.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <ext/range.hpp>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int CYCLIC_ALIASES;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int TOO_MUCH_ROWS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_ELEMENT_OF_SET;
    extern const int ALIAS_REQUIRED;
    extern const int EMPTY_NESTED_TABLE;
    extern const int NOT_AN_AGGREGATE;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
    extern const int DUPLICATE_COLUMN;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int ILLEGAL_AGGREGATION;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_DEEP_AST;
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

bool functionIsInOperator(const String & name)
{
    return name == "in" || name == "notIn";
}

bool functionIsInOrGlobalInOperator(const String & name)
{
    return name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn";
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

}


ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & ast_,
    const Context & context_,
    StoragePtr storage_,
    const NamesAndTypesList & columns_,
    size_t subquery_depth_,
    bool do_global_)
    : ast(ast_), context(context_), settings(context.getSettings()),
    subquery_depth(subquery_depth_), columns(columns_),
    storage(storage_ ? storage_ : getTable()),
    do_global(do_global_)
{
    init();
}


void ExpressionAnalyzer::init()
{
    removeDuplicateColumns(columns);

    select_query = typeid_cast<ASTSelectQuery *>(ast.get());

    /// Depending on the user's profile, check for the execution rights
    /// distributed subqueries inside the IN or JOIN sections and process these subqueries.
    InJoinSubqueriesPreprocessor(context).process(select_query);

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, settings).perform();

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    addASTAliases(ast);

    /// Common subexpression elimination. Rewrite rules.
    normalizeTree();

    /// ALIAS columns should not be substituted for ASTAsterisk, we will add them now, after normalizeTree.
    addAliasColumns();

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries();

    /// Optimize if with constant condition after constats are substituted instead of sclalar subqueries
    optimizeIfWithConstantCondition();

    /// GROUP BY injective function elimination.
    optimizeGroupBy();

    /// Remove duplicate items from ORDER BY.
    optimizeOrderBy();

    // Remove duplicated elements from LIMIT BY clause.
    optimizeLimitBy();

    /// array_join_alias_to_name, array_join_result_to_source.
    getArrayJoinedColumns();

    /// Delete the unnecessary from `columns` list. Create `unknown_required_columns`. Form `columns_added_by_join`.
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

void ExpressionAnalyzer::optimizeIfWithConstantCondition()
{
    optimizeIfWithConstantConditionImpl(ast, aliases);
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

void ExpressionAnalyzer::optimizeIfWithConstantConditionImpl(ASTPtr & current_ast, ExpressionAnalyzer::Aliases & aliases) const
{
    if (!current_ast)
        return;

    for (ASTPtr & child : current_ast->children)
    {
        ASTFunction * function_node = typeid_cast<ASTFunction *>(child.get());
        if (!function_node || function_node->name != "if")
        {
            optimizeIfWithConstantConditionImpl(child, aliases);
            continue;
        }

        optimizeIfWithConstantConditionImpl(function_node->arguments, aliases);
        ASTExpressionList * args = typeid_cast<ASTExpressionList *>(function_node->arguments.get());

        ASTPtr condition_expr = args->children.at(0);
        ASTPtr then_expr = args->children.at(1);
        ASTPtr else_expr = args->children.at(2);


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

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(columns, settings);

    if (select_query && select_query->array_join_expression_list())
    {
        getRootActions(select_query->array_join_expression_list(), true, false, temp_actions);
        addMultipleArrayJoinAction(temp_actions);
    }

    if (select_query)
    {
        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            if (static_cast<const ASTTableJoin &>(*join->table_join).using_expression_list)
                getRootActions(static_cast<const ASTTableJoin &>(*join->table_join).using_expression_list, true, false, temp_actions);

            addJoinAction(temp_actions, true);
        }
    }

    getAggregates(ast, temp_actions);

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
                getRootActions(group_asts[i], true, false, temp_actions);

                const auto & column_name = group_asts[i]->getColumnName();
                const auto & block = temp_actions->getSampleBlock();

                if (!block.has(column_name))
                    throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                const auto & col = block.getByName(column_name);

                /// Constant expressions have non-null column pointer at this stage.
                if (const auto is_constexpr = col.column)
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
        aggregated_columns = temp_actions->getSampleBlock().getColumnsList();
    }
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables()
{
    /// Adds existing external tables (not subqueries) to the external_tables dictionary.
    findExternalTables(ast);

    /// Converts GLOBAL subqueries to external tables; Puts them into the external_tables dictionary: name -> StoragePtr.
    initGlobalSubqueries(ast);
}


void ExpressionAnalyzer::initGlobalSubqueries(ASTPtr & ast)
{
    /// Recursive calls. We do not go into subqueries.

    for (auto & child : ast->children)
        if (!typeid_cast<ASTSelectQuery *>(child.get()))
            initGlobalSubqueries(child);

    /// Bottom-up actions.

    if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        /// For GLOBAL IN.
        if (do_global && (node->name == "globalIn" || node->name == "globalNotIn"))
            addExternalStorage(node->arguments->children.at(1));
    }
    else if (ASTTablesInSelectQueryElement * node = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        /// For GLOBAL JOIN.
        if (do_global && node->table_join
            && static_cast<const ASTTableJoin &>(*node->table_join).locality == ASTTableJoin::Locality::Global)
            addExternalStorage(node->table_expression);
    }
}


void ExpressionAnalyzer::findExternalTables(ASTPtr & ast)
{
    /// Traverse from the bottom. Intentionally go into subqueries.
    for (auto & child : ast->children)
        findExternalTables(child);

    /// If table type identifier
    StoragePtr external_storage;

    if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
        if (node->kind == ASTIdentifier::Table)
            if ((external_storage = context.tryGetExternalTable(node->name)))
                external_tables[node->name] = external_storage;
}


static std::shared_ptr<InterpreterSelectQuery> interpretSubquery(
    ASTPtr & subquery_or_table_name, const Context & context, size_t subquery_depth, const Names & required_columns);


void ExpressionAnalyzer::addExternalStorage(ASTPtr & subquery_or_table_name_or_table_expression)
{
    /// With nondistributed queries, creating temporary tables does not make sense.
    if (!(storage && storage->isRemote()))
        return;

    ASTPtr subquery;
    ASTPtr table_name;
    ASTPtr subquery_or_table_name;

    if (typeid_cast<const ASTIdentifier *>(subquery_or_table_name_or_table_expression.get()))
    {
        table_name = subquery_or_table_name_or_table_expression;
        subquery_or_table_name = table_name;
    }
    else if (auto ast_table_expr = typeid_cast<const ASTTableExpression *>(subquery_or_table_name_or_table_expression.get()))
    {
        if (ast_table_expr->database_and_table_name)
        {
            table_name = ast_table_expr->database_and_table_name;
            subquery_or_table_name = table_name;
        }
        else if (ast_table_expr->subquery)
        {
            subquery = ast_table_expr->subquery;
            subquery_or_table_name = subquery;
        }
    }
    else if (typeid_cast<const ASTSubquery *>(subquery_or_table_name_or_table_expression.get()))
    {
        subquery = subquery_or_table_name_or_table_expression;
        subquery_or_table_name = subquery;
    }

    if (!subquery_or_table_name)
        throw Exception("Logical error: unknown AST element passed to ExpressionAnalyzer::addExternalStorage method", ErrorCodes::LOGICAL_ERROR);

    if (table_name)
    {
        /// If this is already an external table, you do not need to add anything. Just remember its presence.
        if (external_tables.end() != external_tables.find(static_cast<const ASTIdentifier &>(*table_name).name))
            return;
    }

    /// Generate the name for the external table.
    String external_table_name = "_data" + toString(external_table_id);
    while (external_tables.count(external_table_name))
    {
        ++external_table_id;
        external_table_name = "_data" + toString(external_table_id);
    }

    auto interpreter = interpretSubquery(subquery_or_table_name, context, subquery_depth, {});

    Block sample = interpreter->getSampleBlock();
    NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>(sample.getColumnsList());

    StoragePtr external_storage = StorageMemory::create(external_table_name, columns);

    /** There are two ways to perform distributed GLOBAL subqueries.
      *
      * "push" method:
      * Subquery data is sent to all remote servers, where they are then used.
      * For this method, the data is sent in the form of "external tables" and will be available on each remote server by the name of the type _data1.
      * Replace in the query a subquery for this name.
      *
      * "pull" method:
      * Remote servers download the subquery data from the request initiating server.
      * For this method, replace the subquery with another subquery of the form (SELECT * FROM remote ('host: port', _query_QUERY_ID, _data1))
      * This subquery, in fact, says - "you need to download data from there."
      *
      * The "pull" method takes precedence, because in it a remote server can decide that it does not need data and does not download it in such cases.
      */

    if (settings.global_subqueries_method == GlobalSubqueriesMethod::PUSH)
    {
        /** We replace the subquery with the name of the temporary table.
          * It is in this form, the request will go to the remote server.
          * This temporary table will go to the remote server, and on its side,
          *  instead of doing a subquery, you just need to read it.
          */

        auto database_and_table_name = std::make_shared<ASTIdentifier>(StringRange(), external_table_name, ASTIdentifier::Table);

        if (auto ast_table_expr = typeid_cast<ASTTableExpression *>(subquery_or_table_name_or_table_expression.get()))
        {
            ast_table_expr->subquery.reset();
            ast_table_expr->database_and_table_name = database_and_table_name;

            ast_table_expr->children.clear();
            ast_table_expr->children.emplace_back(database_and_table_name);
        }
        else
            subquery_or_table_name_or_table_expression = database_and_table_name;
    }
    else if (settings.global_subqueries_method == GlobalSubqueriesMethod::PULL)
    {
        throw Exception("Support for 'pull' method of execution of global subqueries is disabled.", ErrorCodes::SUPPORT_IS_DISABLED);

        /// TODO
/*        String host_port = getFQDNOrHostName() + ":" + toString(context.getTCPPort());
        String database = "_query_" + context.getCurrentQueryId();

        auto subquery = std::make_shared<ASTSubquery>();
        subquery_or_table_name = subquery;

        auto select = std::make_shared<ASTSelectQuery>();
        subquery->children.push_back(select);

        auto exp_list = std::make_shared<ASTExpressionList>();
        select->select_expression_list = exp_list;
        select->children.push_back(select->select_expression_list);

        Names column_names = external_storage->getColumnNamesList();
        for (const auto & name : column_names)
            exp_list->children.push_back(std::make_shared<ASTIdentifier>(StringRange(), name));

        auto table_func = std::make_shared<ASTFunction>();
        select->table = table_func;
        select->children.push_back(select->table);

        table_func->name = "remote";
        auto args = std::make_shared<ASTExpressionList>();
        table_func->arguments = args;
        table_func->children.push_back(table_func->arguments);

        auto address_lit = std::make_shared<ASTLiteral>(StringRange(), host_port);
        args->children.push_back(address_lit);

        auto database_lit = std::make_shared<ASTLiteral>(StringRange(), database);
        args->children.push_back(database_lit);

        auto table_lit = std::make_shared<ASTLiteral>(StringRange(), external_table_name);
        args->children.push_back(table_lit);*/
    }
    else
        throw Exception("Unknown global subqueries execution method", ErrorCodes::UNKNOWN_GLOBAL_SUBQUERIES_METHOD);

    external_tables[external_table_name] = external_storage;
    subqueries_for_sets[external_table_name].source = interpreter->execute().in;
    subqueries_for_sets[external_table_name].source_sample = interpreter->getSampleBlock();
    subqueries_for_sets[external_table_name].table = external_storage;

    /** NOTE If it was written IN tmp_table - the existing temporary (but not external) table,
      *  then a new temporary table will be created (for example, _data1),
      *  and the data will then be copied to it.
      * Maybe this can be avoided.
      */
}


NamesAndTypesList::iterator ExpressionAnalyzer::findColumn(const String & name, NamesAndTypesList & cols)
{
    return std::find_if(cols.begin(), cols.end(),
        [&](const NamesAndTypesList::value_type & val) { return val.name == name; });
}


/// ignore_levels - aliases in how many upper levels of the subtree should be ignored.
/// For example, with ignore_levels=1 ast can not be put in the dictionary, but its children can.
void ExpressionAnalyzer::addASTAliases(ASTPtr & ast, int ignore_levels)
{
    /// Bottom-up traversal. We do not go into subqueries.
    for (auto & child : ast->children)
    {
        int new_ignore_levels = std::max(0, ignore_levels - 1);

        /// The top-level aliases in the ARRAY JOIN section have a special meaning, we will not add them
        ///  (skip the expression list itself and its children).
        if (typeid_cast<ASTArrayJoin *>(ast.get()))
            new_ignore_levels = 3;

        /// Don't descent into UNION ALL, table functions and subqueries.
        if (!typeid_cast<ASTTableExpression *>(child.get())
            && !typeid_cast<ASTSelectQuery *>(child.get()))
            addASTAliases(child, new_ignore_levels);
    }

    if (ignore_levels > 0)
        return;

    String alias = ast->tryGetAlias();
    if (!alias.empty())
    {
        if (aliases.count(alias) && ast->getTreeHash() != aliases[alias]->getTreeHash())
            throw Exception("Different expressions with the same alias " + alias, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

        aliases[alias] = ast;
    }
}


StoragePtr ExpressionAnalyzer::getTable()
{
    if (const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get()))
    {
        auto select_database = select->database();
        auto select_table = select->table();

        if (select_table
            && !typeid_cast<const ASTSelectQuery *>(select_table.get())
            && !typeid_cast<const ASTFunction *>(select_table.get()))
        {
            String database = select_database
                ? typeid_cast<const ASTIdentifier &>(*select_database).name
                : "";
            const String & table = typeid_cast<const ASTIdentifier &>(*select_table).name;
            return context.tryGetTable(database, table);
        }
    }

    return StoragePtr();
}


void ExpressionAnalyzer::normalizeTree()
{
    SetOfASTs tmp_set;
    MapOfASTs tmp_map;
    normalizeTreeImpl(ast, tmp_map, tmp_set, "", 0);
}


/// finished_asts - already processed vertices (and by what they replaced)
/// current_asts - vertices in the current call stack of this method
/// current_alias - the alias referencing to the ancestor of ast (the deepest ancestor with aliases)
void ExpressionAnalyzer::normalizeTreeImpl(
    ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, size_t level)
{
    if (level > settings.limits.max_ast_depth)
        throw Exception("Normalized AST is too deep. Maximum: " + settings.limits.max_ast_depth.toString(), ErrorCodes::TOO_DEEP_AST);

    if (finished_asts.count(ast))
    {
        ast = finished_asts[ast];
        return;
    }

    ASTPtr initial_ast = ast;
    current_asts.insert(initial_ast.get());

    String my_alias = ast->tryGetAlias();
    if (!my_alias.empty())
        current_alias = my_alias;

    /// rewrite rules that act when you go from top to bottom.
    bool replaced = false;

    ASTFunction * func_node = typeid_cast<ASTFunction *>(ast.get());
    if (func_node)
    {
        /** Is there a column in the table whose name fully matches the function entry?
          * For example, in the table there is a column "domain(URL)", and we requested domain(URL).
          */
        String function_string = func_node->getColumnName();
        NamesAndTypesList::const_iterator it = findColumn(function_string);
        if (columns.end() != it)
        {
            ast = std::make_shared<ASTIdentifier>(func_node->range, function_string);
            current_asts.insert(ast.get());
            replaced = true;
        }

        /// `IN t` can be specified, where t is a table, which is equivalent to `IN (SELECT * FROM t)`.
        if (functionIsInOrGlobalInOperator(func_node->name))
            if (ASTIdentifier * right = typeid_cast<ASTIdentifier *>(func_node->arguments->children.at(1).get()))
                right->kind = ASTIdentifier::Table;

        /// Special cases for count function.
        String func_name_lowercase = Poco::toLower(func_node->name);
        if (startsWith(func_name_lowercase, "count"))
        {
            /// Select implementation of countDistinct based on settings.
            /// Important that it is done as query rewrite. It means rewritten query
            ///  will be sent to remote servers during distributed query execution,
            ///  and on all remote servers, function implementation will be same.
            if (endsWith(func_node->name, "Distinct") && func_name_lowercase == "countdistinct")
                func_node->name = settings.count_distinct_implementation;

            /// As special case, treat count(*) as count(), not as count(list of all columns).
            if (func_name_lowercase == "count" && func_node->arguments->children.size() == 1
                && typeid_cast<const ASTAsterisk *>(func_node->arguments->children[0].get()))
            {
                func_node->arguments->children.clear();
            }
        }
    }
    else if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        if (node->kind == ASTIdentifier::Column)
        {
            /// If it is an alias, but not a parent alias (for constructs like "SELECT column + 1 AS column").
            Aliases::const_iterator jt = aliases.find(node->name);
            if (jt != aliases.end() && current_alias != node->name)
            {
                /// Let's replace it with the corresponding tree node.
                if (current_asts.count(jt->second.get()))
                    throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);
                if (!my_alias.empty() && my_alias != jt->second->getAliasOrColumnName())
                {
                    /// In a construct like "a AS b", where a is an alias, you must set alias b to the result of substituting alias a.
                    ast = jt->second->clone();
                    ast->setAlias(my_alias);
                }
                else
                {
                    ast = jt->second;
                }

                replaced = true;
            }
        }
    }
    else if (ASTExpressionList * node = typeid_cast<ASTExpressionList *>(ast.get()))
    {
        /// Replace * with a list of columns.
        ASTs & asts = node->children;
        for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
        {
            if (ASTAsterisk * asterisk = typeid_cast<ASTAsterisk *>(asts[i].get()))
            {
                ASTs all_columns;
                for (const auto & column_name_type : columns)
                    all_columns.emplace_back(std::make_shared<ASTIdentifier>(asterisk->range, column_name_type.name));

                asts.erase(asts.begin() + i);
                asts.insert(asts.begin() + i, all_columns.begin(), all_columns.end());
            }
        }
    }
    else if (ASTTablesInSelectQueryElement * node = typeid_cast<ASTTablesInSelectQueryElement *>(ast.get()))
    {
        if (node->table_expression)
        {
            auto & database_and_table_name = static_cast<ASTTableExpression &>(*node->table_expression).database_and_table_name;
            if (database_and_table_name)
            {
                if (ASTIdentifier * right = typeid_cast<ASTIdentifier *>(database_and_table_name.get()))
                {
                    right->kind = ASTIdentifier::Table;
                }
            }
        }
    }

    /// If we replace the root of the subtree, we will be called again for the new root, in case the alias is replaced by an alias.
    if (replaced)
    {
        normalizeTreeImpl(ast, finished_asts, current_asts, current_alias, level + 1);
        current_asts.erase(initial_ast.get());
        current_asts.erase(ast.get());
        finished_asts[initial_ast] = ast;
        return;
    }

    /// Recurring calls. Don't go into subqueries.
    /// We also do not go to the left argument of lambda expressions, so as not to replace the formal parameters
    ///  on aliases in expressions of the form 123 AS x, arrayMap(x -> 1, [2]).

    if (func_node && func_node->name == "lambda")
    {
        /// We skip the first argument. We also assume that the lambda function can not have parameters.
        for (size_t i = 1, size = func_node->arguments->children.size(); i < size; ++i)
        {
            auto & child = func_node->arguments->children[i];

            if (typeid_cast<const ASTSelectQuery *>(child.get())
                || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            normalizeTreeImpl(child, finished_asts, current_asts, current_alias, level + 1);
        }
    }
    else
    {
        for (auto & child : ast->children)
        {
            if (typeid_cast<const ASTSelectQuery *>(child.get())
                || typeid_cast<const ASTTableExpression *>(child.get()))
                continue;

            normalizeTreeImpl(child, finished_asts, current_asts, current_alias, level + 1);
        }
    }

    /// If the WHERE clause or HAVING consists of a single alias, the reference must be replaced not only in children, but also in where_expression and having_expression.
    if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get()))
    {
        if (select->prewhere_expression)
            normalizeTreeImpl(select->prewhere_expression, finished_asts, current_asts, current_alias, level + 1);
        if (select->where_expression)
            normalizeTreeImpl(select->where_expression, finished_asts, current_asts, current_alias, level + 1);
        if (select->having_expression)
            normalizeTreeImpl(select->having_expression, finished_asts, current_asts, current_alias, level + 1);
    }

    /// Actions to be performed from the bottom up.

    if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->kind == ASTFunction::TABLE_FUNCTION)
        {
        }
        else if (node->name == "lambda")
        {
            node->kind = ASTFunction::LAMBDA_EXPRESSION;
        }
        else if (context.getAggregateFunctionFactory().isAggregateFunctionName(node->name))
        {
            node->kind = ASTFunction::AGGREGATE_FUNCTION;
        }
        else if (node->name == "arrayJoin")
        {
            node->kind = ASTFunction::ARRAY_JOIN;
        }
        else
        {
            node->kind = ASTFunction::FUNCTION;
        }

        if (node->parameters && node->kind != ASTFunction::AGGREGATE_FUNCTION)
            throw Exception("The only parametric functions (functions with two separate parenthesis pairs) are aggregate functions"
                ", and '" + node->name + "' is not an aggregate function.", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);
    }

    current_asts.erase(initial_ast.get());
    current_asts.erase(ast.get());
    finished_asts[initial_ast] = ast;
}


void ExpressionAnalyzer::addAliasColumns()
{
    if (!select_query)
        return;

    if (!storage)
        return;

    columns.insert(std::end(columns), std::begin(storage->alias_columns), std::end(storage->alias_columns));
}


void ExpressionAnalyzer::executeScalarSubqueries()
{
    if (!select_query)
        executeScalarSubqueriesImpl(ast);
    else
    {
        for (auto & child : ast->children)
        {
            /// Do not go to FROM, JOIN, UNION.
            if (!typeid_cast<const ASTTableExpression *>(child.get())
                && child.get() != select_query->next_union_all.get())
            {
                executeScalarSubqueriesImpl(child);
            }
        }
    }
}


static ASTPtr addTypeConversion(std::unique_ptr<ASTLiteral> && ast, const String & type_name)
{
    auto func = std::make_shared<ASTFunction>(ast->range);
    ASTPtr res = func;
    func->alias = ast->alias;
    ast->alias.clear();
    func->kind = ASTFunction::FUNCTION;
    func->name = "CAST";
    auto exp_list = std::make_shared<ASTExpressionList>(ast->range);
    func->arguments = exp_list;
    func->children.push_back(func->arguments);
    exp_list->children.emplace_back(ast.release());
    exp_list->children.emplace_back(std::make_shared<ASTLiteral>(StringRange(), type_name));
    return res;
}


void ExpressionAnalyzer::executeScalarSubqueriesImpl(ASTPtr & ast)
{
    /** Replace subqueries that return exactly one row
      * ("scalar" subqueries) to the corresponding constants.
      *
      * If the subquery returns more than one column, it is replaced by a tuple of constants.
      *
      * Features
      *
      * A replacement occurs during query analysis, and not during the main runtime.
      * This means that the progress indicator will not work during the execution of these requests,
      *  and also such queries can not be aborted.
      *
      * But the query result can be used for the index in the table.
      *
      * Scalar subqueries are executed on the request-initializer server.
      * The request is sent to remote servers with already substituted constants.
      */

    if (ASTSubquery * subquery = typeid_cast<ASTSubquery *>(ast.get()))
    {
        Context subquery_context = context;
        Settings subquery_settings = context.getSettings();
        subquery_settings.limits.max_result_rows = 1;
        subquery_settings.extremes = 0;
        subquery_context.setSettings(subquery_settings);

        ASTPtr query = subquery->children.at(0);
        BlockIO res = InterpreterSelectQuery(query, subquery_context, QueryProcessingStage::Complete, subquery_depth + 1).execute();

        Block block;
        try
        {
            block = res.in->read();

            if (!block)
            {
                /// Interpret subquery with empty result as Null literal
                ast = std::make_unique<ASTLiteral>(ast->range, Null());
                return;
            }

            if (block.rows() != 1 || res.in->read())
                throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::TOO_MUCH_ROWS)
                throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
            else
                throw;
        }

        size_t columns = block.columns();
        if (columns == 1)
        {
            auto lit = std::make_unique<ASTLiteral>(ast->range, (*block.safeGetByPosition(0).column)[0]);
            lit->alias = subquery->alias;
            ast = addTypeConversion(std::move(lit), block.safeGetByPosition(0).type->getName());
        }
        else
        {
            auto tuple = std::make_shared<ASTFunction>(ast->range);
            tuple->alias = subquery->alias;
            ast = tuple;
            tuple->kind = ASTFunction::FUNCTION;
            tuple->name = "tuple";
            auto exp_list = std::make_shared<ASTExpressionList>(ast->range);
            tuple->arguments = exp_list;
            tuple->children.push_back(tuple->arguments);

            exp_list->children.resize(columns);
            for (size_t i = 0; i < columns; ++i)
            {
                exp_list->children[i] = addTypeConversion(
                    std::make_unique<ASTLiteral>(ast->range, (*block.safeGetByPosition(i).column)[0]),
                    block.safeGetByPosition(i).type->getName());
            }
        }
    }
    else
    {
        /** Don't descend into subqueries in FROM section.
          */
        if (!typeid_cast<ASTTableExpression *>(ast.get()))
        {
            /** Don't descend into subqueries in arguments of IN operator.
              * But if an argument is not subquery, than deeper may be scalar subqueries and we need to descend in them.
              */
            ASTFunction * func = typeid_cast<ASTFunction *>(ast.get());

            if (func && func->kind == ASTFunction::FUNCTION
                && functionIsInOrGlobalInOperator(func->name))
            {
                for (auto & child : ast->children)
                {
                    if (child != func->arguments)
                        executeScalarSubqueriesImpl(child);
                    else
                        for (size_t i = 0, size = func->arguments->children.size(); i < size; ++i)
                            if (i != 1 || !typeid_cast<ASTSubquery *>(func->arguments->children[i].get()))
                                executeScalarSubqueriesImpl(func->arguments->children[i]);
                }
            }
            else
                for (auto & child : ast->children)
                    executeScalarSubqueriesImpl(child);
        }
    }
}


void ExpressionAnalyzer::optimizeGroupBy()
{
    if (!(select_query && select_query->group_expression_list))
        return;

    const auto is_literal = [] (const ASTPtr& ast) {
        return typeid_cast<const ASTLiteral*>(ast.get());
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

        while (columns.end() != std::find_if(columns.begin(), columns.end(),
            [&unused_column_name](const NameAndTypePair & name_type) { return name_type.name == unused_column_name; }))
        {
            ++unused_column;
            unused_column_name = toString(unused_column);
        }

        select_query->group_expression_list = std::make_shared<ASTExpressionList>();
        select_query->group_expression_list->children.emplace_back(std::make_shared<ASTLiteral>(StringRange(), UInt64(unused_column)));
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


void ExpressionAnalyzer::makeSetsForIndex()
{
    if (storage && ast && storage->supportsIndexForIn())
        makeSetsForIndexImpl(ast, storage->getSampleBlock());
}

void ExpressionAnalyzer::makeSetsForIndexImpl(ASTPtr & node, const Block & sample_block)
{
    for (auto & child : node->children)
        makeSetsForIndexImpl(child, sample_block);

    ASTFunction * func = typeid_cast<ASTFunction *>(node.get());
    if (func && func->kind == ASTFunction::FUNCTION && functionIsInOperator(func->name))
    {
        IAST & args = *func->arguments;
        ASTPtr & arg = args.children.at(1);

        if (!typeid_cast<ASTSet *>(arg.get()) && !typeid_cast<ASTSubquery *>(arg.get()) && !typeid_cast<ASTIdentifier *>(arg.get()))
        {
            try
            {
                makeExplicitSet(func, sample_block, true);
            }
            catch (const DB::Exception & e)
            {
                /// in `sample_block` there are no columns that add `getActions`
                if (e.code() != ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK)
                    throw;
            }
        }
    }
}


static std::shared_ptr<InterpreterSelectQuery> interpretSubquery(
    ASTPtr & subquery_or_table_name, const Context & context, size_t subquery_depth, const Names & required_columns)
{
    /// Subquery or table name. The name of the table is similar to the subquery `SELECT * FROM t`.
    const ASTSubquery * subquery = typeid_cast<const ASTSubquery *>(subquery_or_table_name.get());
    const ASTIdentifier * table = typeid_cast<const ASTIdentifier *>(subquery_or_table_name.get());

    if (!subquery && !table)
        throw Exception("IN/JOIN supports only SELECT subqueries.", ErrorCodes::BAD_ARGUMENTS);

    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    Context subquery_context = context;
    Settings subquery_settings = context.getSettings();
    subquery_settings.limits.max_result_rows = 0;
    subquery_settings.limits.max_result_bytes = 0;
    /// The calculation of `extremes` does not make sense and is not necessary (if you do it, then the `extremes` of the subquery can be taken instead of the whole query).
    subquery_settings.extremes = 0;
    subquery_context.setSettings(subquery_settings);

    ASTPtr query;
    if (table)
    {
        /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
        const auto select_query = std::make_shared<ASTSelectQuery>();
        query = select_query;

        const auto select_expression_list = std::make_shared<ASTExpressionList>();
        select_query->select_expression_list = select_expression_list;
        select_query->children.emplace_back(select_query->select_expression_list);

        /// get columns list for target table
        const auto & storage = context.getTable("", table->name);
        const auto & columns = storage->getColumnsListNonMaterialized();
        select_expression_list->children.reserve(columns.size());

        /// manually substitute column names in place of asterisk
        for (const auto & column : columns)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(
                StringRange{}, column.name));

        select_query->replaceDatabaseAndTable("", table->name);
    }
    else
    {
        query = subquery->children.at(0);

        /** Columns with the same name can be specified in a subquery. For example, SELECT x, x FROM t
          * This is bad, because the result of such a query can not be saved to the table, because the table can not have the same name columns.
          * Saving to the table is required for GLOBAL subqueries.
          *
          * To avoid this situation, we will rename the same columns.
          */

        std::set<std::string> all_column_names;
        std::set<std::string> assigned_column_names;

        if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(query.get()))
        {
            for (auto & expr : select->select_expression_list->children)
                all_column_names.insert(expr->getAliasOrColumnName());

            for (auto & expr : select->select_expression_list->children)
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
    }

    if (required_columns.empty())
        return std::make_shared<InterpreterSelectQuery>(
            query, subquery_context, QueryProcessingStage::Complete, subquery_depth + 1);
    else
        return std::make_shared<InterpreterSelectQuery>(
            query, subquery_context, required_columns, QueryProcessingStage::Complete, subquery_depth + 1);
}


void ExpressionAnalyzer::makeSet(ASTFunction * node, const Block & sample_block)
{
    /** You need to convert the right argument to a set.
      * This can be a table name, a value, a value enumeration, or a subquery.
      * The enumeration of values is parsed as a function `tuple`.
      */
    IAST & args = *node->arguments;
    ASTPtr & arg = args.children.at(1);

    /// Already converted.
    if (typeid_cast<ASTSet *>(arg.get()))
        return;

    /// If the subquery or table name for SELECT.
    ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(arg.get());
    if (typeid_cast<ASTSubquery *>(arg.get()) || identifier)
    {
        /// We get the stream of blocks for the subquery. Create Set and put it in place of the subquery.
        String set_id = arg->getColumnName();
        auto ast_set = std::make_shared<ASTSet>(set_id);
        ASTPtr ast_set_ptr = ast_set;

        /// A special case is if the name of the table is specified on the right side of the IN statement, and the table has the type Set (a previously prepared set).
        /// TODO This syntax does not support the specification of the database name.
        if (identifier)
        {
            StoragePtr table = context.tryGetTable("", identifier->name);

            if (table)
            {
                StorageSet * storage_set = typeid_cast<StorageSet *>(table.get());

                if (storage_set)
                {
                    SetPtr & set = storage_set->getSet();
                    ast_set->set = set;
                    arg = ast_set_ptr;
                    return;
                }
            }
        }

        SubqueryForSet & subquery_for_set = subqueries_for_sets[set_id];

        /// If you already created a Set with the same subquery / table.
        if (subquery_for_set.set)
        {
            ast_set->set = subquery_for_set.set;
            arg = ast_set_ptr;
            return;
        }

        ast_set->set = std::make_shared<Set>(settings.limits);

        /** The following happens for GLOBAL INs:
          * - in the addExternalStorage function, the IN (SELECT ...) subquery is replaced with IN _data1,
          *   in the subquery_for_set object, this subquery is set as source and the temporary table _data1 as the table.
          * - this function shows the expression IN_data1.
          */
        if (!subquery_for_set.source)
        {
            auto interpreter = interpretSubquery(arg, context, subquery_depth, {});
            subquery_for_set.source = std::make_shared<LazyBlockInputStream>(
                [interpreter]() mutable { return interpreter->execute().in; });
            subquery_for_set.source_sample = interpreter->getSampleBlock();

            /** Why is LazyBlockInputStream used?
              *
              * The fact is that when processing a request of the form
              *  SELECT ... FROM remote_test WHERE column GLOBAL IN (subquery),
              *  if the distributed remote_test table contains localhost as one of the servers,
              *  the request will be interpreted locally again (and not sent over TCP, as in the case of a remote server).
              *
              * The query execution pipeline will be:
              * CreatingSets
              *  subquery execution, filling the temporary table with _data1 (1)
              *  CreatingSets
              *   reading from the table _data1, creating the set (2)
              *   read from the table subordinate to remote_test.
              *
              * (The second part of the pipeline under CreateSets is a reinterpretation of the request inside StorageDistributed,
              *  the query differs in that the database name and tables are replaced with subordinates, and the subquery is replaced with _data1.)
              *
              * But when creating the pipeline, when creating the source (2), it will be found that the _data1 table is empty
              *  (because the query has not started yet), and empty source will be returned as the source.
              * And then, when the query is executed, an empty set will be created in step (2).
              *
              * Therefore, we make the initialization of step (2) lazy
              *  - so that it does not occur until step (1) is completed, on which the table will be populated.
              *
              * Note: this solution is not very good, you need to think better.
              */
        }

        subquery_for_set.set = ast_set->set;
        arg = ast_set_ptr;
    }
    else
    {
        /// An explicit enumeration of values in parentheses.
        makeExplicitSet(node, sample_block, false);
    }
}

/// The case of an explicit enumeration of values.
void ExpressionAnalyzer::makeExplicitSet(ASTFunction * node, const Block & sample_block, bool create_ordered_set)
{
    IAST & args = *node->arguments;

    if (args.children.size() != 2)
        throw Exception("Wrong number of arguments passed to function in", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTPtr & arg = args.children.at(1);

    DataTypes set_element_types;
    ASTPtr & left_arg = args.children.at(0);

    ASTFunction * left_arg_tuple = typeid_cast<ASTFunction *>(left_arg.get());

    /** NOTE If tuple in left hand side specified non-explicitly
      * Example: identity((a, b)) IN ((1, 2), (3, 4))
      *  instead of        (a, b)) IN ((1, 2), (3, 4))
      * then set creation of set doesn't work correctly.
      */
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        for (const auto & arg : left_arg_tuple->arguments->children)
        {
            const auto & data_type = sample_block.getByName(arg->getColumnName()).type;

            /// @note prevent crash in query: SELECT (1, [1]) in (1, 1)
            if (const auto array = typeid_cast<const DataTypeArray * >(data_type.get()))
                throw Exception("Incorrect element of tuple: " + array->getName(), ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            set_element_types.push_back(data_type);
        }
    }
    else
    {
        DataTypePtr left_type = sample_block.getByName(left_arg->getColumnName()).type;
        if (DataTypeArray * array_type = typeid_cast<DataTypeArray *>(left_type.get()))
            set_element_types.push_back(array_type->getNestedType());
        else
            set_element_types.push_back(left_type);
    }

    /// The case `x in (1, 2)` distinguishes from the case `x in 1` (also `x in (1)`).
    bool single_value = false;
    ASTPtr elements_ast = arg;

    if (ASTFunction * set_func = typeid_cast<ASTFunction *>(arg.get()))
    {
        if (set_func->name == "tuple")
        {
            if (set_func->arguments->children.empty())
            {
                /// Empty set.
                elements_ast = set_func->arguments;
            }
            else
            {
                /// Distinguish the case `(x, y) in ((1, 2), (3, 4))` from the case `(x, y) in (1, 2)`.
                ASTFunction * any_element = typeid_cast<ASTFunction *>(set_func->arguments->children.at(0).get());
                if (set_element_types.size() >= 2 && (!any_element || any_element->name != "tuple"))
                    single_value = true;
                else
                    elements_ast = set_func->arguments;
            }
        }
        else
        {
            if (set_element_types.size() >= 2)
                throw Exception("Incorrect type of 2nd argument for function " + node->name
                    + ". Must be subquery or set of " + toString(set_element_types.size()) + "-element tuples.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            single_value = true;
        }
    }
    else if (typeid_cast<ASTLiteral *>(arg.get()))
    {
        single_value = true;
    }
    else
    {
        throw Exception("Incorrect type of 2nd argument for function " + node->name + ". Must be subquery or set of values.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    if (single_value)
    {
        ASTPtr exp_list = std::make_shared<ASTExpressionList>();
        exp_list->children.push_back(elements_ast);
        elements_ast = exp_list;
    }

    auto ast_set = std::make_shared<ASTSet>(arg->getColumnName());
    ast_set->set = std::make_shared<Set>(settings.limits);
    ast_set->is_explicit = true;
    ast_set->set->createFromAST(set_element_types, elements_ast, context, create_ordered_set);
    arg = ast_set;
}


static String getUniqueName(const Block & block, const String & prefix)
{
    int i = 1;
    while (block.has(prefix + toString(i)))
        ++i;
    return prefix + toString(i);
}


/** For getActionsImpl.
  * A stack of ExpressionActions corresponding to nested lambda expressions.
  * The new action should be added to the highest possible level.
  * For example, in the expression "select arrayMap(x -> x + column1 * column2, array1)"
  *  calculation of the product must be done outside the lambda expression (it does not depend on x), and the calculation of the sum is inside (depends on x).
  */
struct ExpressionAnalyzer::ScopeStack
{
    struct Level
    {
        ExpressionActionsPtr actions;
        NameSet new_columns;
    };

    using Levels = std::vector<Level>;

    Levels stack;
    Settings settings;

    ScopeStack(const ExpressionActionsPtr & actions, const Settings & settings_)
        : settings(settings_)
    {
        stack.emplace_back();
        stack.back().actions = actions;

        const Block & sample_block = actions->getSampleBlock();
        for (size_t i = 0, size = sample_block.columns(); i < size; ++i)
            stack.back().new_columns.insert(sample_block.getByPosition(i).name);
    }

    void pushLevel(const NamesAndTypesList & input_columns)
    {
        stack.emplace_back();
        Level & prev = stack[stack.size() - 2];

        ColumnsWithTypeAndName all_columns;
        NameSet new_names;

        for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
        {
            all_columns.emplace_back(nullptr, it->type, it->name);
            new_names.insert(it->name);
            stack.back().new_columns.insert(it->name);
        }

        const Block & prev_sample_block = prev.actions->getSampleBlock();
        for (size_t i = 0, size = prev_sample_block.columns(); i < size; ++i)
        {
            const ColumnWithTypeAndName & col = prev_sample_block.getByPosition(i);
            if (!new_names.count(col.name))
                all_columns.push_back(col);
        }

        stack.back().actions = std::make_shared<ExpressionActions>(all_columns, settings);
    }

    size_t getColumnLevel(const std::string & name)
    {
        for (int i = static_cast<int>(stack.size()) - 1; i >= 0; --i)
            if (stack[i].new_columns.count(name))
                return i;

        throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    void addAction(const ExpressionAction & action, const Names & additional_required_columns = Names())
    {
        size_t level = 0;
        for (size_t i = 0; i < additional_required_columns.size(); ++i)
            level = std::max(level, getColumnLevel(additional_required_columns[i]));
        Names required = action.getNeededColumns();
        for (size_t i = 0; i < required.size(); ++i)
            level = std::max(level, getColumnLevel(required[i]));

        Names added;
        stack[level].actions->add(action, added);

        stack[level].new_columns.insert(added.begin(), added.end());

        for (size_t i = 0; i < added.size(); ++i)
        {
            const ColumnWithTypeAndName & col = stack[level].actions->getSampleBlock().getByName(added[i]);
            for (size_t j = level + 1; j < stack.size(); ++j)
                stack[j].actions->addInput(col);
        }
    }

    ExpressionActionsPtr popLevel()
    {
        ExpressionActionsPtr res = stack.back().actions;
        stack.pop_back();
        return res;
    }

    const Block & getSampleBlock() const
    {
        return stack.back().actions->getSampleBlock();
    }
};


void ExpressionAnalyzer::getRootActions(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActionsPtr & actions)
{
    ScopeStack scopes(actions, settings);
    getActionsImpl(ast, no_subqueries, only_consts, scopes);
    actions = scopes.popLevel();
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

        getArrayJoinedColumnsImpl(ast);

        /// If the result of ARRAY JOIN is not used, it is necessary to ARRAY-JOIN any column,
        /// to get the correct number of rows.
        if (array_join_result_to_source.empty())
        {
            ASTPtr expr = select_query->array_join_expression_list()->children.at(0);
            String source_name = expr->getColumnName();
            String result_name = expr->getAliasOrColumnName();

            /// This is an array.
            if (!typeid_cast<ASTIdentifier *>(expr.get()) || findColumn(source_name, columns) != columns.end())
            {
                array_join_result_to_source[result_name] = source_name;
            }
            else /// This is a nested table.
            {
                bool found = false;
                for (const auto & column_name_type : columns)
                {
                    String table_name = DataTypeNested::extractNestedTableName(column_name_type.name);
                    String column_name = DataTypeNested::extractNestedColumnName(column_name_type.name);
                    if (table_name == source_name)
                    {
                        array_join_result_to_source[DataTypeNested::concatenateNestedName(result_name, column_name)] = column_name_type.name;
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


/// Fills the array_join_result_to_source: on which columns-arrays to replicate, and how to call them after that.
void ExpressionAnalyzer::getArrayJoinedColumnsImpl(ASTPtr ast)
{
    if (typeid_cast<ASTTablesInSelectQuery *>(ast.get()))
        return;

    if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        if (node->kind == ASTIdentifier::Column)
        {
            String table_name = DataTypeNested::extractNestedTableName(node->name);

            if (array_join_alias_to_name.count(node->name))
            {
                /// ARRAY JOIN was written with an array column. Example: SELECT K1 FROM ... ARRAY JOIN ParsedParams.Key1 AS K1
                array_join_result_to_source[node->name] = array_join_alias_to_name[node->name];    /// K1 -> ParsedParams.Key1
            }
            else if (array_join_alias_to_name.count(table_name))
            {
                /// ARRAY JOIN was written with a nested table. Example: SELECT PP.KEY1 FROM ... ARRAY JOIN ParsedParams AS PP
                String nested_column = DataTypeNested::extractNestedColumnName(node->name);    /// Key1
                array_join_result_to_source[node->name]    /// PP.Key1 -> ParsedParams.Key1
                    = DataTypeNested::concatenateNestedName(array_join_alias_to_name[table_name], nested_column);
            }
            else if (array_join_name_to_alias.count(table_name))
            {
                /** Example: SELECT ParsedParams.Key1 FROM ... ARRAY JOIN ParsedParams AS PP.
                  * That is, the query uses the original array, replicated by itself.
                  */

                String nested_column = DataTypeNested::extractNestedColumnName(node->name);    /// Key1
                array_join_result_to_source[    /// PP.Key1 -> ParsedParams.Key1
                    DataTypeNested::concatenateNestedName(array_join_name_to_alias[table_name], nested_column)] = node->name;
            }
        }
    }
    else
    {
        for (auto & child : ast->children)
            if (!typeid_cast<const ASTSubquery *>(child.get())
                && !typeid_cast<const ASTSelectQuery *>(child.get()))
                getArrayJoinedColumnsImpl(child);
    }
}


void ExpressionAnalyzer::getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack)
{
    /// If the result of the calculation already exists in the block.
    if ((typeid_cast<ASTFunction *>(ast.get()) || typeid_cast<ASTLiteral *>(ast.get()))
        && actions_stack.getSampleBlock().has(ast->getColumnName()))
        return;

    if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        std::string name = node->getColumnName();
        if (!only_consts && !actions_stack.getSampleBlock().has(name))
        {
            /// The requested column is not in the block.
            /// If such a column exists in the table, then the user probably forgot to surround it with an aggregate function or add it to GROUP BY.

            bool found = false;
            for (const auto & column_name_type : columns)
                if (column_name_type.name == name)
                    found = true;

            if (found)
                throw Exception("Column " + name + " is not under aggregate function and not in GROUP BY.",
                    ErrorCodes::NOT_AN_AGGREGATE);
        }
    }
    else if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->kind == ASTFunction::LAMBDA_EXPRESSION)
            throw Exception("Unexpected lambda expression", ErrorCodes::UNEXPECTED_EXPRESSION);

        /// Function arrayJoin.
        if (node->kind == ASTFunction::ARRAY_JOIN)
        {
            if (node->arguments->children.size() != 1)
                throw Exception("arrayJoin requires exactly 1 argument", ErrorCodes::TYPE_MISMATCH);

            ASTPtr arg = node->arguments->children.at(0);
            getActionsImpl(arg, no_subqueries, only_consts, actions_stack);
            if (!only_consts)
            {
                String result_name = node->getColumnName();
                actions_stack.addAction(ExpressionAction::copyColumn(arg->getColumnName(), result_name));
                NameSet joined_columns;
                joined_columns.insert(result_name);
                actions_stack.addAction(ExpressionAction::arrayJoin(joined_columns, false, context));
            }

            return;
        }

        if (node->kind == ASTFunction::FUNCTION)
        {
            if (functionIsInOrGlobalInOperator(node->name))
            {
                if (!no_subqueries)
                {
                    /// Let's find the type of the first argument (then getActionsImpl will be called again and will not affect anything).
                    getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);

                    /// Transform tuple or subquery into a set.
                    makeSet(node, actions_stack.getSampleBlock());
                }
                else
                {
                    if (!only_consts)
                    {
                        /// We are in the part of the tree that we are not going to compute. You just need to define types.
                        /// Do not subquery and create sets. We insert an arbitrary column of the correct type.
                        ColumnWithTypeAndName fake_column;
                        fake_column.name = node->getColumnName();
                        fake_column.type = std::make_shared<DataTypeUInt8>();
                        actions_stack.addAction(ExpressionAction::addColumn(fake_column));
                        getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);
                    }
                    return;
                }
            }

            /// A special function `indexHint`. Everything that is inside it is not calculated
            /// (and is used only for index analysis, see PKCondition).
            if (node->name == "indexHint")
            {
                actions_stack.addAction(ExpressionAction::addColumn(ColumnWithTypeAndName(
                    std::make_shared<ColumnConstUInt8>(1, 1), std::make_shared<DataTypeUInt8>(), node->getColumnName())));
                return;
            }

            const FunctionPtr & function = FunctionFactory::instance().get(node->name, context);

            Names argument_names;
            DataTypes argument_types;
            bool arguments_present = true;

            /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
            bool has_lambda_arguments = false;

            for (auto & child : node->arguments->children)
            {
                ASTFunction * lambda = typeid_cast<ASTFunction *>(child.get());
                ASTSet * set = typeid_cast<ASTSet *>(child.get());
                if (lambda && lambda->name == "lambda")
                {
                    /// If the argument is a lambda expression, just remember its approximate type.
                    if (lambda->arguments->children.size() != 2)
                        throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                    ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(lambda->arguments->children.at(0).get());

                    if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                        throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

                    has_lambda_arguments = true;
                    argument_types.emplace_back(std::make_shared<DataTypeExpression>(DataTypes(lambda_args_tuple->arguments->children.size())));
                    /// Select the name in the next cycle.
                    argument_names.emplace_back();
                }
                else if (set)
                {
                    ColumnWithTypeAndName column;
                    column.type = std::make_shared<DataTypeSet>();

                    /// If the argument is a set given by an enumeration of values, give it a unique name,
                    ///  so that sets with the same record do not fuse together (they can have different types).
                    if (set->is_explicit)
                        column.name = getUniqueName(actions_stack.getSampleBlock(), "__set");
                    else
                        column.name = set->getColumnName();

                    if (!actions_stack.getSampleBlock().has(column.name))
                    {
                        column.column = std::make_shared<ColumnSet>(1, set->set);

                        actions_stack.addAction(ExpressionAction::addColumn(column));
                    }

                    argument_types.push_back(column.type);
                    argument_names.push_back(column.name);
                }
                else
                {
                    /// If the argument is not a lambda expression, call it recursively and find out its type.
                    getActionsImpl(child, no_subqueries, only_consts, actions_stack);
                    std::string name = child->getColumnName();
                    if (actions_stack.getSampleBlock().has(name))
                    {
                        argument_types.push_back(actions_stack.getSampleBlock().getByName(name).type);
                        argument_names.push_back(name);
                    }
                    else
                    {
                        if (only_consts)
                        {
                            arguments_present = false;
                        }
                        else
                        {
                            throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
                        }
                    }
                }
            }

            if (only_consts && !arguments_present)
                return;

            Names additional_requirements;

            if (has_lambda_arguments && !only_consts)
            {
                function->getLambdaArgumentTypes(argument_types);

                /// Call recursively for lambda expressions.
                for (size_t i = 0; i < node->arguments->children.size(); ++i)
                {
                    ASTPtr child = node->arguments->children[i];

                    ASTFunction * lambda = typeid_cast<ASTFunction *>(child.get());
                    if (lambda && lambda->name == "lambda")
                    {
                        DataTypeExpression * lambda_type = typeid_cast<DataTypeExpression *>(argument_types[i].get());
                        ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(lambda->arguments->children.at(0).get());
                        ASTs lambda_arg_asts = lambda_args_tuple->arguments->children;
                        NamesAndTypesList lambda_arguments;

                        for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
                        {
                            ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(lambda_arg_asts[j].get());
                            if (!identifier)
                                throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                            String arg_name = identifier->name;

                            lambda_arguments.emplace_back(arg_name, lambda_type->getArgumentTypes()[j]);
                        }

                        actions_stack.pushLevel(lambda_arguments);
                        getActionsImpl(lambda->arguments->children.at(1), no_subqueries, only_consts, actions_stack);
                        ExpressionActionsPtr lambda_actions = actions_stack.popLevel();

                        String result_name = lambda->arguments->children.at(1)->getColumnName();
                        lambda_actions->finalize(Names(1, result_name));
                        DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;
                        argument_types[i] = std::make_shared<DataTypeExpression>(lambda_type->getArgumentTypes(), result_type);

                        Names captured = lambda_actions->getRequiredColumns();
                        for (size_t j = 0; j < captured.size(); ++j)
                            if (findColumn(captured[j], lambda_arguments) == lambda_arguments.end())
                                additional_requirements.push_back(captured[j]);

                        /// We can not name `getColumnName()`,
                        ///  because it does not uniquely define the expression (the types of arguments can be different).
                        argument_names[i] = getUniqueName(actions_stack.getSampleBlock(), "__lambda");

                        ColumnWithTypeAndName lambda_column;
                        lambda_column.column = std::make_shared<ColumnExpression>(1, lambda_actions, lambda_arguments, result_type, result_name);
                        lambda_column.type = argument_types[i];
                        lambda_column.name = argument_names[i];
                        actions_stack.addAction(ExpressionAction::addColumn(lambda_column));
                    }
                }
            }

            if (only_consts)
            {
                for (size_t i = 0; i < argument_names.size(); ++i)
                {
                    if (!actions_stack.getSampleBlock().has(argument_names[i]))
                    {
                        arguments_present = false;
                        break;
                    }
                }
            }

            if (arguments_present)
                actions_stack.addAction(ExpressionAction::applyFunction(function, argument_names, node->getColumnName()),
                                        additional_requirements);
        }
    }
    else if (ASTLiteral * node = typeid_cast<ASTLiteral *>(ast.get()))
    {
        DataTypePtr type = applyVisitor(FieldToDataType(), node->value);

        ColumnWithTypeAndName column;
        column.column = type->createConstColumn(1, node->value);
        column.type = type;
        column.name = node->getColumnName();

        actions_stack.addAction(ExpressionAction::addColumn(column));
    }
    else
    {
        for (auto & child : ast->children)
            getActionsImpl(child, no_subqueries, only_consts, actions_stack);
    }
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
    if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
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

            getRootActions(arguments[i], true, false, actions);
            const std::string & name = arguments[i]->getColumnName();
            types[i] = actions->getSampleBlock().getByName(name).type;
            aggregate.argument_names[i] = name;
        }

        aggregate.function = context.getAggregateFunctionFactory().get(node->name, types);

        if (node->parameters)
        {
            const ASTs & parameters = typeid_cast<const ASTExpressionList &>(*node->parameters).children;
            Array params_row(parameters.size());

            for (size_t i = 0; i < parameters.size(); ++i)
            {
                const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(parameters[i].get());
                if (!lit)
                    throw Exception("Parameters to aggregate functions must be literals",
                        ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);

                params_row[i] = lit->value;
            }

            aggregate.parameters = params_row;
            aggregate.function->setParameters(params_row);
        }

        aggregate.function->setArguments(types);

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

    if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
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
        chain.settings = settings;
        chain.steps.emplace_back(std::make_shared<ExpressionActions>(columns, settings));
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

    initChain(chain, columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->array_join_expression_list(), only_types, false, step.actions);

    addMultipleArrayJoinAction(step.actions);

    return true;
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions, bool only_types) const
{
    if (only_types)
        actions->add(ExpressionAction::ordinaryJoin(nullptr, columns_added_by_join));
    else
        for (auto & subquery_for_set : subqueries_for_sets)
            if (subquery_for_set.second.join)
                actions->add(ExpressionAction::ordinaryJoin(subquery_for_set.second.join, columns_added_by_join));
}

bool ExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->join())
        return false;

    initChain(chain, columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    const ASTTablesInSelectQueryElement & join_element = static_cast<const ASTTablesInSelectQueryElement &>(*select_query->join());
    const ASTTableJoin & join_params = static_cast<const ASTTableJoin &>(*join_element.table_join);
    const ASTTableExpression & table_to_join = static_cast<const ASTTableExpression &>(*join_element.table_expression);

    if (join_params.using_expression_list)
        getRootActions(join_params.using_expression_list, only_types, false, step.actions);

    /// Two JOINs are not supported with the same subquery, but different USINGs.
    String join_id = join_element.getTreeID();

    SubqueryForSet & subquery_for_set = subqueries_for_sets[join_id];

    /// Special case - if table name is specified on the right of JOIN, then the table has the type Join (the previously prepared mapping).
    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        StoragePtr table = context.tryGetTable("", static_cast<const ASTIdentifier &>(*table_to_join.database_and_table_name).name);

        if (table)
        {
            StorageJoin * storage_join = typeid_cast<StorageJoin *>(table.get());

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
            join_key_names_left, join_key_names_right,
            settings.join_use_nulls, settings.limits,
            join_params.kind, join_params.strictness);

        Names required_joined_columns(join_key_names_right.begin(), join_key_names_right.end());
        for (const auto & name_type : columns_added_by_join)
            required_joined_columns.push_back(name_type.name);

        /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
          * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
          *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
          * - this function shows the expression JOIN _data1.
          */
        if (!subquery_for_set.source)
        {
            ASTPtr table;
            if (table_to_join.database_and_table_name)
                table = table_to_join.database_and_table_name;
            else
                table = table_to_join.subquery;

            auto interpreter = interpretSubquery(table, context, subquery_depth, required_joined_columns);
            subquery_for_set.source = std::make_shared<LazyBlockInputStream>([interpreter]() mutable { return interpreter->execute().in; });
            subquery_for_set.source_sample = interpreter->getSampleBlock();
        }

        /// TODO You do not need to set this up when JOIN is only needed on remote servers.
        subquery_for_set.join = join;
        subquery_for_set.join->setSampleBlock(subquery_for_set.source_sample);
    }

    addJoinAction(step.actions, false);

    return true;
}


bool ExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->where_expression)
        return false;

    initChain(chain, columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->where_expression->getColumnName());
    getRootActions(select_query->where_expression, only_types, false, step.actions);

    return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->group_expression_list)
        return false;

    initChain(chain, columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    ASTs asts = select_query->group_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        step.required_output.push_back(asts[i]->getColumnName());
        getRootActions(asts[i], only_types, false, step.actions);
    }

    return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    initChain(chain, columns);
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
    getRootActions(select_query->having_expression, only_types, false, step.actions);

    return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->select_expression_list, only_types, false, step.actions);

    ASTs asts = select_query->select_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        step.required_output.push_back(asts[i]->getColumnName());
    }
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->order_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->order_expression_list, only_types, false, step.actions);

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

void ExpressionAnalyzer::appendProjectResult(DB::ExpressionActionsChain & chain, bool only_types) const
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    NamesWithAliases result_columns;

    ASTs asts = select_query->select_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        result_columns.emplace_back(asts[i]->getColumnName(), asts[i]->getAliasOrColumnName());
        step.required_output.push_back(result_columns.back().second);
    }

    step.actions->add(ExpressionAction::project(result_columns));
}


Block ExpressionAnalyzer::getSelectSampleBlock()
{
    assertSelect();

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(aggregated_columns, settings);
    NamesWithAliases result_columns;

    ASTs asts = select_query->select_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        result_columns.emplace_back(asts[i]->getColumnName(), asts[i]->getAliasOrColumnName());
        getRootActions(asts[i], true, false, temp_actions);
    }

    temp_actions->add(ExpressionAction::project(result_columns));

    return temp_actions->getSampleBlock();
}

void ExpressionAnalyzer::getActionsBeforeAggregation(ASTPtr ast, ExpressionActionsPtr & actions, bool no_subqueries)
{
    ASTFunction * node = typeid_cast<ASTFunction *>(ast.get());

    if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
        for (auto & argument : node->arguments->children)
            getRootActions(argument, no_subqueries, false, actions);
    else
        for (auto & child : ast->children)
            getActionsBeforeAggregation(child, actions, no_subqueries);
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool project_result)
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(columns, settings);
    NamesWithAliases result_columns;
    Names result_names;

    ASTs asts;

    if (auto node = typeid_cast<const ASTExpressionList *>(ast.get()))
        asts = node->children;
    else
        asts = ASTs(1, ast);

    for (size_t i = 0; i < asts.size(); ++i)
    {
        std::string name = asts[i]->getColumnName();
        std::string alias;
        if (project_result)
            alias = asts[i]->getAliasOrColumnName();
        else
            alias = name;
        result_columns.emplace_back(name, alias);
        result_names.push_back(alias);
        getRootActions(asts[i], false, false, actions);
    }

    if (project_result)
    {
        actions->add(ExpressionAction::project(result_columns));
    }
    else
    {
        /// We will not delete the original columns.
        for (const auto & column_name_type : columns)
            result_names.push_back(column_name_type.name);
    }

    actions->finalize(result_names);

    return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(NamesAndTypesList(), settings);

    getRootActions(ast, true, true, actions);

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
                getRequiredColumnsImpl(expressions[i], required, empty, empty, empty);
            }

            ignored.insert(expressions[i]->getAliasOrColumnName());
        }
    }

    /** You also need to ignore the identifiers of the columns that are obtained by JOIN.
      * (Do not assume that they are required for reading from the "left" table).
      */
    NameSet available_joined_columns;
    collectJoinedColumns(available_joined_columns, columns_added_by_join);

    NameSet required_joined_columns;
    getRequiredColumnsImpl(ast, required, ignored, available_joined_columns, required_joined_columns);

    for (NamesAndTypesList::iterator it = columns_added_by_join.begin(); it != columns_added_by_join.end();)
    {
        if (required_joined_columns.count(it->name))
            ++it;
        else
            columns_added_by_join.erase(it++);
    }

/*    for (const auto & name_type : columns_added_by_join)
        std::cerr << "JOINed column (required, not key): " << name_type.name << std::endl;
    std::cerr << std::endl;*/

    /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
    NameSet array_join_sources;
    for (const auto & result_source : array_join_result_to_source)
        array_join_sources.insert(result_source.second);

    for (const auto & column_name_type : columns)
        if (array_join_sources.count(column_name_type.name))
            required.insert(column_name_type.name);

    /// You need to read at least one column to find the number of rows.
    if (required.empty())
        required.insert(ExpressionActions::getSmallestColumn(columns));

    unknown_required_columns = required;

    for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end();)
    {
        unknown_required_columns.erase(it->name);

        if (!required.count(it->name))
            columns.erase(it++);
        else
            ++it;
    }

    /// Perhaps, there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing the request they are perceived as real.
    if (storage)
    {
        for (auto it = unknown_required_columns.begin(); it != unknown_required_columns.end();)
        {
            if (storage->hasColumn(*it))
            {
                columns.push_back(storage->getColumn(*it));
                unknown_required_columns.erase(it++);
            }
            else
                ++it;
        }
    }
}

void ExpressionAnalyzer::collectJoinedColumns(NameSet & joined_columns, NamesAndTypesList & joined_columns_name_type)
{
    if (!select_query)
        return;

    const ASTTablesInSelectQueryElement * node = select_query->join();

    if (!node)
        return;

    const ASTTableJoin & table_join = static_cast<const ASTTableJoin &>(*node->table_join);
    const ASTTableExpression & table_expression = static_cast<const ASTTableExpression &>(*node->table_expression);

    Block nested_result_sample;
    if (table_expression.database_and_table_name)
    {
        const auto & table = context.getTable("", static_cast<const ASTIdentifier &>(*table_expression.database_and_table_name).name);
        nested_result_sample = table->getSampleBlockNonMaterialized();
    }
    else if (table_expression.subquery)
    {
        const auto & subquery = table_expression.subquery->children.at(0);
        nested_result_sample = InterpreterSelectQuery::getSampleBlock(subquery, context);
    }

    if (table_join.using_expression_list)
    {
        auto & keys = typeid_cast<ASTExpressionList &>(*table_join.using_expression_list);
        for (const auto & key : keys.children)
        {
            if (join_key_names_left.end() == std::find(join_key_names_left.begin(), join_key_names_left.end(), key->getColumnName()))
                join_key_names_left.push_back(key->getColumnName());
            else
                throw Exception("Duplicate column " + key->getColumnName() + " in USING list", ErrorCodes::DUPLICATE_COLUMN);

            if (join_key_names_right.end() == std::find(join_key_names_right.begin(), join_key_names_right.end(), key->getAliasOrColumnName()))
                join_key_names_right.push_back(key->getAliasOrColumnName());
            else
                throw Exception("Duplicate column " + key->getAliasOrColumnName() + " in USING list", ErrorCodes::DUPLICATE_COLUMN);
        }
    }

    for (const auto i : ext::range(0, nested_result_sample.columns()))
    {
        const auto & col = nested_result_sample.safeGetByPosition(i);
        if (join_key_names_right.end() == std::find(join_key_names_right.begin(), join_key_names_right.end(), col.name)
            && !joined_columns.count(col.name)) /// Duplicate columns in the subquery for JOIN do not make sense.
        {
            joined_columns.insert(col.name);
            joined_columns_name_type.emplace_back(col.name, col.type);
        }
    }

/*    for (const auto & name : join_key_names_left)
        std::cerr << "JOIN key (left): " << name << std::endl;
    for (const auto & name : join_key_names_right)
        std::cerr << "JOIN key (right): " << name << std::endl;
    std::cerr << std::endl;
    for (const auto & name : joined_columns)
        std::cerr << "JOINed column: " << name << std::endl;
    std::cerr << std::endl;*/
}


Names ExpressionAnalyzer::getRequiredColumns()
{
    if (!unknown_required_columns.empty())
        throw Exception("Unknown identifier: " + *unknown_required_columns.begin(), ErrorCodes::UNKNOWN_IDENTIFIER);

    Names res;
    for (const auto & column_name_type : columns)
        res.push_back(column_name_type.name);

    return res;
}


void ExpressionAnalyzer::getRequiredColumnsImpl(ASTPtr ast,
    NameSet & required_columns, NameSet & ignored_names,
    const NameSet & available_joined_columns, NameSet & required_joined_columns)
{
    /** Find all the identifiers in the query.
      * We will look for them recursively, bypassing by depth AST.
      * In this case
      * - for lambda functions we will not take formal parameters;
      * - do not go into subqueries (there are their identifiers);
      * - is some exception for the ARRAY JOIN section (it has a slightly different identifier);
      * - identifiers available from JOIN, we put in required_joined_columns.
      */

    if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        if (node->kind == ASTIdentifier::Column
            && !ignored_names.count(node->name)
            && !ignored_names.count(DataTypeNested::extractNestedTableName(node->name)))
        {
            if (!available_joined_columns.count(node->name))
                required_columns.insert(node->name);
            else
                required_joined_columns.insert(node->name);
        }

        return;
    }

    if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->kind == ASTFunction::LAMBDA_EXPRESSION)
        {
            if (node->arguments->children.size() != 2)
                throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(node->arguments->children.at(0).get());

            if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

            /// You do not need to add formal parameters of the lambda expression in required_columns.
            Names added_ignored;
            for (auto & child : lambda_args_tuple->arguments->children)
            {
                ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(child.get());
                if (!identifier)
                    throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

                String & name = identifier->name;
                if (!ignored_names.count(name))
                {
                    ignored_names.insert(name);
                    added_ignored.push_back(name);
                }
            }

            getRequiredColumnsImpl(node->arguments->children.at(1),
                required_columns, ignored_names,
                available_joined_columns, required_joined_columns);

            for (size_t i = 0; i < added_ignored.size(); ++i)
                ignored_names.erase(added_ignored[i]);

            return;
        }

        /// A special function `indexHint`. Everything that is inside it is not calculated
        /// (and is used only for index analysis, see PKCondition).
        if (node->name == "indexHint")
            return;
    }

    /// Recursively traverses an expression.
    for (auto & child : ast->children)
    {
        /** We will not go to the ARRAY JOIN section, because we need to look at the names of non-ARRAY-JOIN columns.
          * There, `collectUsedColumns` will send us separately.
          */
        if (!typeid_cast<ASTSelectQuery *>(child.get())
            && !typeid_cast<ASTArrayJoin *>(child.get()))
            getRequiredColumnsImpl(child, required_columns, ignored_names, available_joined_columns, required_joined_columns);
    }
}

}
