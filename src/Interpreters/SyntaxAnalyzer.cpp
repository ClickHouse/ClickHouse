#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Core/NamesAndTypes.h>

#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/OptimizeIfWithConstantConditionVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ExpressionActions.h> /// getSmallestColumn()
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/OptimizeIfChains.h>
#include <Interpreters/ArithmeticOperationsInAgrFuncOptimize.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/queryToString.h>

#include <Functions/FunctionFactory.h>

#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>

#include <IO/WriteHelpers.h>
#include <Storages/IStorage.h>

#include <functional>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_NESTED_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int EXPECTED_ALL_OR_ANY;
}

namespace
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs

/// Select implementation of a function based on settings.
/// Important that it is done as query rewrite. It means rewritten query
///  will be sent to remote servers during distributed query execution,
///  and on all remote servers, function implementation will be same.
template <char const * func_name>
struct CustomizeFunctionsData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_name;

    void visit(ASTFunction & func, ASTPtr &)
    {
        if (Poco::toLower(func.name) == func_name)
        {
            func.name = customized_func_name;
        }
    }
};

char countdistinct[] = "countdistinct";
using CustomizeFunctionsVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<countdistinct>>, true>;

char in[] = "in";
using CustomizeInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<in>>, true>;

char notIn[] = "notin";
using CustomizeNotInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<notIn>>, true>;

char globalIn[] = "globalin";
using CustomizeGlobalInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<globalIn>>, true>;

char globalNotIn[] = "globalnotin";
using CustomizeGlobalNotInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<globalNotIn>>, true>;


/// Translate qualified names such as db.table.column, table.column, table_alias.column to names' normal form.
/// Expand asterisks and qualified asterisks with column names.
/// There would be columns in normal form & column aliases after translation. Column & column alias would be normalized in QueryNormalizer.
void translateQualifiedNames(ASTPtr & query, const ASTSelectQuery & select_query, const NameSet & source_columns_set,
                             const std::vector<TableWithColumnNames> & tables_with_columns)
{
    LogAST log;
    TranslateQualifiedNamesVisitor::Data visitor_data(source_columns_set, tables_with_columns);
    TranslateQualifiedNamesVisitor visitor(visitor_data, log.stream());
    visitor.visit(query);

    /// This may happen after expansion of COLUMNS('regexp').
    if (select_query.select()->children.empty())
        throw Exception("Empty list of columns in SELECT query", ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED);
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
/// Also we have to remove duplicates in case of GLOBAL subqueries. Their results are placed into tables so duplicates are impossible.
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
void executeScalarSubqueries(ASTPtr & query, const Context & context, size_t subquery_depth, Scalars & scalars, bool only_analyze)
{
    LogAST log;
    ExecuteScalarSubqueriesVisitor::Data visitor_data{context, subquery_depth, scalars, only_analyze};
    ExecuteScalarSubqueriesVisitor(visitor_data, log.stream()).visit(query);
}

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

/** You can not completely remove GROUP BY. Because if there were no aggregate functions, then it turns out that there will be no aggregation.
  * Instead, leave `GROUP BY const`.
  * Next, see deleting the constants in the analyzeAggregation method.
  */
void appendUnusedGroupByColumn(ASTSelectQuery * select_query, const NameSet & source_columns)
{
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

/// Eliminates injective function calls and constant expressions from group by statement.
void optimizeGroupBy(ASTSelectQuery * select_query, const NameSet & source_columns, const Context & context)
{
    const FunctionFactory & function_factory = FunctionFactory::instance();

    if (!select_query->groupBy())
    {
        // If there is a HAVING clause without GROUP BY, make sure we have some aggregation happen.
        if (select_query->having())
            appendUnusedGroupByColumn(select_query, source_columns);
        return;
    }

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
                const auto & dict_ptr = context.getExternalDictionariesLoader().getDictionary(dict_name);
                const auto & attr_name = function->arguments->children[1]->as<ASTLiteral &>().value.safeGet<String>();

                if (!dict_ptr->isInjective(attr_name))
                {
                    ++i;
                    continue;
                }
            }
            else if (!function_factory.get(function->name, context)->isInjective(Block{}))
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
        appendUnusedGroupByColumn(select_query, source_columns);
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

void optimizeIf(ASTPtr & query, Aliases & aliases, bool if_chain_to_miltiif)
{
    /// Optimize if with constant condition after constants was substituted instead of scalar subqueries.
    OptimizeIfWithConstantConditionVisitor(aliases).visit(query);

    if (if_chain_to_miltiif)
        OptimizeIfChainsVisitor().visit(query);
}

void optimizeArithmeticOperationsInAgr(ASTPtr & query, bool optimize_arithmetic_operations_in_agr_func)
{
    if (optimize_arithmetic_operations_in_agr_func)
    {
        /// Removing arithmetic operations from functions
        ArithmeticOperationsInAgrFuncVisitor::Data data = {};
        ArithmeticOperationsInAgrFuncVisitor(data).visit(query);
    }
}

void getArrayJoinedColumns(ASTPtr & query, SyntaxAnalyzerResult & result, const ASTSelectQuery * select_query,
                           const NamesAndTypesList & source_columns, const NameSet & source_columns_set)
{
    if (ASTPtr array_join_expression_list = select_query->arrayJoinExpressionList())
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
            ASTPtr expr = select_query->arrayJoinExpressionList()->children.at(0);
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
                for (const auto & column : source_columns)
                {
                    auto splitted = Nested::splitName(column.name);
                    if (splitted.first == source_name && !splitted.second.empty())
                    {
                        result.array_join_result_to_source[Nested::concatenateName(result_name, splitted.second)] = column.name;
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

void setJoinStrictness(ASTSelectQuery & select_query, JoinStrictness join_default_strictness, bool old_any, ASTTableJoin & out_table_join)
{
    const ASTTablesInSelectQueryElement * node = select_query.join();
    if (!node)
        return;

    auto & table_join = const_cast<ASTTablesInSelectQueryElement *>(node)->table_join->as<ASTTableJoin &>();

    if (table_join.strictness == ASTTableJoin::Strictness::Unspecified &&
        table_join.kind != ASTTableJoin::Kind::Cross)
    {
        if (join_default_strictness == JoinStrictness::ANY)
            table_join.strictness = ASTTableJoin::Strictness::Any;
        else if (join_default_strictness == JoinStrictness::ALL)
            table_join.strictness = ASTTableJoin::Strictness::All;
        else
            throw Exception("Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty",
                            DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
    }

    if (old_any)
    {
        if (table_join.strictness == ASTTableJoin::Strictness::Any &&
            table_join.kind == ASTTableJoin::Kind::Inner)
        {
            table_join.strictness = ASTTableJoin::Strictness::Semi;
            table_join.kind = ASTTableJoin::Kind::Left;
        }

        if (table_join.strictness == ASTTableJoin::Strictness::Any)
            table_join.strictness = ASTTableJoin::Strictness::RightAny;
    }
    else
    {
        if (table_join.strictness == ASTTableJoin::Strictness::Any)
            if (table_join.kind == ASTTableJoin::Kind::Full)
                throw Exception("ANY FULL JOINs are not implemented.", ErrorCodes::NOT_IMPLEMENTED);
    }

    out_table_join = table_join;
}

/// Find the columns that are obtained by JOIN.
void collectJoinedColumns(TableJoin & analyzed_join, const ASTSelectQuery & select_query,
                          const std::vector<TableWithColumnNames> & tables, const Aliases & aliases)
{
    const ASTTablesInSelectQueryElement * node = select_query.join();
    if (!node)
        return;

    const auto & table_join = node->table_join->as<ASTTableJoin &>();

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();
        for (const auto & key : keys.children)
            analyzed_join.addUsingKey(key);
    }
    else if (table_join.on_expression)
    {
        bool is_asof = (table_join.strictness == ASTTableJoin::Strictness::Asof);

        CollectJoinOnKeysVisitor::Data data{analyzed_join, tables[0], tables[1], aliases, is_asof};
        CollectJoinOnKeysVisitor(data).visit(table_join.on_expression);
        if (!data.has_some)
            throw Exception("Cannot get JOIN keys from JOIN ON section: " + queryToString(table_join.on_expression),
                            ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
        if (is_asof)
            data.asofToJoinKeys();
    }
}

std::vector<const ASTFunction *> getAggregates(ASTPtr & query, const ASTSelectQuery & select_query)
{
    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query.where())
        assertNoAggregates(select_query.where(), "in WHERE");
    if (select_query.prewhere())
        assertNoAggregates(select_query.prewhere(), "in PREWHERE");

    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);

    /// There can not be other aggregate functions within the aggregate functions.
    for (const ASTFunction * node : data.aggregates)
        for (auto & arg : node->arguments->children)
            assertNoAggregates(arg, "inside another aggregate function");
    return data.aggregates;
}

}

/// Add columns from storage to source_columns list. Deduplicate resulted list.
/// Special columns are non physical columns, for example ALIAS
void SyntaxAnalyzerResult::collectSourceColumns(bool add_special)
{
    if (storage)
    {
        const ColumnsDescription & columns = storage->getColumns();

        auto columns_from_storage = add_special ? columns.getAll() : columns.getAllPhysical();
        if (source_columns.empty())
            source_columns.swap(columns_from_storage);
        else
            source_columns.insert(source_columns.end(), columns_from_storage.begin(), columns_from_storage.end());
    }

    source_columns_set = removeDuplicateColumns(source_columns);
}


/// Calculate which columns are required to execute the expression.
/// Then, delete all other columns from the list of available columns.
/// After execution, columns will only contain the list of columns needed to read from the table.
void SyntaxAnalyzerResult::collectUsedColumns(const ASTPtr & query, bool is_select)
{
    /// We calculate required_source_columns with source_columns modifications and swap them on exit
    required_source_columns = source_columns;

    RequiredSourceColumnsVisitor::Data columns_context;
    RequiredSourceColumnsVisitor(columns_context).visit(query);

    NameSet source_column_names;
    for (const auto & column : source_columns)
        source_column_names.insert(column.name);

    NameSet required = columns_context.requiredColumns();

    if (columns_context.has_table_join)
    {
        NameSet available_columns;
        for (const auto & name : source_columns)
            available_columns.insert(name.name);

        /// Add columns obtained by JOIN (if needed).
        for (const auto & joined_column : analyzed_join->columnsFromJoinedTable())
        {
            const auto & name = joined_column.name;
            if (available_columns.count(name))
                continue;

            if (required.count(name))
            {
                /// Optimisation: do not add columns needed only in JOIN ON section.
                if (columns_context.nameInclusion(name) > analyzed_join->rightKeyInclusion(name))
                    analyzed_join->addJoinedColumn(joined_column);

                required.erase(name);
            }
        }
    }

    NameSet array_join_sources;
    if (columns_context.has_array_join)
    {
        /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
        for (const auto & result_source : array_join_result_to_source)
            array_join_sources.insert(result_source.second);

        for (const auto & column_name_type : source_columns)
            if (array_join_sources.count(column_name_type.name))
                required.insert(column_name_type.name);
    }

    /// You need to read at least one column to find the number of rows.
    if (is_select && required.empty())
    {
        optimize_trivial_count = true;

        /// We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
        /// Because it is the column that is cheapest to read.
        struct ColumnSizeTuple
        {
            size_t compressed_size;
            size_t type_size;
            size_t uncompressed_size;
            String name;

            bool operator<(const ColumnSizeTuple & that) const
            {
                return std::tie(compressed_size, type_size, uncompressed_size)
                    < std::tie(that.compressed_size, that.type_size, that.uncompressed_size);
            }
        };

        std::vector<ColumnSizeTuple> columns;
        if (storage)
        {
            auto column_sizes = storage->getColumnSizes();
            for (auto & source_column : source_columns)
            {
                auto c = column_sizes.find(source_column.name);
                if (c == column_sizes.end())
                    continue;
                size_t type_size = source_column.type->haveMaximumSizeOfValue() ? source_column.type->getMaximumSizeOfValueInMemory() : 100;
                columns.emplace_back(ColumnSizeTuple{c->second.data_compressed, type_size, c->second.data_uncompressed, source_column.name});
            }
        }

        if (!columns.empty())
            required.insert(std::min_element(columns.begin(), columns.end())->name);
        else
            /// If we have no information about columns sizes, choose a column of minimum size of its data type.
            required.insert(ExpressionActions::getSmallestColumn(source_columns));
    }

    NameSet unknown_required_source_columns = required;

    for (NamesAndTypesList::iterator it = source_columns.begin(); it != source_columns.end();)
    {
        const String & column_name = it->name;
        unknown_required_source_columns.erase(column_name);

        if (!required.count(column_name))
            source_columns.erase(it++);
        else
            ++it;
    }

    /// If there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing they are also considered.
    if (storage)
    {
        const auto storage_virtuals = storage->getVirtuals();
        for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
        {
            auto column = storage_virtuals.tryGetByName(*it);
            if (column)
            {
                source_columns.push_back(*column);
                unknown_required_source_columns.erase(it++);
            }
            else
                ++it;
        }
    }

    if (!unknown_required_source_columns.empty())
    {
        std::stringstream ss;
        ss << "Missing columns:";
        for (const auto & name : unknown_required_source_columns)
            ss << " '" << name << "'";
        ss << " while processing query: '" << queryToString(query) << "'";

        ss << ", required columns:";
        for (const auto & name : columns_context.requiredColumns())
            ss << " '" << name << "'";

        if (!source_column_names.empty())
        {
            ss << ", source columns:";
            for (const auto & name : source_column_names)
                ss << " '" << name << "'";
        }
        else
            ss << ", no source columns";

        if (columns_context.has_table_join)
        {
            ss << ", joined columns:";
            for (const auto & column : analyzed_join->columnsFromJoinedTable())
                ss << " '" << column.name << "'";
        }

        if (!array_join_sources.empty())
        {
            ss << ", arrayJoin columns:";
            for (const auto & name : array_join_sources)
                ss << " '" << name << "'";
        }

        throw Exception(ss.str(), ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    required_source_columns.swap(source_columns);
}


SyntaxAnalyzerResultPtr SyntaxAnalyzer::analyzeSelect(
    ASTPtr & query,
    SyntaxAnalyzerResult && result,
    const SelectQueryOptions & select_options,
    const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
    const Names & required_result_columns,
    std::shared_ptr<TableJoin> table_join) const
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception("Select analyze for not select asts.", ErrorCodes::LOGICAL_ERROR);

    size_t subquery_depth = select_options.subquery_depth;
    bool remove_duplicates = select_options.remove_duplicates;

    const auto & settings = context.getSettingsRef();

    const NameSet & source_columns_set = result.source_columns_set;

    if (table_join)
    {
        result.analyzed_join = table_join;
        result.analyzed_join->resetCollected();
    }
    else /// TODO: remove. For now ExpressionAnalyzer expects some not empty object here
        result.analyzed_join = std::make_shared<TableJoin>();

    if (remove_duplicates)
        renameDuplicatedColumns(select_query);

    /// TODO: Remove unneeded conversion
    std::vector<TableWithColumnNames> tables_with_column_names;
    tables_with_column_names.reserve(tables_with_columns.size());
    for (const auto & table : tables_with_columns)
        tables_with_column_names.emplace_back(table.removeTypes());

    if (tables_with_columns.size() > 1)
    {
        result.analyzed_join->columns_from_joined_table = tables_with_columns[1].columns;
        result.analyzed_join->deduplicateAndQualifyColumnNames(
            source_columns_set, tables_with_columns[1].table.getQualifiedNamePrefix());
    }

    translateQualifiedNames(query, *select_query, source_columns_set, tables_with_column_names);

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, settings.optimize_min_equality_disjunction_chain_length.value).perform();

    normalize(query, result.aliases, settings);

    /// Remove unneeded columns according to 'required_result_columns'.
    /// Leave all selected columns in case of DISTINCT; columns that contain arrayJoin function inside.
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    removeUnneededColumnsFromSelectClause(select_query, required_result_columns, remove_duplicates);

    /// Executing scalar subqueries - replacing them with constant values.
    executeScalarSubqueries(query, context, subquery_depth, result.scalars, select_options.only_analyze);

    {
        optimizeIf(query, result.aliases, settings.optimize_if_chain_to_miltiif);

        optimizeArithmeticOperationsInAgr(query, settings.optimize_arithmetic_operations_in_agr_func);

        /// Push the predicate expression down to the subqueries.
        result.rewrite_subqueries = PredicateExpressionsOptimizer(context, tables_with_column_names, settings).optimize(*select_query);

        /// GROUP BY injective function elimination.
        optimizeGroupBy(select_query, source_columns_set, context);

        /// Remove duplicate items from ORDER BY.
        optimizeOrderBy(select_query);

        /// Remove duplicated elements from LIMIT BY clause.
        optimizeLimitBy(select_query);

        /// Remove duplicated columns from USING(...).
        optimizeUsing(select_query);

        /// array_join_alias_to_name, array_join_result_to_source.
        getArrayJoinedColumns(query, result, select_query, result.source_columns, source_columns_set);

        setJoinStrictness(*select_query, settings.join_default_strictness, settings.any_join_distinct_right_table_keys,
                          result.analyzed_join->table_join);
        collectJoinedColumns(*result.analyzed_join, *select_query, tables_with_column_names, result.aliases);
    }

    result.aggregates = getAggregates(query, *select_query);
    result.collectUsedColumns(query, true);

    if (result.optimize_trivial_count)
        result.optimize_trivial_count = settings.optimize_trivial_count_query &&
            !select_query->where() && !select_query->prewhere() && !select_query->groupBy() && !select_query->having() &&
            !select_query->sampleSize() && !select_query->sampleOffset() && !select_query->final() &&
            (tables_with_column_names.size() < 2 || isLeft(result.analyzed_join->kind()));

    return std::make_shared<const SyntaxAnalyzerResult>(result);
}

SyntaxAnalyzerResultPtr SyntaxAnalyzer::analyze(ASTPtr & query, const NamesAndTypesList & source_columns, ConstStoragePtr storage, bool allow_aggregations) const
{
    if (query->as<ASTSelectQuery>())
        throw Exception("Not select analyze for select asts.", ErrorCodes::LOGICAL_ERROR);

    const auto & settings = context.getSettingsRef();

    SyntaxAnalyzerResult result(source_columns, storage, false);

    normalize(query, result.aliases, settings);

    /// Executing scalar subqueries. Column defaults could be a scalar subquery.
    executeScalarSubqueries(query, context, 0, result.scalars, false);

    optimizeIf(query, result.aliases, settings.optimize_if_chain_to_miltiif);

    if (allow_aggregations)
    {
        GetAggregatesVisitor::Data data;
        GetAggregatesVisitor(data).visit(query);

        /// There can not be other aggregate functions within the aggregate functions.
        for (const ASTFunction * node : data.aggregates)
            for (auto & arg : node->arguments->children)
                assertNoAggregates(arg, "inside another aggregate function");
        result.aggregates = data.aggregates;
    }
    else
        assertNoAggregates(query, "in wrong place");

    result.collectUsedColumns(query, false);
    return std::make_shared<const SyntaxAnalyzerResult>(result);
}

void SyntaxAnalyzer::normalize(ASTPtr & query, Aliases & aliases, const Settings & settings)
{
    CustomizeFunctionsVisitor::Data data{settings.count_distinct_implementation};
    CustomizeFunctionsVisitor(data).visit(query);

    if (settings.transform_null_in)
    {
        CustomizeInVisitor::Data data_null_in{"nullIn"};
        CustomizeInVisitor(data_null_in).visit(query);

        CustomizeNotInVisitor::Data data_not_null_in{"notNullIn"};
        CustomizeNotInVisitor(data_not_null_in).visit(query);

        CustomizeGlobalInVisitor::Data data_global_null_in{"globalNullIn"};
        CustomizeGlobalInVisitor(data_global_null_in).visit(query);

        CustomizeGlobalNotInVisitor::Data data_global_not_null_in{"globalNotNullIn"};
        CustomizeGlobalNotInVisitor(data_global_not_null_in).visit(query);
    }

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    QueryAliasesVisitor(aliases).visit(query);

    /// Mark table ASTIdentifiers with not a column marker
    MarkTableIdentifiersVisitor::Data identifiers_data{aliases};
    MarkTableIdentifiersVisitor(identifiers_data).visit(query);

    /// Common subexpression elimination. Rewrite rules.
    QueryNormalizer::Data normalizer_data(aliases, settings);
    QueryNormalizer(normalizer_data).visit(query);
}

}
