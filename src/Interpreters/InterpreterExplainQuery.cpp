#include <Core/SettingsEnums.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterExplainQuery.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableOverrideUtils.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Formats/FormatFactory.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/FunctionSecretArgumentsFinder.h>

#include <Access/Common/SQLSecurityDefs.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/StepWallClockRegistry.h>
#include <QueryPipeline/printPipeline.h>

#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Core/Settings.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>


namespace ProfileEvents
{
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
}


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsUInt64 query_plan_max_step_description_length;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsExplainQueryPlanDefault explain_query_plan_default;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// Walk the AST and expand parameterized view "table function" calls into their inlined,
    /// parameter-substituted subqueries, so `EXPLAIN SYNTAX` shows the resolved query.
    ///
    /// In the analyzer path, without this expansion the query tree would show the unexpanded
    /// table function call. In the legacy path, `ExplainAnalyzedSyntaxMatcher` covers the FROM
    /// table expression via `StorageView::replaceWithSubquery`, which only rewrites the first
    /// table expression and leaves JOIN sides untouched; this visitor is complementary, expanding
    /// parameterized views that appear on the right side of a JOIN as well.
    struct ExpandParameterizedViewsMatcher
    {
        struct Data : public WithContext
        {
            explicit Data(ContextPtr context_) : WithContext(context_) {}
        };

        static bool needChildVisit(ASTPtr &, ASTPtr &)
        {
            return true;
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                expandTables(*select, data);
        }

        /// Iterate all table expressions in the SELECT (FROM and JOINs) and expand
        /// any that are parameterized view calls.
        static void expandTables(ASTSelectQuery & select, const Data & data)
        {
            if (!select.tables() || select.tables()->children.empty())
                return;

            for (auto & child : select.tables()->children)
            {
                auto * table_element = child->as<ASTTablesInSelectQueryElement>();
                if (!table_element || !table_element->table_expression)
                    continue;

                auto * table_expr = table_element->table_expression->as<ASTTableExpression>();
                if (!table_expr || !table_expr->table_function)
                    continue;

                tryExpandTableExpression(*table_expr, data);
            }
        }

        /// If the table expression is a parameterized view call, replace it with
        /// the parameter-substituted inner query as a subquery.
        static void tryExpandTableExpression(ASTTableExpression & table_expr, const Data & data)
        {
            const auto * func = table_expr.table_function->as<ASTFunction>();
            if (!func)
                return;

            /// FINAL and SAMPLE are valid on a parameterized view at execution time, but
            /// rewriting the view call into a subquery here would attach them to the
            /// subquery, where they are rejected with `UNSUPPORTED_METHOD`. Leave the
            /// original call intact so `EXPLAIN SYNTAX` matches what execution accepts.
            if (table_expr.final || table_expr.sample_size || table_expr.sample_offset)
                return;

            auto query_context = data.getContext()->getQueryContext();

            /// A registered table function (e.g. `numbers`) takes precedence over a view with
            /// the same name, matching `QueryAnalyzer::resolveTableFunction`. Without this check
            /// a user view shadowing a built-in table function would be expanded here while
            /// regular execution would still resolve the built-in.
            if (TableFunctionFactory::instance().isTableFunctionName(func->name))
                return;

            String database_name = query_context->getCurrentDatabase();
            String table_name = func->name;
            if (func->isCompoundName())
            {
                std::vector<std::string> parts;
                splitInto<'.'>(parts, func->name);
                if (parts.size() != 2)
                    return;
                database_name = parts[0];
                table_name = parts[1];
            }

            auto storage = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, query_context);
            if (!storage)
                return;

            const auto * storage_view = storage->as<StorageView>();
            if (!storage_view || !storage_view->isParameterizedView())
                return;

            auto metadata = storage->getInMemoryMetadataPtr(query_context, false);

            /// For views created with `SQL SECURITY DEFINER` or `NONE`, execution resolves the
            /// inner tables via `StorageView::getSQLSecurityOverriddenContext`. Inlining the view
            /// here would instead re-analyze the inner query under the invoker's context, so
            /// `EXPLAIN SYNTAX` would fail for users that can query the view but not its inner
            /// tables. Leave the original parameterized call intact in that case.
            if (metadata->sql_security_type && metadata->sql_security_type != SQLSecurityType::INVOKER)
                return;

            auto view_query = metadata->getSelectQuery().inner_query->clone();
            NameToNameMap parameter_values = analyzeFunctionParamValues(table_expr.table_function, query_context);
            StorageView::replaceQueryParametersIfParameterizedView(view_query, parameter_values);

            /// Replace the table function with a subquery in-place on this table expression,
            /// rather than using `StorageView::replaceWithSubquery` which only handles the
            /// first table expression in the SELECT. Preserve the explicit alias from the
            /// original table function (e.g. `... FROM my_pv(n=1) AS t`) so identifiers in
            /// the outer query keep resolving; otherwise fall back to the view's table name
            /// so the rendered `EXPLAIN SYNTAX` keeps referring to the view.
            String alias = table_expr.table_function->tryGetAlias();
            if (alias.empty())
                alias = table_name;

            table_expr.table_function = nullptr;
            table_expr.subquery = make_intrusive<ASTSubquery>(std::move(view_query));
            table_expr.subquery->setAlias(alias);

            table_expr.children.clear();
            table_expr.children.push_back(table_expr.subquery);
        }
    };

    using ExpandParameterizedViewsVisitor = InDepthNodeVisitor<ExpandParameterizedViewsMatcher, true>;

    struct ExplainAnalyzedSyntaxMatcher
    {
        struct Data : public WithContext
        {
            explicit Data(ContextPtr context_) : WithContext(context_) {}
        };

        static bool needChildVisit(ASTPtr & node, ASTPtr &)
        {
            return !node->as<ASTSelectQuery>();
        }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * select = ast->as<ASTSelectQuery>())
                visit(*select, ast, data);
        }

        static void visit(ASTSelectQuery & select, ASTPtr & node, Data & data)
        {
            InterpreterSelectQuery interpreter(
                node, data.getContext(), SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify());

            const SelectQueryInfo & query_info = interpreter.getQueryInfo();
            if (query_info.view_query)
            {
                ASTPtr tmp;
                StorageView::replaceWithSubquery(select, query_info.view_query->clone(), tmp, query_info.is_parameterized_view);
            }
        }
    };

    using ExplainAnalyzedSyntaxVisitor = InDepthNodeVisitor<ExplainAnalyzedSyntaxMatcher, true>;

    class TableFunctionSecretsVisitor : public InDepthQueryTreeVisitor<TableFunctionSecretsVisitor>
    {
        friend class InDepthQueryTreeVisitor;
        bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child [[maybe_unused]])
        {
            QueryTreeNodeType type = parent->getNodeType();
            return type == QueryTreeNodeType::QUERY || type == QueryTreeNodeType::JOIN || type == QueryTreeNodeType::TABLE_FUNCTION;
        }

        void visitImpl(VisitQueryTreeNodeType & query_tree_node)
        {
            auto * table_function_node_ptr = query_tree_node->as<TableFunctionNode>();
            if (!table_function_node_ptr)
                return;

            if (FunctionSecretArgumentsFinder::Result secret_arguments = TableFunctionSecretArgumentsFinderTreeNode(*table_function_node_ptr).getResult(); secret_arguments.count)
            {
                auto & argument_nodes = table_function_node_ptr->getArguments().getNodes();

                for (size_t n = secret_arguments.start; n < secret_arguments.start + secret_arguments.count; ++n)
                {
                    ConstantNode * constant_node = nullptr;
                    if (secret_arguments.are_named)
                    {
                        auto * function_node = argument_nodes[n]->as<FunctionNode>();
                        if (function_node && function_node->getArguments().getNodes().size() >= 2)
                            constant_node = function_node->getArguments().getNodes().at(1)->as<ConstantNode>();
                    }

                    if (!constant_node)
                    {
                        constant_node = argument_nodes[n]->as<ConstantNode>();
                    }

                    if (constant_node)
                        constant_node->setMaskId();
                }
            }
        }
    };

}

BlockIO InterpreterExplainQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterExplainQuery::getSampleBlock(const ASTExplainQuery::ExplainKind kind)
{
    if (kind == ASTExplainQuery::ExplainKind::QueryEstimates)
    {
        auto cols = NamesAndTypes{
            {"database", std::make_shared<DataTypeString>()},
            {"table", std::make_shared<DataTypeString>()},
            {"parts", std::make_shared<DataTypeUInt64>()},
            {"rows", std::make_shared<DataTypeUInt64>()},
            {"marks", std::make_shared<DataTypeUInt64>()},
        };
        return Block({
            {cols[0].type->createColumn(), cols[0].type, cols[0].name},
            {cols[1].type->createColumn(), cols[1].type, cols[1].name},
            {cols[2].type->createColumn(), cols[2].type, cols[2].name},
            {cols[3].type->createColumn(), cols[3].type, cols[3].name},
            {cols[4].type->createColumn(), cols[4].type, cols[4].name},
        });
    }

    Block res;
    ColumnWithTypeAndName col;
    col.name = "explain";
    col.type = std::make_shared<DataTypeString>();
    col.column = col.type->createColumn();
    res.insert(col);
    return res;
}

/// Split str by line feed and write as separate row to ColumnString.
static void fillColumn(IColumn & column, const std::string & str)
{
    size_t start = 0;
    size_t end = 0;
    size_t size = str.size();

    while (end < size)
    {
        if (str[end] == '\n')
        {
            column.insertData(str.data() + start, end - start);
            start = end + 1;
        }

        ++end;
    }

    if (start < end)
        column.insertData(str.data() + start, end - start);
}

namespace
{

/// Settings. Different for each explain type.

struct QueryASTSettings
{
    bool graph = false;
    bool optimize = false;

    constexpr static char name[] = "AST";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"graph", graph},
        {"optimize", optimize}
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

struct QueryTreeSettings
{
    bool run_passes = true;
    bool dump_tree = true;
    bool dump_passes = false;
    bool dump_ast = false;
    Int64 passes = -1;

    /// Only for EXPLAIN SYNTAX
    bool ast_one_line = false;

    constexpr static char name[] = "QUERY TREE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"run_passes", run_passes},
        {"dump_tree", dump_tree},
        {"dump_passes", dump_passes},
        {"dump_ast", dump_ast}
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings =
    {
        {"passes", passes}
    };
};

struct QueryPlanSettings
{
    ExplainPlanOptions query_plan_options;

    /// Apply query plan optimizations.
    bool optimize = true;
    bool keep_logical_steps = false;
    bool json = false;

    constexpr static char name[] = "PLAN";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_plan_options.header},
            {"description", query_plan_options.description},
            {"actions", query_plan_options.actions},
            {"indexes", query_plan_options.indexes},
            {"indices", query_plan_options.indexes},
            {"projections", query_plan_options.projections},
            {"optimize", optimize},
            {"json", json},
            {"sorting", query_plan_options.sorting},
            {"distributed", query_plan_options.distributed},
            {"keep_logical_steps", keep_logical_steps},
            {"input_headers", query_plan_options.input_headers},
            {"column_structure", query_plan_options.column_structure},
            {"compact", query_plan_options.compact},
            {"pretty", query_plan_options.pretty},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

struct QueryPipelineSettings
{
    QueryPlan::ExplainPipelineOptions query_pipeline_options;
    bool graph = false;
    bool compact = true;

    constexpr static char name[] = "PIPELINE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_pipeline_options.header},
            {"graph", graph},
            {"compact", compact},
            {"distributed", query_pipeline_options.distributed},
            {"compact_repeated_processor_chains", query_pipeline_options.compact_repeated_processor_chains},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

struct QueryAnalyzeSettings
{
    ExplainPlanOptions query_plan_options
    {.actions = true,
    .indexes = true,
    .compact = true,
    .pretty = true};

    constexpr static char name[] = "ANALYZE";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"actions", query_plan_options.actions},
        {"indexes", query_plan_options.indexes},
        {"compact", query_plan_options.compact},
        {"pretty", query_plan_options.pretty},
        {"header", query_plan_options.header},
        {"description", query_plan_options.description},
        {"projections", query_plan_options.projections},
        {"sorting", query_plan_options.sorting},
        {"input_headers", query_plan_options.input_headers},
        {"column_structure", query_plan_options.column_structure},
        {"processors", query_plan_options.processors_profile},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

template <typename Settings>
struct ExplainSettings : public Settings
{
    using Settings::boolean_settings;
    using Settings::integer_settings;

    bool has(const std::string & name_) const
    {
        return hasBooleanSetting(name_) || hasIntegerSetting(name_);
    }

    bool hasBooleanSetting(const std::string & name_) const
    {
        return boolean_settings.count(name_) > 0;
    }

    bool hasIntegerSetting(const std::string & name_) const
    {
        return integer_settings.count(name_) > 0;
    }

    void setBooleanSetting(const std::string & name_, bool value)
    {
        auto it = boolean_settings.find(name_);
        if (it == boolean_settings.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown setting for ExplainSettings: {}", name_);

        it->second.get() = value;
    }

    void setIntegerSetting(const std::string & name_, Int64 value)
    {
        auto it = integer_settings.find(name_);
        if (it == integer_settings.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown setting for ExplainSettings: {}", name_);

        it->second.get() = value;
    }

    std::string getSettingsList() const
    {
        std::string res;
        for (const auto & setting : boolean_settings)
        {
            if (!res.empty())
                res += ", ";

            res += setting.first;
        }
        for (const auto & setting : integer_settings)
        {
            if (!res.empty())
                res += ", ";

            res += setting.first;
        }

        return res;
    }
};

struct QuerySyntaxSettings
{
    bool oneline = false;
    bool run_query_tree_passes = false;
    Int64 query_tree_passes = -1;

    constexpr static char name[] = "SYNTAX";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"oneline", oneline},
        {"run_query_tree_passes", run_query_tree_passes}
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings =
    {
        {"query_tree_passes", query_tree_passes}
    };
};

template <typename Settings>
ExplainSettings<Settings> checkAndGetSettings(const ASTPtr & ast_settings, bool set_default_pretty_explain_settings = true)
{
    ExplainSettings<Settings> settings;

    /// These lines are needed to impose the default settings for EXPLAIN PLAN
    /// We set them here instead of QueryPlanSettings, because internally
    /// we sometimes use EXPLAIN PLAN output for logging
    if constexpr (std::is_same_v<Settings, QueryPlanSettings> || std::is_same_v<Settings, QueryAnalyzeSettings>)
    {
        if (set_default_pretty_explain_settings)
        {
            settings.query_plan_options.actions = true;
            settings.query_plan_options.compact = true;
            settings.query_plan_options.pretty  = true;
        }
    }

    if (!ast_settings)
        return settings;

    const auto & set_query = ast_settings->as<ASTSetQuery &>();

    for (const auto & change : set_query.changes)
    {
        if (!settings.has(change.name))
            throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting \"{}\" for EXPLAIN {} query. "
                            "Supported settings: {}", change.name, Settings::name, settings.getSettingsList());

        if (change.value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid type {} for setting \"{}\" only integer settings are supported",
                change.value.getTypeName(), change.name);

        if (settings.hasBooleanSetting(change.name))
        {
            auto value = change.value.safeGet<UInt64>();
            if (value > 1)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid value {} for setting \"{}\". "
                                "Expected boolean type", value, change.name);

            settings.setBooleanSetting(change.name, value);
        }
        else
        {
            auto value = change.value.safeGet<UInt64>();
            settings.setIntegerSetting(change.name, value);
        }
    }

    return settings;
}

bool explainQueryTree(
    ASTPtr explained_query,
    ContextPtr query_context,
    const QueryTreeSettings & settings,
    WriteBuffer & buf,
    bool format_ast_as_syntax)
{
    if (explained_query->as<ASTSelectWithUnionQuery>() == nullptr)
        return false;

    auto query_tree = buildQueryTree(explained_query, query_context);
    bool need_newline = false;

    if (!query_context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select])
    {
        TableFunctionSecretsVisitor visitor;
        visitor.visit(query_tree);
    }

    if (settings.run_passes)
    {
        auto query_tree_pass_manager = QueryTreePassManager(query_context);
        addQueryTreePasses(query_tree_pass_manager);

        size_t pass_index = settings.passes < 0 ? query_tree_pass_manager.getPasses().size() : static_cast<size_t>(settings.passes);

        if (settings.dump_passes)
        {
            query_tree_pass_manager.dump(buf, pass_index);
            need_newline = true;
        }

        query_tree_pass_manager.run(query_tree, pass_index);
    }

    if (settings.dump_tree)
    {
        if (need_newline)
            buf << "\n\n";

        query_tree->dumpTree(buf);
        need_newline = true;
    }

    if (settings.dump_ast)
    {
        if (need_newline)
            buf << "\n\n";

        IAST::FormatSettings format_settings(settings.ast_one_line);
        format_settings.show_secrets = query_context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select];

        ConvertToASTOptions ast_options;
        /// `EXPLAIN SYNTAX` shows the query in a canonical, close-to-syntax form, so constants are
        /// rendered as their source expressions and function calls are preferred over operator syntax.
        /// `EXPLAIN QUERY TREE` (dump_ast) must show the query as it actually is after the query tree passes,
        /// so neither source-expression rendering nor operator-to-function conversion is applied there.
        ast_options.use_source_expression_for_constants = format_ast_as_syntax;

        IAST::FormatState format_state;
        IAST::FormatStateStacked format_frame;
        format_frame.allow_operators = !format_ast_as_syntax;
        query_tree->toAST(ast_options)->format(buf, format_settings, format_state, format_frame);
    }

    return true;
}

}

static void formatHeaderExplainAnalyze(
        UInt64 total_time_ns,
        UInt64 planning_ns,
        UInt64 execute_ns,
        UInt64 read_rows,
        UInt64 read_bytes,
        Int64 peak_memory,
        WriteBuffer & out)
{
    out << "Query summary:\n";

    /// Total time, split into the planning (logical plan, optimization, physical pipeline) and execution phases.
    out << "  Time:        " << formatReadableTime(static_cast<double>(total_time_ns))
        << " (planning " << formatReadableTime(static_cast<double>(planning_ns))
        << " · execution " << formatReadableTime(static_cast<double>(execute_ns)) << ")\n";

    /// Rows/bytes read from tables, with throughput relative to the execution time.
    out << "  Read:        " << formatReadableQuantity(static_cast<double>(read_rows)) << " rows, "
        << formatReadableSizeWithDecimalSuffix(static_cast<double>(read_bytes));
    if (execute_ns)
    {
        const double rows_per_sec = static_cast<double>(read_rows) * 1e9 / static_cast<double>(execute_ns);
        const double bytes_per_sec = static_cast<double>(read_bytes) * 1e9 / static_cast<double>(execute_ns);
        out << " (" << formatReadableQuantity(rows_per_sec) << " rows/s., "
            << formatReadableSizeWithDecimalSuffix(bytes_per_sec) << "/s.)";
    }
    out << "\n";

    out << "  Peak memory: " << formatReadableSizeWithBinarySuffix(static_cast<double>(peak_memory)) << "\n";

    out << "\n";
}

struct InterpreterExplainQuery::AnalyzedInnerQuery
{
    QueryPlan plan;
    ContextPtr context;
    std::function<std::unique_ptr<QueryPlan>()> parallel_replicas_builder;
    bool ignore_quota = false;
    bool ignore_limits = false;
    UInt64 planning_ns = 0;
    ExplainPlanOptions query_plan_options;
};

InterpreterExplainQuery::InterpreterExplainQuery(const ASTPtr & query_, ContextPtr context_, const SelectQueryOptions & options_)
    : WithContext(context_)
    , query(query_)
    , options(options_)
{
}

InterpreterExplainQuery::~InterpreterExplainQuery() = default;

bool InterpreterExplainQuery::isExecutableAnalyze() const
{
    const auto & ast = query->as<const ASTExplainQuery &>();
    if (ast.getKind() != ASTExplainQuery::Analyze)
        return false;

    /// Only an inner SELECT is executed by EXPLAIN ANALYZE; other inner queries are rejected in executeImpl.
    if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
        return false;

    /// Distributed EXPLAIN ANALYZE is rejected before execution, so do not plan it here (e.g. while
    /// charging quota in executeQuery). The quota is charged as for a generic query and the error follows.
    if (getContext()->getSettingsRef()[Setting::make_distributed_plan])
        return false;

    return true;
}

InterpreterExplainQuery::AnalyzedInnerQuery & InterpreterExplainQuery::getAnalyzedInnerQuery() const
{
    if (analyzed_inner_query)
        return *analyzed_inner_query;

    const auto & ast = query->as<const ASTExplainQuery &>();

    /// Mirror the context and option setup that executeImpl applies before planning the inner SELECT,
    /// so the effective ignore_quota / ignore_limits we expose match what actual execution would use.
    auto inner_options = options;
    inner_options.setExplain();

    auto planning_context = Context::createCopy(getContext());
    inner_options.max_step_description_length = planning_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
    InterpreterSetQuery::applySettingsFromQuery(query, planning_context);

    auto result = std::make_unique<AnalyzedInnerQuery>();

    result->query_plan_options = checkAndGetSettings<QueryAnalyzeSettings>(ast.getSettings()).query_plan_options;

    Stopwatch watch;
    if (planning_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), planning_context, inner_options);
        result->context = interpreter.getContext();
        result->parallel_replicas_builder = interpreter.getQueryPlanWithParallelReplicasBuilder();
        /// Force planning so the effective ignore flags settle before we read them.
        interpreter.getQueryPlan();
        result->ignore_quota = interpreter.ignoreQuota();
        result->ignore_limits = interpreter.ignoreLimits();
        result->plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), planning_context, inner_options);
        interpreter.buildQueryPlan(result->plan);
        result->context = interpreter.getContext();
        result->ignore_quota = interpreter.ignoreQuota();
        result->ignore_limits = interpreter.ignoreLimits();
    }
    result->planning_ns = watch.elapsed();

    analyzed_inner_query = std::move(result);
    return *analyzed_inner_query;
}

bool InterpreterExplainQuery::ignoreQuota() const
{
    if (!isExecutableAnalyze())
        return IInterpreter::ignoreQuota();
    return getAnalyzedInnerQuery().ignore_quota;
}

bool InterpreterExplainQuery::ignoreLimits() const
{
    if (!isExecutableAnalyze())
        return IInterpreter::ignoreLimits();
    return getAnalyzedInnerQuery().ignore_limits;
}

QueryPipeline InterpreterExplainQuery::executeImpl()
{
    const auto & ast = query->as<const ASTExplainQuery &>();

    Block sample_block = getSampleBlock(ast.getKind());
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    WriteBufferFromOwnString buf;
    bool single_line = false;
    bool insert_buf = true;

    ContextPtr query_context = getContext();

    options.setExplain();
    options.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];

    /// https://github.com/ClickHouse/ClickHouse/issues/88467
    /// EXPLAIN is to get a good picture of how the query will execute after *static* planning.
    /// Hence disable any optimizations that stagger the planning or introduce variablility due to caches.
    auto explain_query_context = Context::createCopy(query_context);

    if (ast.getKind() != ASTExplainQuery::Analyze)
    {
        explain_query_context->setSetting("use_skip_indexes_on_data_read", false);
        explain_query_context->setSetting("use_query_condition_cache", false);
    }

    InterpreterSetQuery::applySettingsFromQuery(query, explain_query_context);
    query_context = std::move(explain_query_context);

    switch (ast.getKind())
    {
        case ASTExplainQuery::ParsedAST:
        {
            auto settings = checkAndGetSettings<QueryASTSettings>(ast.getSettings());
            if (settings.optimize)
            {
                ExplainAnalyzedSyntaxVisitor::Data data(query_context);
                ExplainAnalyzedSyntaxVisitor(data).visit(query);
            }

            if (settings.graph)
                dumpASTInDotFormat(*ast.getExplainedQuery(), buf);
            else
                dumpAST(*ast.getExplainedQuery(), buf);
            break;
        }
        case ASTExplainQuery::AnalyzedSyntax:
        {
            auto settings = checkAndGetSettings<QuerySyntaxSettings>(ast.getSettings());

            /// Inline any parameterized view calls with their parameter-substituted inner queries,
            /// so EXPLAIN SYNTAX shows what the view actually expands to.
            ExpandParameterizedViewsMatcher::Data expand_views_data(query_context);
            ExpandParameterizedViewsVisitor(expand_views_data).visit(query);

            if (query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                bool explain_ok = explainQueryTree(ast.getExplainedQuery(), query_context, QueryTreeSettings{
                    .run_passes = settings.run_query_tree_passes,
                    .dump_tree = false,
                    .dump_passes = false,
                    .dump_ast = true,
                    .passes = settings.query_tree_passes,
                    .ast_one_line = settings.oneline,
                }, buf, /*format_ast_as_syntax=*/ true);

                if (explain_ok)
                    break;
                auto query_context_mutable = Context::createCopy(query_context);
                query_context_mutable->setSetting("allow_experimental_analyzer", false);
                query_context = std::move(query_context_mutable);
            }

            ExplainAnalyzedSyntaxVisitor::Data data(query_context);
            ExplainAnalyzedSyntaxVisitor(data).visit(query);

            IAST::FormatSettings format_settings(settings.oneline);
            IAST::FormatState format_state;
            IAST::FormatStateStacked format_frame;
            format_frame.allow_operators = false;
            ast.getExplainedQuery()->format(buf, format_settings, format_state, format_frame);
            break;
        }
        case ASTExplainQuery::QueryTree:
        {
            if (!query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "EXPLAIN QUERY TREE is only supported with the analyzer. SET enable_analyzer = 1.");

            auto settings = checkAndGetSettings<QueryTreeSettings>(ast.getSettings());
            if (!settings.dump_tree && !settings.dump_ast)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Either 'dump_tree' or 'dump_ast' must be set for EXPLAIN QUERY TREE query");

            if (!explainQueryTree(ast.getExplainedQuery(), query_context, settings, buf, /*format_ast_as_syntax=*/ false))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN QUERY TREE query");

            break;
        }
        case ASTExplainQuery::QueryPlan:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN query");

            bool pretty_version = query_context->getSettingsRef()[Setting::explain_query_plan_default] == ExplainQueryPlanDefault::PRETTY;

            auto ast_settings = ast.getSettings();

            if (ast_settings)
                for (const auto & change : ast_settings->as<ASTSetQuery &>().changes)
                {
                    if (change.name != "json" && change.name != "distributed")
                        continue;
                    if (change.value.getType() == Field::Types::UInt64 && change.value.safeGet<UInt64>() != 0)
                        pretty_version = false;
                }

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast_settings, pretty_version);

            QueryPlan plan;

            ContextPtr context;

            if (query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), query_context, options);
                context = interpreter.getContext();
                plan = std::move(interpreter).extractQueryPlan();
            }
            else
            {
                InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), query_context, options);
                interpreter.buildQueryPlan(plan);
                context = interpreter.getContext();
            }

            if (settings.optimize)
            {
                auto optimization_settings = QueryPlanOptimizationSettings(context);
                optimization_settings.keep_logical_steps = settings.keep_logical_steps;
                optimization_settings.is_explain = true;
                optimization_settings.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
                plan.optimize(optimization_settings);
            }

            if (settings.json)
            {
                if (settings.query_plan_options.distributed)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Option 'distributed' is not supported with option 'json'");

                /// Add extra layers to make plan look more like from postgres.
                auto plan_map = std::make_unique<JSONBuilder::JSONMap>();
                plan_map->add("Plan", plan.explainPlan(settings.query_plan_options));
                auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
                plan_array->add(std::move(plan_map));

                auto format_settings = getFormatSettings(query_context);
                format_settings.json.quote_64bit_integers = false;

                JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
                JSONBuilder::FormatContext format_context{.out = buf};

                plan_array->format(json_format_settings, format_context);

                single_line = true;
            }
            else
                plan.explainPlan(buf, settings.query_plan_options, 0, query_context->getSettingsRef()[Setting::query_plan_max_step_description_length]);
            break;
        }
        case ASTExplainQuery::QueryPipeline:
        {
            if (dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
            {
                auto settings = checkAndGetSettings<QueryPipelineSettings>(ast.getSettings());
                QueryPlan plan;
                ContextPtr context;

                if (query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                {
                    InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), query_context, options);
                    context = interpreter.getContext();
                    plan = std::move(interpreter).extractQueryPlan();
                }
                else
                {
                    InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), query_context, options);
                    interpreter.buildQueryPlan(plan);
                    context = interpreter.getContext();
                }

                auto optimization_settings = QueryPlanOptimizationSettings(context);
                optimization_settings.is_explain = true;
                optimization_settings.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
                auto pipeline = plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings(context));

                if (settings.graph)
                {
                    if (settings.query_pipeline_options.distributed)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Option 'distributed' is not supported with option 'graph'");

                    /// Pipe holds QueryPlan, should not go out-of-scope
                    QueryPlanResourceHolder resources;
                    auto pipe = QueryPipelineBuilder::getPipe(std::move(*pipeline), resources);
                    const auto & processors = pipe.getProcessors();

                    if (settings.compact)
                        printPipelineCompact(processors, buf, settings.query_pipeline_options.header);
                    else
                        printPipeline(processors, buf);
                }
                else
                {
                    plan.explainPipeline(buf, settings.query_pipeline_options);
                }
            }
            else if (dynamic_cast<const ASTInsertQuery *>(ast.getExplainedQuery().get()))
            {
                auto insert_context = Context::createCopy(getContext());
                InterpreterInsertQuery insert(
                    ast.getExplainedQuery(),
                    insert_context,
                    /* allow_materialized */ false,
                    /* no_squash */ false,
                    /* no_destination */ false,
                    /* async_insert */ false);
                auto io = insert.execute();
                printPipeline(io.pipeline.getProcessors(), buf);
                // we do not need it anymore, it would not be executed
                io.pipeline.cancel();
            }
            else
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT and INSERT is supported for EXPLAIN PIPELINE query");
            break;
        }
        case ASTExplainQuery::QueryEstimates:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN ESTIMATE query");

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
            QueryPlan plan;
            ContextPtr context = query_context;

            if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                InterpreterSelectQueryAnalyzer interpreter(ast.getExplainedQuery(), query_context, SelectQueryOptions());
                context = interpreter.getContext();
                plan = std::move(interpreter).extractQueryPlan();
            }
            else
            {
                InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), query_context, SelectQueryOptions());
                context = interpreter.getContext();
                interpreter.buildQueryPlan(plan);
            }

            // Collect the selected marks, rows, parts during build query pipeline.
            // Hold on to the returned QueryPipelineBuilderPtr because `plan` may have pointers into
            // it (through QueryPlanResourceHolder).
            auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));

            plan.explainEstimate(res_columns);
            insert_buf = false;
            break;
        }
        case ASTExplainQuery::TableOverride:
        {
            if (auto * table_function = ast.getTableFunction()->as<ASTFunction>(); !table_function || table_function->name != "mysql")
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY, "EXPLAIN TABLE OVERRIDE is not supported for the {}() table function", table_function->name);
            }
            auto storage = query_context->getQueryContext()->executeTableFunction(ast.getTableFunction());
            auto metadata = storage->getInMemoryMetadataPtr(query_context, false);
            const StorageInMemoryMetadata & metadata_snapshot = *metadata;
            TableOverrideAnalyzer::Result override_info;
            TableOverrideAnalyzer override_analyzer(ast.getTableOverride());
            override_analyzer.analyze(metadata_snapshot, override_info);
            override_info.appendTo(buf);
            break;
        }
        case ASTExplainQuery::CurrentTransaction:
        {
            if (ast.getSettings())
                throw Exception(ErrorCodes::UNKNOWN_SETTING, "Settings are not supported for EXPLAIN CURRENT TRANSACTION query.");

            if (auto txn = query_context->getCurrentTransaction())
            {
                String dump = txn->dumpDescription();
                buf.write(dump.data(), dump.size());
            }
            else
            {
                writeCString("<no current transaction>", buf);
            }

            break;
        }
        case ASTExplainQuery::WhatIf:
        {
            const auto & query_ast = ast.getExplainedQuery();
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(query_ast.get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN WHATIF query");

            auto whatif_result = WhatIfIndexEstimator::run(query_ast, query_context, ast.getSettings());
            whatif_result.format(buf);
            break;
        }
        case DB::ASTExplainQuery::Analyze:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only SELECT is currently supported for EXPLAIN ANALYZE query");

            /// Distributed query planning rewrites the plan into exchange/remote steps, which EXPLAIN ANALYZE cannot execute here.
            if (query_context->getSettingsRef()[Setting::make_distributed_plan])
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "EXPLAIN ANALYZE doesn't support queries executed in distributed mode");

            /// Plan the inner SELECT. This is cached when ignoreQuota / ignoreLimits already triggered
            /// it during quota charging in executeQuery, so the inner query is never planned twice.
            /// getAnalyzedInnerQuery also validates the EXPLAIN ANALYZE settings (the same check that was
            /// previously done here), so invalid settings are still rejected, just without re-parsing.
            /// EXPLAIN ANALYZE executes the inner SELECT, so quota and result limits must follow the same
            /// rules as running that SELECT directly; the inner interpreter resolves the effective
            /// ignore_quota / ignore_limits during planning (e.g. exempt system tables such as `system.one`).
            auto & analyzed = getAnalyzedInnerQuery();
            QueryPlan plan = std::move(analyzed.plan);
            ContextPtr context = analyzed.context;
            auto parallel_replicas_builder = analyzed.parallel_replicas_builder;
            const bool inner_ignore_quota = analyzed.ignore_quota;
            const bool inner_ignore_limits = analyzed.ignore_limits;
            UInt64 planning_ns = analyzed.planning_ns;
            Stopwatch watch;

            auto optimization_settings = QueryPlanOptimizationSettings(context);

            optimization_settings.max_step_description_length = query_context->getSettingsRef()[Setting::query_plan_max_step_description_length];
            optimization_settings.query_plan_with_parallel_replicas_builder = parallel_replicas_builder;

            watch.restart();
            plan.optimize(optimization_settings);
            planning_ns += watch.elapsed();

            /// Build the per-plan pretty-names registry now: buildQueryPipeline below moves the ActionsDAGs
            /// out of the plan steps, so the names must be snapshotted before the pipeline consumes the plan.
            /// EXPLAIN ANALYZE rejects distributed plans above, so this covers the whole plan tree.
            PrettyNamesPerPlan precomputed_pretty_names = QueryPlanFormat::buildPrettyNamesPerPlan(plan);

            plan.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

            watch.restart();
            auto pipeline_builder = plan.buildQueryPipeline(optimization_settings, BuildQueryPipelineSettings(context), false);
            planning_ns += watch.elapsed();

            watch.restart();
            auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));

            pipeline.setNormalizedQueryHash(query_context->getNormalizedQueryHash());
            auto to_complete = options.to_stage == QueryProcessingStage::Complete;
            auto quota = (!inner_ignore_quota && to_complete) ? context->getQuota() : nullptr;

            /// setLimitsAndQuota attaches a transform, so it must run before the pipeline is completed below.
            if (!inner_ignore_limits && to_complete)
            {
                auto limits = StreamLocalLimits::forQueryResult(context->getSettingsRef());
                pipeline.setLimitsAndQuota(limits, quota);
            }

            if (quota)
                pipeline.setQuota(quota);

            pipeline.complete(std::make_shared<EmptySink>(pipeline.getSharedHeader()));

            /// Inspect the materialized pipeline rather than the plan: remote execution always shows up as one of
            /// these sources, including when it comes from nested sub-plans the plan walk would miss.
            for (const auto & processor : pipeline.getProcessors())
            {
                const auto * proc_ptr = processor.get();
                if (dynamic_cast<const RemoteSource *>(proc_ptr)
                    || dynamic_cast<const RemoteTotalsSource *>(proc_ptr)
                    || dynamic_cast<const RemoteExtremesSource *>(proc_ptr)
                    || dynamic_cast<const DelayedSource *>(proc_ptr))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "EXPLAIN ANALYZE doesn't support queries executed in distributed mode");
            }

            planning_ns += watch.elapsed();

            auto step_wall_clock_registry = std::make_unique<StepWallClockRegistry>();
            step_wall_clock_registry->populateFromPlan(plan);
            pipeline.setStepWallClockRegistry(std::move(step_wall_clock_registry));

            CompletedPipelineExecutor executor(pipeline);

            if (auto cancel_callback = getContext()->getInteractiveCancelCallback())
                executor.setCancelCallback(
                    std::move(cancel_callback),
                    query_context->getSettingsRef()[Setting::interactive_delay] / 1000);

            auto outer_thread_group = CurrentThread::getGroup();
            if (!outer_thread_group)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "EXPLAIN ANALYZE: current thread is not attached to a thread group");

            auto analyze_thread_group = std::make_shared<ThreadGroup>(outer_thread_group);
            analyze_thread_group->memory_tracker.setDescription("EXPLAIN ANALYZE");

            watch.restart();
            {
                ThreadGroupSwitcher switcher(analyze_thread_group, ThreadName::COMPLETED_PIPELINE_EXECUTOR, /*allow_existing_group=*/true);
                executor.execute();
            }
            UInt64 execute_ns = watch.elapsed();

            UInt64 total_time_ns = planning_ns + execute_ns;

            UInt64 read_rows   = analyze_thread_group->performance_counters[ProfileEvents::SelectedRows];
            UInt64 read_bytes  = analyze_thread_group->performance_counters[ProfileEvents::SelectedBytes];
            Int64  peak_memory = analyze_thread_group->memory_tracker.getPeak();

            AnalyzeStepsStats steps_to_stats(pipeline, execute_ns);

            formatHeaderExplainAnalyze(total_time_ns, planning_ns, execute_ns, read_rows, read_bytes, peak_memory, buf);

            plan.explainPlan(buf,
            analyzed.query_plan_options,
            0,
            query_context->getSettingsRef()[Setting::query_plan_max_step_description_length],
            &precomputed_pretty_names,
            "",
            false,
            &steps_to_stats);
        }
    }
    buf.finalize();
    if (insert_buf)
    {
        if (single_line)
            res_columns[0]->insertData(buf.str().data(), buf.str().size());
        else
            fillColumn(*res_columns[0], buf.str());
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(sample_block.cloneWithColumns(std::move(res_columns)))));
}

void registerInterpreterExplainQuery(InterpreterFactory & factory);
void registerInterpreterExplainQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    { return std::make_unique<InterpreterExplainQuery>(args.query, args.context, args.options); };
    factory.registerInterpreter("InterpreterExplainQuery", create_fn);
}

}
