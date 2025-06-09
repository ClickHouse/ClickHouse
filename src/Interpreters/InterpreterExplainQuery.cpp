#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterExplainQuery.h>

#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableOverrideUtils.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Formats/FormatFactory.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FunctionSecretArgumentsFinder.h>

#include <Storages/StorageView.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/printPipeline.h>

#include <Common/JSONBuilder.h>
#include <Core/Settings.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_statistics_optimize;
    extern const SettingsBool format_display_secrets_in_show_and_select;
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
                auto & argument_nodes = table_function_node_ptr->getArgumentsNode()->as<ListNode &>().getNodes();

                for (size_t n = secret_arguments.start; n < secret_arguments.start + secret_arguments.count; ++n)
                {
                    if (secret_arguments.are_named)
                        argument_nodes[n]->as<FunctionNode&>().getArguments().getNodes()[1]->as<ConstantNode&>().setMaskId();
                    else
                        argument_nodes[n]->as<ConstantNode&>().setMaskId();
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
            {"projections", query_plan_options.projections},
            {"optimize", optimize},
            {"json", json},
            {"sorting", query_plan_options.sorting},
            {"distributed", query_plan_options.distributed},
            {"keep_logical_steps", keep_logical_steps},
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

    constexpr static char name[] = "SYNTAX";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"oneline", oneline},
    };

    std::unordered_map<std::string, std::reference_wrapper<Int64>> integer_settings;
};

template <typename Settings>
ExplainSettings<Settings> checkAndGetSettings(const ASTPtr & ast_settings)
{
    if (!ast_settings)
        return {};

    ExplainSettings<Settings> settings;
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

}

QueryPipeline InterpreterExplainQuery::executeImpl()
{
    const auto & ast = query->as<const ASTExplainQuery &>();

    Block sample_block = getSampleBlock(ast.getKind());
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    WriteBufferFromOwnString buf;
    bool single_line = false;
    bool insert_buf = true;

    options.setExplain();

    ContextPtr query_context = getContext();

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

            ExplainAnalyzedSyntaxVisitor::Data data(query_context);
            ExplainAnalyzedSyntaxVisitor(data).visit(query);

            ast.getExplainedQuery()->format(buf, IAST::FormatSettings(settings.oneline));
            break;
        }
        case ASTExplainQuery::QueryTree:
        {
            if (!query_context->getSettingsRef()[Setting::allow_experimental_analyzer])
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "EXPLAIN QUERY TREE is only supported with a new analyzer. SET enable_analyzer = 1.");

            if (ast.getExplainedQuery()->as<ASTSelectWithUnionQuery>() == nullptr)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN QUERY TREE query");

            auto settings = checkAndGetSettings<QueryTreeSettings>(ast.getSettings());
            if (!settings.dump_tree && !settings.dump_ast)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Either 'dump_tree' or 'dump_ast' must be set for EXPLAIN QUERY TREE query");

            auto query_tree = buildQueryTree(ast.getExplainedQuery(), query_context);
            bool need_newline = false;

            if (!getContext()->getSettingsRef()[Setting::format_display_secrets_in_show_and_select])
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

                IAST::FormatSettings format_settings(false);
                format_settings.show_secrets = getContext()->getSettingsRef()[Setting::format_display_secrets_in_show_and_select];

                query_tree->toAST()->format(buf, format_settings);
            }

            break;
        }
        case ASTExplainQuery::QueryPlan:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT is supported for EXPLAIN query");

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
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
                plan.explainPlan(buf, settings.query_plan_options);
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

                auto pipeline = plan.buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));

                if (settings.graph)
                {
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
                InterpreterInsertQuery insert(
                    ast.getExplainedQuery(),
                    query_context,
                    /* allow_materialized */ false,
                    /* no_squash */ false,
                    /* no_destination */ false,
                    /* async_isnert */ false);
                auto io = insert.execute();
                printPipeline(io.pipeline.getProcessors(), buf);
                // we do not need it anymore, it would be executed
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
            auto metadata_snapshot = storage->getInMemoryMetadata();
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
    }
    buf.finalize();
    if (insert_buf)
    {
        if (single_line)
            res_columns[0]->insertData(buf.str().data(), buf.str().size());
        else
            fillColumn(*res_columns[0], buf.str());
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(sample_block.cloneWithColumns(std::move(res_columns))));
}

void registerInterpreterExplainQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    { return std::make_unique<InterpreterExplainQuery>(args.query, args.context, args.options); };
    factory.registerInterpreter("InterpreterExplainQuery", create_fn);
}

}
