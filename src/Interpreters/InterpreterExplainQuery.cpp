#include <Interpreters/InterpreterExplainQuery.h>

#include <QueryPipeline/BlockIO.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableOverrideUtils.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Formats/FormatFactory.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <Storages/StorageView.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/printPipeline.h>

#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
    extern const int LOGICAL_ERROR;
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
                StorageView::replaceWithSubquery(select, query_info.view_query->clone(), tmp);
            }
        }
    };

    using ExplainAnalyzedSyntaxVisitor = InDepthNodeVisitor<ExplainAnalyzedSyntaxMatcher, true>;

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
    else
    {
        Block res;
        ColumnWithTypeAndName col;
        col.name = "explain";
        col.type = std::make_shared<DataTypeString>();
        col.column = col.type->createColumn();
        res.insert(col);
        return res;
    }
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

    constexpr static char name[] = "AST";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
        {"graph", graph},
    };
};

struct QueryPlanSettings
{
    QueryPlan::ExplainPlanOptions query_plan_options;

    /// Apply query plan optimizations.
    bool optimize = true;
    bool json = false;

    constexpr static char name[] = "PLAN";

    std::unordered_map<std::string, std::reference_wrapper<bool>> boolean_settings =
    {
            {"header", query_plan_options.header},
            {"description", query_plan_options.description},
            {"actions", query_plan_options.actions},
            {"indexes", query_plan_options.indexes},
            {"optimize", optimize},
            {"json", json}
    };
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
};

template <typename Settings>
struct ExplainSettings : public Settings
{
    using Settings::boolean_settings;

    bool has(const std::string & name_) const
    {
        return boolean_settings.count(name_) > 0;
    }

    void setBooleanSetting(const std::string & name_, bool value)
    {
        auto it = boolean_settings.find(name_);
        if (it == boolean_settings.end())
            throw Exception("Unknown setting for ExplainSettings: " + name_, ErrorCodes::LOGICAL_ERROR);

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

        return res;
    }
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
            throw Exception("Unknown setting \"" + change.name + "\" for EXPLAIN " + Settings::name + " query. "
                            "Supported settings: " + settings.getSettingsList(), ErrorCodes::UNKNOWN_SETTING);

        if (change.value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Invalid type {} for setting \"{}\" only boolean settings are supported",
                change.value.getTypeName(), change.name);

        auto value = change.value.get<UInt64>();
        if (value > 1)
            throw Exception("Invalid value " + std::to_string(value) + " for setting \"" + change.name +
                            "\". Only boolean settings are supported", ErrorCodes::INVALID_SETTING_VALUE);

        settings.setBooleanSetting(change.name, value);
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

    SelectQueryOptions options;
    options.setExplain();

    switch (ast.getKind())
    {
        case ASTExplainQuery::ParsedAST:
        {
            auto settings = checkAndGetSettings<QueryASTSettings>(ast.getSettings());
            if (settings.graph)
                dumpASTInDotFormat(*ast.getExplainedQuery(), buf);
            else
                dumpAST(*ast.getExplainedQuery(), buf);
            break;
        }
        case ASTExplainQuery::AnalyzedSyntax:
        {
            if (ast.getSettings())
                throw Exception("Settings are not supported for EXPLAIN SYNTAX query.", ErrorCodes::UNKNOWN_SETTING);

            ExplainAnalyzedSyntaxVisitor::Data data(getContext());
            ExplainAnalyzedSyntaxVisitor(data).visit(query);

            ast.getExplainedQuery()->format(IAST::FormatSettings(buf, false));
            break;
        }
        case ASTExplainQuery::QueryPlan:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception("Only SELECT is supported for EXPLAIN query", ErrorCodes::INCORRECT_QUERY);

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
            QueryPlan plan;

            InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), options);
            interpreter.buildQueryPlan(plan);

            if (settings.optimize)
                plan.optimize(QueryPlanOptimizationSettings::fromContext(getContext()));

            if (settings.json)
            {
                /// Add extra layers to make plan look more like from postgres.
                auto plan_map = std::make_unique<JSONBuilder::JSONMap>();
                plan_map->add("Plan", plan.explainPlan(settings.query_plan_options));
                auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
                plan_array->add(std::move(plan_map));

                auto format_settings = getFormatSettings(getContext());
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

                InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), options);
                interpreter.buildQueryPlan(plan);
                auto pipeline = plan.buildQueryPipeline(
                    QueryPlanOptimizationSettings::fromContext(getContext()),
                    BuildQueryPipelineSettings::fromContext(getContext()));

                if (settings.graph)
                {
                    /// Pipe holds QueryPlan, should not go out-of-scope
                    auto pipe = QueryPipelineBuilder::getPipe(std::move(*pipeline));
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
                InterpreterInsertQuery insert(ast.getExplainedQuery(), getContext());
                auto io = insert.execute();
                printPipeline(io.pipeline.getProcessors(), buf);
            }
            else
                throw Exception("Only SELECT and INSERT is supported for EXPLAIN PIPELINE query", ErrorCodes::INCORRECT_QUERY);
            break;
        }
        case ASTExplainQuery::QueryEstimates:
        {
            if (!dynamic_cast<const ASTSelectWithUnionQuery *>(ast.getExplainedQuery().get()))
                throw Exception("Only SELECT is supported for EXPLAIN ESTIMATE query", ErrorCodes::INCORRECT_QUERY);

            auto settings = checkAndGetSettings<QueryPlanSettings>(ast.getSettings());
            QueryPlan plan;

            InterpreterSelectWithUnionQuery interpreter(ast.getExplainedQuery(), getContext(), SelectQueryOptions());
            interpreter.buildQueryPlan(plan);
            // collect the selected marks, rows, parts during build query pipeline.
            plan.buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(getContext()),
                BuildQueryPipelineSettings::fromContext(getContext()));

            if (settings.optimize)
                plan.optimize(QueryPlanOptimizationSettings::fromContext(getContext()));
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
            auto storage = getContext()->getQueryContext()->executeTableFunction(ast.getTableFunction());
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
                throw Exception("Settings are not supported for EXPLAIN CURRENT TRANSACTION query.", ErrorCodes::UNKNOWN_SETTING);

            if (auto txn = getContext()->getCurrentTransaction())
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
    if (insert_buf)
    {
        if (single_line)
            res_columns[0]->insertData(buf.str().data(), buf.str().size());
        else
            fillColumn(*res_columns[0], buf.str());
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(sample_block.cloneWithColumns(std::move(res_columns))));
}

}
