#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <base/sleep.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 function_sleep_max_microseconds_per_block;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct WaitArgs
{
    ASTPtr condition;
    UInt64 max_tries = 10;
    UInt64 sleep_microseconds = 1000000;
};

class WaitSource final : public ISource
{
public:
    WaitSource(const ContextPtr & context_, const SharedHeader & header_, const WaitArgs & wait_args_)
        : ISource(header_)
        , context(context_)
        , wait_args(wait_args_)
    {
    }

    String getName() const override { return "Wait"; }

    Chunk generate() override
    {
        if (generated)
            return {};

        bool satisfied = false;
        for (size_t i = 0; i < wait_args.max_tries; ++i)
        {
            ContextMutablePtr local_context = Context::createCopy(context);
            local_context->setSetting("max_threads", 1);
            local_context->setSetting("empty_result_for_aggregation_by_empty_set", false);
            local_context->setQueryKindInitial();
            local_context->setCurrentQueryId("");
            auto interpreter = InterpreterSelectWithUnionQuery(wait_args.condition, local_context, SelectQueryOptions().subquery());

            BlockIO block_io = interpreter.execute();
            QueryPipeline & pipeline = block_io.pipeline;
            PullingPipelineExecutor executor(pipeline);
            Chunk chunk;
            if (executor.pull(chunk) && !chunk.empty())
            {
                const auto & column = chunk.getColumns()[0];
                satisfied = !column->isNullAt(0) && column->getUInt(0) != 0;
                if (satisfied)
                    break;
            }

            /// Skip the last sleep
            if (i + 1 >= wait_args.max_tries)
                break;

            UInt64 elapsed = 0;
            auto query_status = context->getProcessListElementSafe();
            while (elapsed < wait_args.sleep_microseconds)
            {
                UInt64 sleep_time = wait_args.sleep_microseconds - elapsed;
                if (query_status)
                    sleep_time = std::min(sleep_time, static_cast<UInt64>(1000000) /* 1 second */);

                sleepForMicroseconds(sleep_time);
                elapsed += sleep_time;

                if (query_status && !query_status->checkTimeLimit())
                    break;
            }
        }

        generated = true;

        UInt8 value = satisfied ? 1 : 0;
        ColumnPtr column = ColumnUInt8::create(1, value);
        return Chunk({std::move(column)}, 1);
    }

private:
    ContextPtr context;
    WaitArgs wait_args;
    bool generated = false;
};

class StorageWait final : public IStorage
{
public:
    StorageWait(const StorageID & table_id_, const WaitArgs & wait_args_)
        : IStorage(table_id_)
        , wait_args(wait_args_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(ColumnsDescription({{"result", std::make_shared<DataTypeUInt8>()}}));
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "Wait"; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override
    {
        return QueryProcessingStage::Enum::FetchColumns;
    }

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override
    {
        const SharedHeader header = std::make_shared<const Block>(getInMemoryMetadataPtr()->getSampleBlock());
        return Pipe(std::make_shared<WaitSource>(context, header, wait_args));
    }

private:
    WaitArgs wait_args;
};

static std::shared_ptr<ASTSelectWithUnionQuery> makeSelectFromExpression(const ASTPtr & expr)
{
    const auto select = std::make_shared<ASTSelectQuery>();

    const auto select_list = std::make_shared<ASTExpressionList>();
    select_list->children.push_back(expr);
    select->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

    auto select_with_union = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union->list_of_selects->children.push_back(select);

    return select_with_union;
}

static Float64 parseLiteralAsFloat64(const ASTPtr & ast)
{
    const auto * literal = ast->as<ASTLiteral>();
    if (!literal)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a literal in AST");

    const Field & value = literal->value;
    return applyVisitor(FieldVisitorConvertToNumber<Float64>(), value);
}

class HasArrayJoinVisitor
{
public:
    bool has_array_join = false;

    void visit(const ASTPtr & ast)
    {
        if (!ast || has_array_join)
            return;

        if (const auto * func = ast->as<ASTFunction>())
        {
            if (func->name == "arrayJoin")
            {
                has_array_join = true;
                return;
            }
        }

        for (const auto & child : ast->children)
            visit(child);
    }
};

static bool containsArrayJoin(const ASTPtr & ast)
{
    HasArrayJoinVisitor visitor;
    visitor.visit(ast);
    return visitor.has_array_join;
}

/// Check whether the AST returns only one row
static void checkASTSelectWithUnionQuery(const ContextPtr & context, const ASTPtr & ast)
{
    const auto * union_query = ast->as<ASTSelectWithUnionQuery>();
    if (!union_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid `condition`, expected `SELECT ...`");

    const auto & selects = union_query->list_of_selects->children;
    if (selects.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`condition` must be `SELECT ... FROM tab`");

    const auto * select_query = selects[0]->as<ASTSelectQuery>();
    if (unlikely(!select_query))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`condition` must be ASTSelectQuery");

    /// no from table, e.g. SELECT 1 + 1
    const bool no_from = select_query->tables() == nullptr;

    /// aggregate without group by, e.g. SELECT count() FROM tab
    bool has_aggregates = false;
    {
        GetAggregatesVisitor::Data data;
        GetAggregatesVisitor visitor(data);
        visitor.visit(select_query->select());
        has_aggregates = !data.aggregates.empty();
    }
    const bool has_group_by = select_query->groupBy() && !select_query->groupBy()->children.empty();
    const bool aggregate_without_group_by = has_aggregates && !has_group_by;

    const bool has_array_join = containsArrayJoin(selects[0]);
    const bool returns_single_row = !has_array_join && (no_from || aggregate_without_group_by);

    if (!returns_single_row)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`condition` must return one Number row");

    const auto condition_header = InterpreterSelectWithUnionQuery(ast, context, SelectQueryOptions().subquery()).getSampleBlock();
    if (condition_header->columns() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`condition` must return one column");

    auto data_type = condition_header->getByPosition(0).type;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(data_type.get()))
        data_type = nullable->getNestedType();

    if (!isNumber(data_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`condition` must return one Number column. Requested: {}", data_type->getName());
}

class TableFunctionWaitUntil final : public ITableFunction
{
public:
    static constexpr auto name = "waitUntil";

    std::string getName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override
    {
        return ColumnsDescription();
    }

private:
    WaitArgs wait_args;

    std::optional<AccessTypeObjects::Source> getSourceAccessObject() const override { return std::nullopt; }

    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr /*context*/,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */,
        bool /* is_insert_query */) const override
    {
        return std::make_shared<StorageWait>(StorageID(getDatabaseName(), table_name), wait_args);
    }

    const char * getStorageEngineName() const override { return "Wait"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override
    {
        const auto & args_func = ast_function->as<ASTFunction &>();
        if (!args_func.arguments)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'waitUntil' must have arguments.");

        const auto & args = args_func.arguments->children;
        if (args.empty() || args.size() > 3)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function waitUntil(condition AST[, max_tries UInt64[, sleep_seconds Float64]])");
        }

        /// condition
        const ASTPtr condition = args[0];
        if (const auto * subquery = condition->as<ASTSubquery>())
            wait_args.condition = subquery->children.at(0);
        else if (condition->as<ASTLiteral>() || condition->as<ASTFunction>())
            wait_args.condition = makeSelectFromExpression(condition);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid `condition`, expected ASTSubquery, ASTLiteral or ASTFunction");

        checkASTSelectWithUnionQuery(context, wait_args.condition);

        /// max_tries
        if (args.size() >= 2)
        {
            wait_args.max_tries = checkAndGetLiteralArgument<UInt64>(args[1], "max_tries");
            if (wait_args.max_tries < 1 || wait_args.max_tries > 30)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "`max_tries` must be between [1, 30]");
        }

        /// sleep_microseconds
        if (args.size() == 3)
        {
            const auto sleep_seconds = parseLiteralAsFloat64(args[2]);
            wait_args.sleep_microseconds = static_cast<UInt64>(sleep_seconds * 1e6);

            const UInt64 sleep_max_microseconds = context->getSettingsRef()[Setting::function_sleep_max_microseconds_per_block];
            if (wait_args.sleep_microseconds > sleep_max_microseconds)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The maximum sleep time is {} microseconds. Requested: {} microseconds",
                    sleep_max_microseconds,
                    wait_args.sleep_microseconds);
            }
        }
    }
};

void registerTableFunctionWaitUntil(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionWaitUntil>(
        TableFunctionProperties{FunctionDocumentation{
            .description = R"(
waitUntil is a control-flow table function that repeatedly evaluates a condition expression or scalar subquery until the condition is
satisfied or a maximum number of attempts is reached. The function blocks execution by sleeping between attempts and returns a single-row
result indicating whether the condition was satisfied within the allowed attempts. This function is usually used for testing.
)",
            .syntax = "waitUntil(condition[, max_tries[, sleep_seconds]])",
            .arguments
            = {{"condition", "An expression or a scalar subquery that evaluates to a single boolean value."},
               {"max_tries", "Maximum number of evaluation attempts. Defaults to 10."},
               {"sleep_seconds", "Number of seconds to sleep between attempts. Defaults to 1."}},
            .returned_value = FunctionDocumentation::ReturnedValue{.description = R"(The function returns a single-column table:
| Name   | Type  | Description                                                     |
| ------ | ----- | --------------------------------------------------------------- |
| result | UInt8 | `1` if the condition is satisfied within `max_tries`, otherwise `0` |

Exactly one row is returned.
)"},
            .examples
            = {FunctionDocumentation::Example{
                   .name = "Literal expression",
                   .query = R"(
SELECT *
FROM waitUntil(false, 3);
)",
                   .result = R"(
┌─result─┐
│      0 │
└────────┘

1 row in set. Elapsed: 2.007 sec.
)"},
               FunctionDocumentation::Example{
                   .name = "Function expression",
                   .query = R"(
SELECT result
FROM waitUntil('2025-12-22 19:00:00' + interval 1 hour < now(), 3, 2);
)",
                   .result = R"(
┌─result─┐
│      1 │
└────────┘

1 row in set. Elapsed: 0.004 sec.
)"},
               FunctionDocumentation::Example{
                   .name = "Scalar subquery",
                   .query = R"(
SELECT result
FROM waitUntil((SELECT count() > 10 FROM tab), 10, 2.5);
)",
                   .result = R"(
┌─result─┐
│      0 │
└────────┘

1 row in set. Elapsed: 22.613 sec.
)"}},
            .category = FunctionDocumentation::Category::TableFunction}},
        TableFunctionFactory::Case::Sensitive);
}

}
