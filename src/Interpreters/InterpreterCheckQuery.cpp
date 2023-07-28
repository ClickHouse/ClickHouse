#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTCheckQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ProcessList.h>
#include <algorithm>


namespace DB
{

namespace
{

Block getBlockFromCheckResult(const CheckResults & check_results, bool check_query_single_value_result)
{
    if (check_query_single_value_result)
    {
        bool result = std::all_of(check_results.begin(), check_results.end(), [] (const CheckResult & res) { return res.success; });
        return Block{{ColumnUInt8::create(1, static_cast<UInt8>(result)), std::make_shared<DataTypeUInt8>(), "result"}};
    }

    NamesAndTypes block_structure = NamesAndTypes{
        {"part_path", std::make_shared<DataTypeString>()},
        {"is_passed", std::make_shared<DataTypeUInt8>()},
        {"message", std::make_shared<DataTypeString>()},
    };
    auto path_column = block_structure[0].type->createColumn();
    auto is_passed_column = block_structure[1].type->createColumn();
    auto message_column = block_structure[2].type->createColumn();

    for (const auto & check_result : check_results)
    {
        path_column->insert(check_result.fs_path);
        is_passed_column->insert(static_cast<UInt8>(check_result.success));
        message_column->insert(check_result.failure_message);
    }

    return Block({
        {std::move(path_column), block_structure[0].type, block_structure[0].name},
        {std::move(is_passed_column), block_structure[1].type, block_structure[1].name},
        {std::move(message_column), block_structure[2].type, block_structure[2].name},
    });
}

class TableCheckResultSource : public ISource
{

public:
    explicit TableCheckResultSource(const ASTPtr & query_ptr_, StoragePtr table_, bool check_query_single_value_result_, ContextPtr context_)
        : ISource(getBlockFromCheckResult({}, check_query_single_value_result_).cloneEmpty())
        , query_ptr(query_ptr_)
        , table(table_)
        , context(context_)
        , check_query_single_value_result(check_query_single_value_result_)
    {
        worker_result = std::async(std::launch::async, [this]{ worker(); });
    }

    String getName() const override { return "TableCheckResultSource"; }

protected:

    std::optional<Chunk> tryGenerate() override
    {

        if (is_check_completed)
            return {};

        auto status = worker_result.wait_for(std::chrono::milliseconds(100));
        is_check_completed = (status == std::future_status::ready);

        if (is_check_completed)
        {
            worker_result.get();
            auto result_block = getBlockFromCheckResult(check_results, check_query_single_value_result);
            check_results.clear();
            return Chunk(result_block.getColumns(), result_block.rows());
        }

        std::lock_guard lock(mutex);
        progress(progress_rows, 0);
        progress_rows = 0;

        if (check_query_single_value_result || check_results.empty())
        {
            return Chunk();
        }

        auto result_block = getBlockFromCheckResult(check_results, check_query_single_value_result);
        check_results.clear();
        return Chunk(result_block.getColumns(), result_block.rows());
    }

private:
    void worker()
    {
        table->checkData(query_ptr, context,
            [this](const CheckResult & check_result, size_t new_total_rows)
            {
                if (isCancelled())
                    return false;

                std::lock_guard lock(mutex);
                if (new_total_rows > total_rows)
                {
                    addTotalRowsApprox(new_total_rows - total_rows);
                    total_rows = new_total_rows;
                }
                progress_rows++;

                if (!check_result.success)
                {
                    LOG_WARNING(&Poco::Logger::get("InterpreterCheckQuery"),
                        "Check query for table {} failed, path {}, reason: {}",
                        table->getStorageID().getNameForLogs(),
                        check_result.fs_path,
                        check_result.failure_message);
                }

                check_results.push_back(check_result);

                bool should_continue = check_result.success || !check_query_single_value_result;
                return should_continue;
            });
    }

    ASTPtr query_ptr;
    StoragePtr table;
    ContextPtr context;
    bool check_query_single_value_result;

    std::future<void> worker_result;

    std::mutex mutex;
    CheckResults check_results;
    size_t progress_rows = 0;
    size_t total_rows = 0;

    bool is_check_completed = false;
};

}

InterpreterCheckQuery::InterpreterCheckQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
{
}


BlockIO InterpreterCheckQuery::execute()
{
    const auto & check = query_ptr->as<ASTCheckQuery &>();
    const auto & context = getContext();
    auto table_id = context->resolveStorageID(check, Context::ResolveOrdinary);

    context->checkAccess(AccessType::SHOW_TABLES, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);

    BlockIO res;
    {
        bool check_query_single_value_result = context->getSettingsRef().check_query_single_value_result;
        auto result_source = std::make_shared<TableCheckResultSource>(query_ptr, table, check_query_single_value_result, context);
        res.pipeline = QueryPipeline(result_source);
    }
    return res;
}

}
