#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterBackupQuery.h>

#include <Backups/BackupsWorker.h>
#include <Backups/BackupSettings.h>
#include <Parsers/ASTBackupQuery.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace
{
    Block getResultRow(const String & id, BackupStatus status)
    {
        auto column_id = ColumnString::create();
        auto column_status = ColumnInt8::create();

        column_id->insert(id);
        column_status->insert(static_cast<Int8>(status));

        Block res_columns;
        res_columns.insert(0, {std::move(column_id), std::make_shared<DataTypeString>(), "id"});
        res_columns.insert(1, {std::move(column_status), std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "status"});

        return res_columns;
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    const ASTBackupQuery & backup_query = query_ptr->as<const ASTBackupQuery &>();
    auto & backups_worker = context->getBackupsWorker();

    auto [id, status] = backups_worker.start(query_ptr, context);

    /// Wait if it's a synchronous operation.
    bool async = BackupSettings::isAsync(backup_query);
    if (!async)
        status = backups_worker.wait(id);

    BlockIO res_io;
    res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(getResultRow(id, status)));
    return res_io;
}

void registerInterpreterBackupQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterBackupQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterBackupQuery", create_fn);
}

}
