#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterBackupQuery.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Backups/BackupsWorker.h>
#include <Backups/BackupSettings.h>
#include <Parsers/ASTBackupQuery.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

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
    auto & backup_query = query_ptr->as<ASTBackupQuery &>();

    /// Remove temporary databases from query: they can't be backed up.
    for (auto it = backup_query.elements.begin(); it != backup_query.elements.end();)
    {
        if (!it->database_name.empty())
        {
            const auto & db = DatabaseCatalog::instance().tryGetDatabase(it->database_name, context);
            if (db && db->isTemporary())
            {
                /// database explicitly specified, throw error
                if (it->type != ASTBackupQuery::ElementType::ALL)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Temporary database '{}' cannot be backed up", it->database_name);

                it = backup_query.elements.erase(it);
                continue;
            }
        }
        ++it;
    }
    if (backup_query.elements.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No targets specified");

    auto & backups_worker = context->getBackupsWorker();

    auto [id, status] = backups_worker.start(query_ptr, context);

    /// Wait if it's a synchronous operation.
    bool async = BackupSettings::isAsync(backup_query);
    if (!async)
        status = backups_worker.wait(id);

    BlockIO res_io;
    res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(getResultRow(id, status))));
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
