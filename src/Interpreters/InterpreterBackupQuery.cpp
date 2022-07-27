#include <Interpreters/InterpreterBackupQuery.h>

#include <Backups/BackupsWorker.h>
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
    Block getResultRow(const BackupsWorker::Info & info)
    {
        auto column_uuid = ColumnUUID::create();
        auto column_backup_name = ColumnString::create();
        auto column_status = ColumnInt8::create();

        column_uuid->insert(info.uuid);
        column_backup_name->insert(info.backup_name);
        column_status->insert(static_cast<Int8>(info.status));

        Block res_columns;
        res_columns.insert(0, {std::move(column_uuid), std::make_shared<DataTypeUUID>(), "uuid"});
        res_columns.insert(1, {std::move(column_backup_name), std::make_shared<DataTypeString>(), "backup_name"});
        res_columns.insert(2, {std::move(column_status), std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "status"});

        return res_columns;
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    auto & backups_worker = context->getBackupsWorker();
    auto [uuid, internal] = backups_worker.start(query_ptr, context);
    BlockIO res_io;
    res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(getResultRow(backups_worker.getInfo(uuid, internal))));
    return res_io;
}

}
