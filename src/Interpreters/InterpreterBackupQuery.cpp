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
        auto column_id = ColumnString::create();
        auto column_status = ColumnInt8::create();

        column_id->insert(info.id);
        column_status->insert(static_cast<Int8>(info.status));

        Block res_columns;
        res_columns.insert(0, {std::move(column_id), std::make_shared<DataTypeString>(), "id"});
        res_columns.insert(1, {std::move(column_status), std::make_shared<DataTypeEnum8>(getBackupStatusEnumValues()), "status"});

        return res_columns;
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    auto & backups_worker = context->getBackupsWorker();
    auto id = backups_worker.start(query_ptr, context);

    auto info = backups_worker.getInfo(id);
    if (info.exception)
        std::rethrow_exception(info.exception);

    BlockIO res_io;
    res_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(getResultRow(info)));
    return res_io;
}

}
