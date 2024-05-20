#include <base/getFQDNOrHostName.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <Interpreters/Context.h>
#include <Common/TransactionID.h>
#include <Common/CurrentThread.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <base/getThreadId.h>

namespace DB
{

ColumnsDescription TransactionsInfoLogElement::getColumnsDescription()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Begin",           static_cast<Int8>(BEGIN)},
            {"Commit",          static_cast<Int8>(COMMIT)},
            {"Rollback",        static_cast<Int8>(ROLLBACK)},

            {"AddPart",         static_cast<Int8>(ADD_PART)},
            {"LockPart",        static_cast<Int8>(LOCK_PART)},
            {"UnlockPart",      static_cast<Int8>(UNLOCK_PART)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "The hostname where transaction was executed."},
        {"type", std::move(type_enum), "The type of the transaction. Possible values: Begin, Commit, Rollback, AddPart, LockPart, UnlockPart."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime64>(6), "Time of the entry"},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "The identifier of a thread."}, /// which thread?

        {"query_id", std::make_shared<DataTypeString>(), "The ID of a query executed in a scope of transaction."},
        {"tid", getTransactionIDDataType(), "The identifier of a transaction."},
        {"tid_hash", std::make_shared<DataTypeUInt64>(), "The hash of the identifier."},

        {"csn", std::make_shared<DataTypeUInt64>(), "The Commit Sequence Number"},

        {"database", std::make_shared<DataTypeString>(), "The name of the database the transaction was executed against."},
        {"table", std::make_shared<DataTypeString>(), "The name of the table the transaction was executed against."},
        {"uuid", std::make_shared<DataTypeUUID>(), "The uuid of the table the transaction was executed against."},
        {"part", std::make_shared<DataTypeString>(), "The name of the part participated in the transaction."}, // ?
    };
}

void TransactionsInfoLogElement::fillCommonFields(const TransactionInfoContext * context)
{
    event_time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    thread_id = getThreadId();

    query_id = std::string(CurrentThread::getQueryId());

    if (!context)
        return;

    table = context->table;
    part_name = context->part_name;
}

void TransactionsInfoLogElement::appendToBlock(MutableColumns & columns) const
{
    assert(type != UNKNOWN);
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(type);
    auto event_time_seconds = event_time / 1000000;
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(thread_id);

    columns[i++]->insert(query_id);
    columns[i++]->insert(Tuple{tid.start_csn, tid.local_tid, tid.host_id});
    columns[i++]->insert(tid.getHash());

    columns[i++]->insert(csn);

    columns[i++]->insert(table.database_name);
    columns[i++]->insert(table.table_name);
    columns[i++]->insert(table.uuid);
    columns[i++]->insert(part_name);
}


void tryWriteEventToSystemLog(LoggerPtr log,
                              TransactionsInfoLogElement::Type type, const TransactionID & tid,
                              const TransactionInfoContext & context)
try
{
    auto system_log =  Context::getGlobalContextInstance()->getTransactionsInfoLog();
    if (!system_log)
        return;

    TransactionsInfoLogElement elem;
    elem.type = type;
    elem.tid = tid;
    elem.fillCommonFields(&context);
    system_log->add(std::move(elem));
}
catch (...)
{
    tryLogCurrentException(log);
}

}
