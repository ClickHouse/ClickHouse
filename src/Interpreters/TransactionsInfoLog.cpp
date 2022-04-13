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
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <base/getThreadId.h>

namespace DB
{

NamesAndTypesList TransactionsInfoLogElement::getNamesAndTypes()
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

    return
    {
        {"type", std::move(type_enum)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime64>(6)},
        {"thread_id", std::make_shared<DataTypeUInt64>()},

        {"query_id", std::make_shared<DataTypeString>()},
        {"tid", getTransactionIDDataType()},
        {"tid_hash", std::make_shared<DataTypeUInt64>()},

        {"csn", std::make_shared<DataTypeUInt64>()},

        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"part", std::make_shared<DataTypeString>()},
    };
}

void TransactionsInfoLogElement::fillCommonFields(const TransactionInfoContext * context)
{
    event_time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    thread_id = getThreadId();

    query_id = CurrentThread::getQueryId().toString();

    if (!context)
        return;

    table = context->table;
    part_name = context->part_name;
}

void TransactionsInfoLogElement::appendToBlock(MutableColumns & columns) const
{
    assert(type != UNKNOWN);
    size_t i = 0;

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


void tryWriteEventToSystemLog(Poco::Logger * log,
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
    system_log->add(elem);
}
catch (...)
{
    tryLogCurrentException(log);
}

}
