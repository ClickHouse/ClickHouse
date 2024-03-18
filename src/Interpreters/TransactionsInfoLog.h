#pragma once

#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Common/TransactionID.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct TransactionInfoContext;

struct TransactionsInfoLogElement
{
    enum Type
    {
        UNKNOWN = 0,

        BEGIN = 1,
        COMMIT = 2,
        ROLLBACK = 3,

        ADD_PART = 10,
        LOCK_PART = 11,
        UNLOCK_PART = 12,
    };

    Type type = UNKNOWN;
    Decimal64 event_time = 0;
    UInt64 thread_id;

    String query_id;
    TransactionID tid = Tx::EmptyTID;

    /// For COMMIT events
    CSN csn = Tx::UnknownCSN;

    /// For *_PART events
    StorageID table = StorageID::createEmpty();
    String part_name;

    static std::string name() { return "TransactionsInfoLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;

    void fillCommonFields(const TransactionInfoContext * context = nullptr);
};

class TransactionsInfoLog : public SystemLog<TransactionsInfoLogElement>
{
    using SystemLog<TransactionsInfoLogElement>::SystemLog;
};


void tryWriteEventToSystemLog(LoggerPtr log, TransactionsInfoLogElement::Type type,
                              const TransactionID & tid, const TransactionInfoContext & context);

}
