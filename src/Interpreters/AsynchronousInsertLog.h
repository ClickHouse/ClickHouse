#pragma once

#include "Common/Exception.h"
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct AsynchronousInsertLogElement
{
    enum Status : Int8
    {
        Ok = 0,
        ParsingError = 1,
        FlushError = 2,
    };

    time_t event_time{};
    Decimal64 event_time_microseconds{};

    String query_id;
    String query_for_logging;
    String database;
    String table;
    String format;
    UInt64 bytes{};
    UInt64 rows{};
    String exception;
    Status status{};

    time_t flush_time{};
    Decimal64 flush_time_microseconds{};
    String flush_query_id;

    static std::string name() { return "AsynchronousInsertLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class AsynchronousInsertLog : public SystemLog<AsynchronousInsertLogElement>
{
public:
    using SystemLog<AsynchronousInsertLogElement>::SystemLog;

    /// This table is usually queried for fixed table name.
    static const char * getDefaultOrderBy() { return "database, table, event_date, event_time"; }
};

}
