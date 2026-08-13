#pragma once

#include <Parsers/CommonParsers.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Optional trailing `SYNC` for SQL catalog DDL.
///
/// When present on the local catalog path (no `ON CLUSTER`), the interpreter waits for
/// replica-group metadata apply and returns per-node status rows.
inline bool parseSQLClusterCatalogSyncTail(bool & sync, IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_sync(Keyword::SYNC);
    if (s_sync.ignore(pos, expected))
        sync = true;
    return true;
}

inline void formatSQLClusterCatalogSyncTail(WriteBuffer & ostr, bool sync)
{
    if (sync)
        ostr << " SYNC";
}

}
