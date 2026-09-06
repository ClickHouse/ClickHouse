#pragma once

namespace DB
{

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
    bool parse_query_from_initial_buffer = false; /// If true, do not read more data while parsing the query. The remaining input can be streaming insert data.
    bool background = false; /// If true, this query is the background run scheduled by executeQueryInBackground.
    /// If true, the query is written to the audit log even though it is internal. Composite statements
    /// (`PARALLEL WITH`, `EXECUTE AS <user> <statement>`) run their parts as internal queries; every part
    /// must be audited on its own, with its own text and outcome. The flag is not inherited by the
    /// internal queries a part may issue in turn.
    bool audit_internal = false;
};

}
