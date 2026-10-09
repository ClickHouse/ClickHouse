#pragma once

namespace DB
{

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    /// If true, the query was written by the user even though it is executed as an `internal` query, i.e. it is not
    /// initiated by the server itself. Subqueries of `PARALLEL WITH` are like that: they are re-executed as nested
    /// queries, but their text comes from the user. Such queries must be subject to all the restrictions of a regular
    /// user query, in particular to access checks - `internal` alone must never be treated as a permission to skip them.
    bool user_initiated = false;
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
    bool parse_query_from_initial_buffer = false; /// If true, do not read more data while parsing the query. The remaining input can be streaming insert data.
    bool background = false; /// If true, this query is the background run scheduled by executeQueryInBackground.
};

}
