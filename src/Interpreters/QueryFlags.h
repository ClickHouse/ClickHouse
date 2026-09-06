#pragma once

namespace DB
{

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
    bool parse_query_from_initial_buffer = false; /// If true, do not read more data while parsing the query. The remaining input can be streaming insert data.
    /// If true, parse only the main query text without parser limits. Auxiliary expressions, such as
    /// query-construction settings, continue to use the limits from the query context.
    bool parse_server_owned_query_without_limits = false;
    bool background = false; /// If true, this query is the background run scheduled by executeQueryInBackground.
};

}
