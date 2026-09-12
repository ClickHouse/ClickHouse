#pragma once

namespace DB
{

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    bool inherit_process_list_element = false; /// If true, run under the process list element already present in the context.
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
    bool parse_query_from_initial_buffer = false; /// If true, do not read more data while parsing the query. The remaining input can be streaming insert data.
    bool background = false; /// If true, this query is the background run scheduled by executeQueryInBackground.
};

}
