#pragma once

namespace DB
{

struct QueryFlags
{
    bool internal = false; /// If true, this query is caused by another query and thus needn't be registered in the ProcessList.
    bool distributed_backup_restore = false; /// If true, this query is a part of backup restore.
};

}
