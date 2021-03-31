#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{

MergeTreeTransaction::MergeTreeTransaction(Snapshot snapshot_, LocalTID local_tid_, UUID host_id)
    : tid({snapshot_, local_tid_, host_id})
    , snapshot(snapshot_)
    , state(RUNNING)
{
}


}
