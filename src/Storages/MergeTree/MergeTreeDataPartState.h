#pragma once

namespace DB
{

/** Part state is a stage of its lifetime. States are ordered and state of a part could be increased only.
  * Part state should be modified under data_parts mutex.
  *
  * Possible state transitions:
  * Temporary -> PreActive:       we are trying to add a fetched, inserted or merged part to active set
  * PreActive -> Outdated:        we could not add a part to active set and are doing a rollback (for example it is duplicated part)
  * PreActive -> Active:          we successfully added a part to active dataset
  * PreActive -> Outdated:        a part was replaced by a covering part or DROP PARTITION
  * Outdated -> Deleting:         a cleaner selected this part for deletion
  * Deleting -> Outdated:         if an ZooKeeper error occurred during the deletion, we will retry deletion
  * Active -> DeleteOnDestroy:    if part was moved to another disk
  */
enum class MergeTreeDataPartState : uint8_t
{
    Temporary,       /// the part is generating now, it is not in data_parts list
    PreActive,       /// the part is in data_parts, but not used for SELECTs
    Active,          /// active data part, used by current and upcoming SELECTs
    Outdated,        /// not active data part, but could be used by only current SELECTs, could be deleted after SELECTs finishes
    Deleting,        /// not active data part with identity refcounter, it is deleting right now by a cleaner
    DeleteOnDestroy, /// part was moved to another disk and should be deleted in own destructor
};

}
