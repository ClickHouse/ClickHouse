#include <Disks/ObjectStorages/MetadataFromDiskTransactionState.h>

namespace DB
{

std::string toString(MetadataFromDiskTransactionState state)
{
    switch (state)
    {
        case MetadataFromDiskTransactionState::PREPARING:
            return "PREPARING";
        case MetadataFromDiskTransactionState::FAILED:
            return "FAILED";
        case MetadataFromDiskTransactionState::COMMITTED:
            return "COMMITTED";
        case MetadataFromDiskTransactionState::PARTIALLY_ROLLED_BACK:
            return "PARTIALLY_ROLLED_BACK";
    }
    __builtin_unreachable();
}

}
