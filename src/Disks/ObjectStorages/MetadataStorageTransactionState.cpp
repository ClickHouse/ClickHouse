#include <Disks/ObjectStorages/MetadataStorageTransactionState.h>
#include <base/defines.h>

namespace DB
{

std::string toString(MetadataStorageTransactionState state)
{
    switch (state)
    {
        case MetadataStorageTransactionState::PREPARING:
            return "PREPARING";
        case MetadataStorageTransactionState::FAILED:
            return "FAILED";
        case MetadataStorageTransactionState::COMMITTED:
            return "COMMITTED";
        case MetadataStorageTransactionState::PARTIALLY_ROLLED_BACK:
            return "PARTIALLY_ROLLED_BACK";
    }
}

}
