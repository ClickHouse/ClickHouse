#pragma once
#include <string>

namespace DB
{

enum class MetadataStorageTransactionState : uint8_t
{
    PREPARING,
    FAILED,
    COMMITTED,
    PARTIALLY_ROLLED_BACK,
};

std::string toString(MetadataStorageTransactionState state);
}
