#pragma once
#include <string>

namespace DB
{

enum class MetadataStorageTransactionState
{
    PREPARING,
    FAILED,
    COMMITTED,
    PARTIALLY_ROLLED_BACK,
};

std::string toString(MetadataStorageTransactionState state);
}
