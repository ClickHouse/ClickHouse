#pragma once
#include <string>

namespace DB
{

enum class MetadataFromDiskTransactionState
{
    PREPARING,
    FAILED,
    COMMITTED,
    PARTIALLY_ROLLED_BACK,
};

std::string toString(MetadataFromDiskTransactionState state);

}
