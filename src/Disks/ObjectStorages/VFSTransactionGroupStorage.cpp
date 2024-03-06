#include "VFSTransactionGroupStorage.h"
#include <utility>

namespace DB
{
constexpr auto MAX_CONCURRENT_ACCESS = 32;
static thread_local std::pair<VFSTransactionGroupStorage const *, VFSTransactionGroup *> instances[MAX_CONCURRENT_ACCESS]
    = {{nullptr, nullptr}};

bool VFSTransactionGroupStorage::tryAdd(VFSTransactionGroup * group) const
{
    for (auto & pair : instances)
        if (pair.first == this)
        {
            if (pair.second)
                return false;
            pair.second = group;
            return true;
        }
    for (auto & pair : instances)
        if (!pair.first)
        {
            pair = {this, group};
            return true;
        }
    return false;
}

void VFSTransactionGroupStorage::remove() const
{
    for (auto & pair : instances)
        if (pair.first == this)
            pair.first = nullptr;
}

VFSTransactionGroup * VFSTransactionGroupStorage::get() const
{
    for (auto & [disk, group] : instances)
        if (disk == this)
            return group;
    return nullptr;
}
}
