#pragma once

namespace DB
{
struct VFSTransactionGroup;

// Store VFSTransactionGroup per thread per object instance
struct VFSTransactionGroupStorage
{
    bool tryAdd(VFSTransactionGroup * group) const;
    VFSTransactionGroup * get() const;
    void remove() const;
};
}
