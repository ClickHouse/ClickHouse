#include <Common/SipHash.h>
#include <Storages/ColumnDependency.h>

namespace DB
{

UInt64 ColumnDependency::Hash::operator() (const ColumnDependency & dependency) const
{
    SipHash hash;
    hash.update(dependency.column_name);
    hash.update(dependency.kind);
    return hash.get64();
}

}
