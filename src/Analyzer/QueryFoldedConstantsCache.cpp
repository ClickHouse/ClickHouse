#include <Analyzer/QueryFoldedConstantsCache.h>

#include <Columns/ColumnConst.h>
#include <Common/SipHash.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace
{

/// A constant of a DAG often has no rows, but its data column always holds the value.
const IColumn * getValueColumn(const IColumn & value)
{
    const IColumn * data = &value;
    if (const auto * column_const = typeid_cast<const ColumnConst *>(data))
        data = &column_const->getDataColumn();
    return data->empty() ? nullptr : data;
}

UInt128 hashValue(const IColumn & data)
{
    SipHash hash;
    hash.update(data.getName());
    data.updateHashWithValue(0, hash);
    return hash.get128();
}

}

void QueryFoldedConstantsCache::recordSource(const IColumn & value, const IAST & source)
{
    const auto * data = getValueColumn(value);
    if (!data || !data->valuesHaveFixedSize())
        return;
    const auto value_hash = hashValue(*data);
    SipHash source_hash;
    source.updateTreeHash(source_hash, /*ignore_aliases=*/true);
    std::lock_guard lock(mutex);
    sources.emplace(value_hash, source_hash.get64());
}

UInt64 QueryFoldedConstantsCache::tryGetSource(const IColumn & value) const
{
    const auto * data = getValueColumn(value);
    if (!data)
        return 0;
    const auto value_hash = hashValue(*data);
    std::lock_guard lock(mutex);
    auto it = sources.find(value_hash);
    return it == sources.end() ? 0 : it->second;
}

}
