#include <Interpreters/HashJoin/SharedFixedHashTableFilter.h>

#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>

namespace DB
{

void replaceSharedRuntimeFilters(
    const TableJoin & table_join, const String & build_key_name, const SharedFixedHashTableRuntimeFilter::ProbeFn & probe_fn)
{
    auto query_context = CurrentThread::get().tryGetQueryContext();
    if (!query_context)
        return;
    auto lookup = query_context->getRuntimeFilterLookup();
    if (!lookup)
        return;

    /// The descriptor's `filter_key` is the rendezvous key `BuildRuntimeFilterTransform` registered the
    /// planner's filter under and the probe side looks it up by, not the stable display name.
    for (const auto & descriptor : table_join.getSharedRuntimeFilterDescriptors())
    {
        if (descriptor.build_key_name != build_key_name)
            continue;

        auto existing = lookup->find(descriptor.filter_key);
        if (!existing)
            continue;

        /// A wide common type (`Int64 = UInt64` promotes to `Int128`) would make the per-row arithmetic
        /// on the probe side slower than the Bloom filter it replaces.
        const auto target_type = removeNullable(descriptor.common_type);
        WhichDataType target_which(target_type);
        if (!target_type->isValueRepresentedByInteger() || target_which.isInt128() || target_which.isUInt128() || target_which.isInt256()
            || target_which.isUInt256() || target_which.isIPv4() || target_which.isLowCardinality())
            continue;

        /// The metadata accessors expose data only once every stream-local filter has merged; copied
        /// metadata is therefore complete or absent, never partial.
        auto filter = std::make_unique<RuntimeFilter>(
            /*filters_to_merge_=*/0,
            existing->getConfig(),
            RuntimeFilter::SharedFixedHashTable(
                existing->getFilterColumnTargetType(), probe_fn, existing->getRecordedKeyRanges(), existing->getRecordedKeyValues()));
        /// `replace` keeps the original registration's display name, so the filter statistics stay legible.
        LOG_TRACE(getLogger("SharedFixedHashTableFilter"), "Published shared fixed-hash-table runtime filter under key '{}'", descriptor.filter_key);
        lookup->replace(descriptor.filter_key, std::move(filter));
    }
}

}
