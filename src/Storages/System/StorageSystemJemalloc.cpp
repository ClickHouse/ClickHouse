#include <Columns/ColumnsNumber.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemJemalloc.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <fmt/core.h>

#include <optional>
#include <unordered_map>
#include <vector>

#include "config.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#    include <Common/Jemalloc.h>
#    include <Common/JemallocCacheArena.h>
#    include <Common/JemallocJITArena.h>
#    include <Common/JemallocMergeTreeArena.h>
#endif


namespace DB
{

#if USE_JEMALLOC

static UInt64 getJeMallocValue(const char * name)
{
    UInt64 value{};
    size_t size = sizeof(value);
    je_mallctl(name, &value, &size, nullptr, 0);
    /// mallctl() fills the value with 32 bit integer for some queries("arenas.nbins" for example).
    /// In this case variable 'size' will be changed from 8 to 4 and the 64 bit variable 'value' will hold the 32 bit actual value times 2^32 on big-endian machines.
    /// We should right shift the value by 32 on big-endian machines(which is unnecessary on little-endian machines).
    if constexpr (std::endian::native == std::endian::big)
    {
        if (size == 4)
            value >>= 32;
    }
    return value;
}

namespace
{

/// Per-bin geometry, static for the lifetime of the process and identical in every arena.
struct JemallocBinsGeometry
{
    struct SmallBin
    {
        UInt64 size;
        UInt64 nregs;
        UInt64 slab_size;
    };
    std::vector<SmallBin> small_bins;
    std::vector<UInt64> lextent_sizes;
};

JemallocBinsGeometry getJemallocBinsGeometry()
{
    JemallocBinsGeometry geometry;

    geometry.small_bins.resize(getJeMallocValue("arenas.nbins"));
    for (UInt64 bin = 0; bin < geometry.small_bins.size(); ++bin)
    {
        geometry.small_bins[bin].size = getJeMallocValue(fmt::format("arenas.bin.{}.size", bin).c_str());
        geometry.small_bins[bin].nregs = getJeMallocValue(fmt::format("arenas.bin.{}.nregs", bin).c_str());
        geometry.small_bins[bin].slab_size = getJeMallocValue(fmt::format("arenas.bin.{}.slab_size", bin).c_str());
    }

    geometry.lextent_sizes.resize(getJeMallocValue("arenas.nlextents"));
    for (UInt64 bin = 0; bin < geometry.lextent_sizes.size(); ++bin)
        geometry.lextent_sizes[bin] = getJeMallocValue(fmt::format("arenas.lextent.{}.size", bin).c_str());

    return geometry;
}

/// One row per bin; stats are read merged over all arenas when `arena` is empty,
/// from the single arena otherwise (then `arena` and `purpose` are also written
/// as the leading columns).
void fillJemallocBinsRows(MutableColumns & res_columns, const JemallocBinsGeometry & geometry, std::optional<UInt64> arena, const char * purpose = "")
{
    const std::string arena_key = arena ? fmt::format("{}", *arena) : fmt::format("{}", MALLCTL_ARENAS_ALL);

    UInt16 bin_index = 0;
    for (UInt64 bin = 0; bin < geometry.small_bins.size(); ++bin, ++bin_index)
    {
        auto nmalloc = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.nmalloc", arena_key, bin).c_str());
        auto ndalloc = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.ndalloc", arena_key, bin).c_str());
        auto curslabs = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.curslabs", arena_key, bin).c_str());
        auto curregs = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.curregs", arena_key, bin).c_str());
        auto nonfull_slabs = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.nonfull_slabs", arena_key, bin).c_str());

        size_t col_num = 0;
        if (arena)
        {
            res_columns.at(col_num++)->insert(*arena);
            res_columns.at(col_num++)->insert(purpose);
        }
        res_columns.at(col_num++)->insert(bin_index);
        res_columns.at(col_num++)->insert(0);
        res_columns.at(col_num++)->insert(geometry.small_bins[bin].size);
        res_columns.at(col_num++)->insert(nmalloc);
        res_columns.at(col_num++)->insert(ndalloc);

        res_columns.at(col_num++)->insert(geometry.small_bins[bin].nregs);
        res_columns.at(col_num++)->insert(curslabs);
        res_columns.at(col_num++)->insert(curregs);
        res_columns.at(col_num++)->insert(geometry.small_bins[bin].slab_size);
        res_columns.at(col_num++)->insert(nonfull_slabs);
    }

    for (UInt64 bin = 0; bin < geometry.lextent_sizes.size(); ++bin, ++bin_index)
    {
        auto nmalloc = getJeMallocValue(fmt::format("stats.arenas.{}.lextents.{}.nmalloc", arena_key, bin).c_str());
        auto ndalloc = getJeMallocValue(fmt::format("stats.arenas.{}.lextents.{}.ndalloc", arena_key, bin).c_str());

        size_t col_num = 0;
        if (arena)
        {
            res_columns.at(col_num++)->insert(*arena);
            res_columns.at(col_num++)->insert(purpose);
        }
        res_columns.at(col_num++)->insert(bin_index);
        res_columns.at(col_num++)->insert(1);
        res_columns.at(col_num++)->insert(geometry.lextent_sizes[bin]);
        res_columns.at(col_num++)->insert(nmalloc);
        res_columns.at(col_num++)->insert(ndalloc);

        res_columns.at(col_num++)->insertDefault();
        res_columns.at(col_num++)->insertDefault();
        res_columns.at(col_num++)->insertDefault();
        res_columns.at(col_num++)->insertDefault();
        res_columns.at(col_num++)->insertDefault();
    }
}

}

static void fillJemallocBins(MutableColumns & res_columns, bool per_arena)
{
    /// Refresh the cached stats (note we have other places in the code, so atomicity is not guaranteed)
    Jemalloc::setValue<uint64_t>("epoch", 1);

    auto geometry = getJemallocBinsGeometry();

    if (!per_arena)
    {
        fillJemallocBinsRows(res_columns, geometry, {});
        return;
    }

    /// Dedicated arenas for long-lived state hold old allocations by design; the label
    /// lets fragmentation reports exclude them. Only already-created arenas are labeled:
    /// this is a read-only path and must not materialize the lazily-created ones.
    std::unordered_map<UInt64, const char *> purposes;
    for (unsigned index : JemallocMergeTreeArena::getArenaIndices())
        purposes[index] = "mergetree";
    if (auto index = JemallocJITArena::tryGetCreatedArenaIndex())
        purposes[*index] = "jit";
    if (auto index = JemallocCacheArena::tryGetCreatedArenaIndex())
        purposes[*index] = "cache";

    auto narenas = getJeMallocValue("arenas.narenas");
    for (UInt64 arena = 0; arena < narenas; ++arena)
    {
        bool initialized = false;
        if (!Jemalloc::tryGetValue(fmt::format("arena.{}.initialized", arena).c_str(), initialized) || !initialized)
            continue;

        auto purpose = purposes.find(arena);
        fillJemallocBinsRows(res_columns, geometry, arena, purpose == purposes.end() ? "" : purpose->second);
    }
}

#else

static void fillJemallocBins(MutableColumns &, bool)
{
    LOG_INFO(getLogger("StorageSystemJemallocBins"), "jemalloc is not enabled");
}

#endif // USE_JEMALLOC


StorageSystemJemallocBins::StorageSystemJemallocBins(const StorageID & table_id_, bool per_arena_)
    : StorageWithCommonVirtualColumns(table_id_)
    , per_arena(per_arena_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription(per_arena));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemJemallocBins::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

ColumnsDescription StorageSystemJemallocBins::getColumnsDescription(bool per_arena)
{
    ColumnsDescription description;
    if (per_arena)
    {
        description.add({ "arena", std::make_shared<DataTypeUInt32>(), "Index of the arena. Joins to `system.jemalloc_sampled_allocations.arena`."});
        description.add({ "purpose", std::make_shared<DataTypeString>(), "Purpose of a dedicated long-lived arena: 'mergetree' (part and table metadata), 'jit', 'cache'. Empty for general-purpose arenas. Dedicated arenas hold long-lived allocations by design; fragmentation reports usually exclude them."});
    }

    auto common = ColumnsDescription
    {
        { "index",          std::make_shared<DataTypeUInt16>(), "Index of the bin ordered by size."},
        { "large",          std::make_shared<DataTypeUInt8>(), "True for large allocations and False for small."},
        { "size",           std::make_shared<DataTypeUInt64>(), "Size of allocations in this bin."},
        { "allocations",    std::make_shared<DataTypeInt64>(), "Number of allocations."},
        { "deallocations",  std::make_shared<DataTypeInt64>(), "Number of deallocations."},
        { "nregs",          std::make_shared<DataTypeInt64>(), "Number of regions per slab."},
        { "curslabs",       std::make_shared<DataTypeInt64>(), "Current number of slabs."},
        { "curregs",        std::make_shared<DataTypeInt64>(), "Current number of regions for this size class."},
        { "slab_size",      std::make_shared<DataTypeUInt64>(), "Size of each slab in bytes. Zero for large size classes, which are not slab-based."},
        { "nonfull_slabs",  std::make_shared<DataTypeInt64>(), "Current number of slabs that contain at least one free region. Zero for large size classes."},
    };
    for (const auto & column : common)
        description.add(column);

    /// `waste` is clamped: the counters may come from two stats snapshots, making the
    /// difference transiently negative, and the cast to UInt64 would wrap it.
    description.setAliases({
        {"availregs", std::make_shared<DataTypeUInt64>(), "nregs * curslabs"},
        {"util", std::make_shared<DataTypeFloat64>(), "curregs / availregs"},
        {"waste", std::make_shared<DataTypeUInt64>(), "greatest(curslabs * slab_size - curregs * size, 0)"},
    });

    return description;
}

Pipe StorageSystemJemallocBins::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);
    MutableColumns res_columns = header.cloneEmptyColumns();

    fillJemallocBins(res_columns, per_arena);

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(header)), std::move(chunk)));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemJemallocBins) }
