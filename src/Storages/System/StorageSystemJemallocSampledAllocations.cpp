#include "config.h"
#include <Storages/System/SystemTableSourceRegistry.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/System/StorageSystemJemallocSampledAllocations.h>

#if USE_JEMALLOC
#    include <Core/Field.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/ReadHelpers.h>
#    include <Processors/ISource.h>
#    include <Processors/Sources/JemallocProfileSource.h>
#    include <Common/Jemalloc.h>
#    include <Common/StringUtils.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int NOT_IMPLEMENTED;
}

#if USE_JEMALLOC

namespace
{

/// Streams rows from the "f:" records of a jemalloc heap profile:
///   @ <pc> <pc> ...
///     t*: ...
///     f: <age_ns> <request_size> <usize> <szind> <arena_ind> <thr_uid>
/// Every "f:" record describes one live sampled allocation attributed to the
/// preceding "@" backtrace; all other lines are ignored.
class JemallocSampledAllocationsSource final : public ISource
{
public:
    JemallocSampledAllocationsSource(std::string filename_, const SharedHeader & header_, size_t max_block_size_)
        : ISource(header_)
        , filename(std::move(filename_))
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "JemallocSampledAllocations"; }

protected:
    Chunk generate() override
    {
        if (is_finished)
            return {};

        if (!file_input)
            file_input = std::make_unique<ReadBufferFromFile>(filename);

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
        std::string line;

        while (res_columns.at(0)->size() < max_block_size && !file_input->eof())
        {
            if (isCancelled())
            {
                is_finished = true;
                return {};
            }

            line.clear();
            readStringUntilNewlineInto(line, *file_input);
            file_input->tryIgnore(1);

            if (line.empty())
                continue;

            if (sample_interval == 0 && line.starts_with("heap_v2/"))
            {
                sample_interval = parseJemallocSamplingInterval(line);
                continue;
            }

            if (line[0] == '@')
            {
                /// A partially parsed backtrace would silently mis-attribute all the
                /// following allocation records, so require full consumption.
                bool fully_parsed = false;
                current_addresses = parseJemallocStackAddresses(line, &fully_parsed);
                if (!fully_parsed)
                    throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                        "Malformed backtrace line in heap profile '{}': '{}'", filename, line);
                current_stack.clear();
                continue;
            }

            std::string_view record(line);
            trimLeft(record);
            if (!record.starts_with("f:"))
                continue;
            record.remove_prefix(std::string_view("f:").size());

            /// Allocation records are emitted only under a backtrace block.
            if (current_addresses.empty())
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                    "Allocation record without a preceding backtrace in heap profile '{}'", filename);

            /// The `weight` alias divides by the interval; emitting rows with 0 would
            /// silently skip the sampling correction (often a factor of thousands).
            if (sample_interval == 0)
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                    "Heap profile '{}' contains allocation records before a valid heap_v2 header", filename);

            /// <age_ns> <request_size> <usize> <szind> <arena_ind> <thr_uid>
            /// Malformed records mean format drift or a truncated dump; dropping them
            /// would silently undercount, so fail instead.
            UInt64 fields[6];
            ReadBufferFromMemory buf(record.data(), record.size());
            for (auto & field : fields)
            {
                skipWhitespaceIfAny(buf);
                if (!tryReadIntText(field, buf))
                    throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                        "Malformed allocation record in heap profile '{}': '{}'", filename, line);
            }
            skipWhitespaceIfAny(buf);
            if (!buf.eof())
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                    "Unexpected trailing data in allocation record in heap profile '{}': '{}'", filename, line);

            /// Materialize the stack as Fields lazily: most stacks carry only "t:" records
            /// and never produce a row.
            if (current_stack.empty())
                current_stack.assign(current_addresses.begin(), current_addresses.end());

            size_t col_num = 0;
            res_columns.at(col_num++)->insert(current_stack);
            for (UInt64 field : fields)
                res_columns.at(col_num++)->insert(field);
            res_columns.at(col_num++)->insert(sample_interval);
        }

        if (file_input->eof())
            is_finished = true;

        size_t num_rows = res_columns.at(0)->size();
        if (num_rows == 0)
        {
            is_finished = true;
            return {};
        }

        return Chunk(std::move(res_columns), num_rows);
    }

private:
    std::string filename;
    size_t max_block_size;
    std::unique_ptr<ReadBufferFromFile> file_input;
    std::vector<UInt64> current_addresses;
    Array current_stack;
    UInt64 sample_interval = 0;
    bool is_finished = false;
};

}

#endif

StorageSystemJemallocSampledAllocations::StorageSystemJemallocSampledAllocations(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription());
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemJemallocSampledAllocations::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

ColumnsDescription StorageSystemJemallocSampledAllocations::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
            "Addresses of the allocating backtrace, leaf frame first (same convention as `system.trace_log.trace`). "
            "Symbolize with `addressToSymbol` and `demangle` (requires `allow_introspection_functions`)."},
        {"age_ns", std::make_shared<DataTypeUInt64>(),
            "Time in nanoseconds the allocation has been alive, relative to the moment the profile was flushed."},
        {"size", std::make_shared<DataTypeUInt64>(),
            "Requested allocation size in bytes."},
        {"usize", std::make_shared<DataTypeUInt64>(),
            "Usable size in bytes, i.e. the size class the request was rounded up to."},
        {"size_class", std::make_shared<DataTypeUInt16>(),
            "Index of the size class. Joins to `system.jemalloc_bins.index` for small size classes."},
        {"arena", std::make_shared<DataTypeUInt32>(),
            "Index of the arena the allocation was served from."},
        {"thr_uid", std::make_shared<DataTypeUInt64>(),
            "jemalloc-internal unique id of the allocating thread."},
        {"sample_interval", std::make_shared<DataTypeUInt64>(),
            "Average number of allocated bytes between samples, taken from the profile header. The same for all rows."},
    };

    description.setAliases({
        {"alloc_time", std::make_shared<DataTypeDateTime>(), "now() - intDiv(age_ns, 1000000000)"},
        {"weight", std::make_shared<DataTypeFloat64>(), "1 / (1 - exp(-usize / sample_interval))"},
    });

    return description;
}

Pipe StorageSystemJemallocSampledAllocations::read(
    [[maybe_unused]] const Names & column_names,
    [[maybe_unused]] const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    [[maybe_unused]] const size_t max_block_size,
    const size_t /*num_streams*/)
{
#if USE_JEMALLOC
    storage_snapshot->check(column_names);

    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);

    auto profile_path = std::string(Jemalloc::flushProfile("/tmp/jemalloc_clickhouse"));

    return Pipe(std::make_shared<JemallocSampledAllocationsSource>(
        std::move(profile_path), std::make_shared<const Block>(std::move(header)), max_block_size));
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "jemalloc is not enabled");
#endif
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemJemallocSampledAllocations) }
