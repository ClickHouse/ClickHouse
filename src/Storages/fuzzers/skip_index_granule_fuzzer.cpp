#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>


/// The file of a skip index, which a server reads granule by granule from its disk, from a replica
/// on a fetch, or from a part attached by a user, together with the definition of the index.
/// The `text` index is not here: it reads its dictionary and postings from the files of a part,
/// see `mergetree_part_fuzzer`. The input is:
///
///     String columns          the columns of the table, as in `CREATE TABLE`: `u UInt64, s String`
///     String index            `<name> <expression> TYPE <type>`, as after `INDEX` in `CREATE TABLE`
///     UInt8 flags             bit 1: version 2 (`.idx2`) of `minmax`; bit 2: the bulk reader;
///                             bit 4: descending top-K of `minmax`; bits 8 and 16: its granularity
///     the index file

using namespace DB;

namespace
{

constexpr size_t max_granules = 1024;

ContextMutablePtr context;

/// Mutations of the index file are the most frequent, so the index of the previous input is kept.
struct CachedIndex
{
    String columns;
    String definition;
    MergeTreeIndexPtr index;
};

CachedIndex cached;

MergeTreeIndexPtr getIndex(const String & columns_definition, const String & index_definition)
{
    if (columns_definition == cached.columns && index_definition == cached.definition)
        return cached.index;

    /// A definition that throws stays cached as no index.
    cached = {columns_definition, index_definition, nullptr};
    auto columns = parseColumnsListFromString(columns_definition, context);
    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->setColumns(columns);

    MergeTreeSettings settings;
    auto index = IndicesDescription::parse(index_definition, columns, /*escape_index_filenames=*/true, context).at(0);
    /// As for the metadata of a table that the server loads from its disk.
    MergeTreeIndexFactory::instance().validate(index, /*attach=*/true, settings);
    cached.index = MergeTreeIndexFactory::instance().get(metadata, index, settings);
    return cached.index;
}

}

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerFunctions();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        ReadBufferFromMemory in(data, size);
        String columns_definition;
        String index_definition;
        UInt8 flags = 0;
        readStringBinary(columns_definition, in, 4096);
        readStringBinary(index_definition, in, 4096);
        readBinary(flags, in);

        auto index_ptr = getIndex(columns_definition, index_definition);
        if (!index_ptr || index_ptr->isTextIndex() || index_ptr->isInert())
            return 0;
        const auto & index = *index_ptr;

        const auto & sample_block = index.index.sample_block;
        const bool is_minmax = index.index.type == "minmax";
        /// Only minmax has a second version; the readers of the others throw `LOGICAL_ERROR` on anything but 1.
        const MergeTreeIndexVersion version = (is_minmax && (flags & 1)) ? 2 : 1;
        const bool minmax_bulk = is_minmax && sample_block.columns() == 1 && !sample_block.getByPosition(0).type->isNullable();
        const bool bulk = (flags & 2) && (index.supportsBulkFiltering() || minmax_bulk);

        if (bulk)
        {
            /// The top-K read of a single-column minmax index makes its bulk granules itself.
            MergeTreeIndexBulkGranulesPtr granules;
            if (is_minmax)
                granules = std::make_shared<MergeTreeIndexBulkGranulesMinMax>(
                    index.index.name, sample_block, /*index_granularity=*/1 + flags / 8 % 4,
                    (flags & 4) ? -1 : 1, /*size_hint=*/0, /*last_part_granule=*/max_granules);
            else
                granules = index.createIndexBulkGranules();

            for (size_t granule_num = 0; granule_num < max_granules && !in.eof(); ++granule_num)
                granules->deserializeBinary(granule_num, in, version);
            std::ignore = granules->numValues();
        }
        else
        {
            /// `MergeTreeIndexReader` reuses one granule for all the marks it reads.
            auto granule = index.createIndexGranule();
            for (size_t granule_num = 0; granule_num < max_granules && !in.eof(); ++granule_num)
            {
                granule->deserializeBinary(in, version);
                std::ignore = granule->empty();
                std::ignore = granule->memoryUsageBytes();
            }
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed definitions and granules are rejected with exceptions.
    }

    return 0;
}
