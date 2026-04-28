#include <Storages/MergeTree/ANNIndex/VectorStreamWriter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    UInt64 computePartitionHash(const String & partition_id, UInt64 seed)
    {
        return sipHash64Keyed(seed, /*key1=*/ 0, partition_id.data(), partition_id.size());
    }
}

struct VectorStreamWriter::OpenPart
{
    DataPartPtr part;
    UInt64 partition_hash = 0;
    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
};

VectorStreamWriter::VectorStreamWriter(Params params_, DiskANNFbinWriter & fbin_writer_, LoggerPtr log_)
    : params(std::move(params_))
    , fbin_writer(fbin_writer_)
    , log(std::move(log_))
{
    if (params.vector_column_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VectorStreamWriter: vector_column_name must not be empty");
    if (params.expected_dim == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VectorStreamWriter: expected_dim must be > 0");
    if (!params.storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "VectorStreamWriter: storage must not be null");
    if (!params.storage_snapshot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "VectorStreamWriter: storage_snapshot must not be null");

    ensureVectorColumnType();
}

VectorStreamWriter::~VectorStreamWriter() = default;

void VectorStreamWriter::ensureVectorColumnType() const
{
    const auto & columns = params.storage_snapshot->metadata->getColumns();
    const auto & col = columns.getPhysical(params.vector_column_name);
    const auto * array_type = typeid_cast<const DataTypeArray *>(col.type.get());
    if (!array_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "VectorStreamWriter: column `{}` must be of type Array(Float32), got `{}`",
            params.vector_column_name, col.type->getName());

    const auto & nested = array_type->getNestedType();
    if (!typeid_cast<const DataTypeFloat32 *>(nested.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "VectorStreamWriter: column `{}` must be of type Array(Float32), got `{}`",
            params.vector_column_name, col.type->getName());
}

void VectorStreamWriter::openPart(DataPartPtr part)
{
    if (current)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter::openPart called while another part `{}` is still open",
            current->part ? current->part->name : std::string("<unknown>"));
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter::openPart called after finalizeFbin");
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "VectorStreamWriter::openPart: null part");

    const UInt64 partition_hash = computePartitionHash(part->info.getPartitionId(), params.hash_seed);

    /// Record coverage synchronously — we rely on it even if reading the part throws later.
    coverage_.addPart(partition_hash,
                      static_cast<UInt64>(part->info.min_block),
                      static_cast<UInt64>(part->info.max_block));

    Names columns_to_read = {
        params.vector_column_name,
        BlockNumberColumn::name,
        BlockOffsetColumn::name,
    };

    /// `createMergeTreeSequentialSource` expects a non-null AlterConversions. An empty one is a
    /// no-op for parts without pending renames/patches.
    auto alter_conversions = std::make_shared<AlterConversions>();

    MarkRanges full_ranges{MarkRange{0, part->getMarksCount()}};

    RangesInDataPart ranges_in_part(part, /*parent_part=*/ nullptr, /*part_index=*/ 0,
                                    /*part_starting_offset=*/ 0, full_ranges,
                                    /*read_hints=*/ {});

    Pipe pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        *params.storage,
        params.storage_snapshot,
        std::move(ranges_in_part),
        alter_conversions,
        /*merged_part_offsets=*/ nullptr,
        columns_to_read,
        /*mark_ranges=*/ std::nullopt,
        /*filtered_rows_count=*/ nullptr,
        /*apply_deleted_mask=*/ true,
        /*read_with_direct_io=*/ false,
        /*prefetch=*/ false);

    auto open = std::make_unique<OpenPart>();
    open->part = std::move(part);
    open->partition_hash = partition_hash;
    open->pipeline = QueryPipeline(std::move(pipe));
    open->executor = std::make_unique<PullingPipelineExecutor>(open->pipeline);
    current = std::move(open);
}

Chunk VectorStreamWriter::readNextChunk()
{
    if (!current)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter::readNextChunk called with no open part");

    Chunk chunk;
    while (current->executor->pull(chunk))
    {
        if (chunk.hasRows())
            return chunk;
        /// Skip empty chunks that the pipeline may produce as frame boundaries.
    }
    /// Pipeline drained.
    return {};
}

void VectorStreamWriter::writeChunk(const Chunk & chunk)
{
    if (!current)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter::writeChunk called with no open part");
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter::writeChunk called after finalizeFbin");

    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    const auto & header = current->executor->getHeader();
    const size_t vec_pos = header.getPositionByName(params.vector_column_name);
    const size_t bn_pos = header.getPositionByName(BlockNumberColumn::name);
    const size_t bo_pos = header.getPositionByName(BlockOffsetColumn::name);

    const auto & columns = chunk.getColumns();

    const auto * col_array = typeid_cast<const ColumnArray *>(columns[vec_pos].get());
    if (!col_array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "VectorStreamWriter: vector column `{}` is not a ColumnArray", params.vector_column_name);

    const auto & nested = col_array->getData();
    const auto * col_float32 = typeid_cast<const ColumnFloat32 *>(&nested);
    if (!col_float32)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "VectorStreamWriter: vector column `{}` must have Float32 element type",
            params.vector_column_name);

    const auto & offsets = col_array->getOffsets();
    const auto & float_data = col_float32->getData();

    /// `_block_number` / `_block_offset` are read as full `ColumnUInt64` for parts that
    /// physically materialise them (merged parts), and as `ColumnConst(ColumnUInt64)` for
    /// parts where the sequential source synthesises the virtual const from `part.info`
    /// (non-merged INSERT parts). Expand any const wrapper before asserting the scalar type
    /// so we can always index row-wise below.
    auto bn_full = columns[bn_pos]->convertToFullIfNeeded();
    auto bo_full = columns[bo_pos]->convertToFullIfNeeded();
    const auto * col_bn = typeid_cast<const ColumnUInt64 *>(bn_full.get());
    const auto * col_bo = typeid_cast<const ColumnUInt64 *>(bo_full.get());
    if (!col_bn || !col_bo)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter: `_block_number`/`_block_offset` virtual columns have unexpected type "
            "(bn_raw={}, bo_raw={}, bn_full={}, bo_full={})",
            columns[bn_pos]->getName(), columns[bo_pos]->getName(),
            bn_full->getName(), bo_full->getName());

    const auto & bn_data = col_bn->getData();
    const auto & bo_data = col_bo->getData();

    for (size_t row = 0; row < num_rows; ++row)
    {
        const size_t begin = row == 0 ? 0 : offsets[row - 1];
        const size_t end = offsets[row];
        const size_t row_dim = end - begin;
        if (row_dim != params.expected_dim)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "VectorStreamWriter: vector at row {} has dim {}, expected {}",
                row, row_dim, params.expected_dim);

        fbin_writer.appendRow(&float_data[begin], row_dim);
        id_map_writer.append(PartRowId{
            /*partition_hash=*/ current->partition_hash,
            /*block_number=*/ bn_data[row],
            /*block_offset=*/ bo_data[row],
        });
    }
}

void VectorStreamWriter::closePart()
{
    current.reset();
}

void VectorStreamWriter::finalizeFbin()
{
    if (finalized)
        return;
    if (current)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "VectorStreamWriter::finalizeFbin called while a part is still open");

    fbin_writer.finalize();
    finalized = true;

    LOG_DEBUG(log,
        "VectorStreamWriter finalized: {} rows, {} partitions in coverage",
        id_map_writer.size(), coverage_.partitionCount());
}

void VectorStreamWriter::dumpFromParts(const std::vector<DataPartPtr> & parts)
{
    for (const auto & part : parts)
    {
        openPart(part);
        while (true)
        {
            Chunk chunk = readNextChunk();
            if (!chunk.hasRows())
                break;
            writeChunk(chunk);
        }
        closePart();
    }
    finalizeFbin();
}

}
