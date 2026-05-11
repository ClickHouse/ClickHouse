#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/DiskANNFbinWriter.h>
#include <Storages/MergeTree/ANNIndex/VectorStreamWriter.h>
#include <Storages/MergeTree/tests/MergeTreeTestHarness.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>

namespace
{
namespace fs = std::filesystem;

fs::path makeUniqueTempDir(const std::string & name)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = fs::temp_directory_path() / ("clickhouse-vsw-" + name + "-" + std::to_string(now));
    fs::create_directories(path);
    return path;
}

class TempDirScope
{
public:
    explicit TempDirScope(const std::string & name) : path(makeUniqueTempDir(name)) {}
    ~TempDirScope() { std::error_code ec; fs::remove_all(path, ec); }
    fs::path path;
};

std::vector<std::vector<float>> makeVectors(size_t num_rows, size_t dim, float base)
{
    std::vector<std::vector<float>> out(num_rows, std::vector<float>(dim, 0.f));
    for (size_t r = 0; r < num_rows; ++r)
        for (size_t c = 0; c < dim; ++c)
            out[r][c] = base + static_cast<float>(r) + static_cast<float>(c);
    return out;
}

std::vector<char> readFileBytes(const fs::path & path)
{
    std::ifstream in(path, std::ios::binary);
    in.seekg(0, std::ios::end);
    std::vector<char> buf(static_cast<size_t>(in.tellg()));
    in.seekg(0);
    if (!buf.empty())
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
    return buf;
}
}

using namespace DB;

/// Scope guard that owns a `TestStorage`. Destructor guarantees `flushAndShutdown` is called,
/// even if the test throws, so that the MergeTree background threads do not outlive the test.
namespace
{
class StorageScope
{
public:
    explicit StorageScope(MergeTreeTestHarness::TestStorage s_) : s(std::move(s_)) {}
    ~StorageScope() { if (s.storage) s.storage->flushAndShutdown(); }

    StorageScope(const StorageScope &) = delete;
    StorageScope & operator=(const StorageScope &) = delete;

    MergeTreeTestHarness::TestStorage & get() { return s; }

private:
    MergeTreeTestHarness::TestStorage s;
};

DataPartsVector activeParts(const StorageMergeTree & storage)
{
    return storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active});
}
}

TEST(VectorStreamWriterTest, SingleSmallPart)
{
    TempDirScope disk("single-part-disk");
    TempDirScope fbin_dir("single-part-fbin");

    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk.path.string(), "store/vsw_single", "emb", /*dim=*/ 4, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    const size_t num_rows = 5;
    const UInt32 partition_value = 7;
    auto vectors = makeVectors(num_rows, 4, /*base=*/ 10.0f);
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, num_rows, partition_value, vectors);

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 1u);

    /// Open an .fbin output file outside the table directory to isolate the writer under test.
    const auto fbin_path = (fbin_dir.path / "vectors.fbin").string();
    {
        auto wbuf = std::make_unique<WriteBufferFromFile>(fbin_path, 4096);
        DiskANNFbinWriter fbin_writer(*wbuf, 4);

        auto context = Context::createCopy(getContext().context);
        auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
        auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

        VectorStreamWriter::Params p;
        p.vector_column_name = setup.vec_column_name;
        p.expected_dim = 4;
        p.hash_seed = 0xDEADBEEF;
        p.storage = setup.storage.get();
        p.storage_snapshot = storage_snapshot;

        VectorStreamWriter writer(std::move(p), fbin_writer, getLogger("VectorStreamWriterTest"));
        writer.dumpFromParts(parts);

        EXPECT_EQ(writer.getWrittenRows(), num_rows);
        EXPECT_EQ(writer.coverage().partitionCount(), 1u);
        EXPECT_EQ(writer.idMapWriter().size(), num_rows);

        wbuf->finalize();
    }

    /// .fbin layout: [npoints u32][dim u32][rows × dim × f32].
    auto bytes = readFileBytes(fbin_path);
    ASSERT_EQ(bytes.size(), 8u + num_rows * 4u * sizeof(float));

    uint32_t header_npoints = 0;
    uint32_t header_dim = 0;
    std::memcpy(&header_npoints, bytes.data() + 0, sizeof(header_npoints));
    std::memcpy(&header_dim, bytes.data() + 4, sizeof(header_dim));
    EXPECT_EQ(header_npoints, num_rows);
    EXPECT_EQ(header_dim, 4u);

    for (size_t r = 0; r < num_rows; ++r)
    {
        for (size_t c = 0; c < 4; ++c)
        {
            float got = 0.f;
            const size_t off = 8 + (r * 4 + c) * sizeof(float);
            std::memcpy(&got, bytes.data() + off, sizeof(got));
            EXPECT_FLOAT_EQ(got, vectors[r][c]) << "row=" << r << " col=" << c;
        }
    }
}

TEST(VectorStreamWriterTest, TwoPartsTwoPartitionsCoverage)
{
    TempDirScope disk("two-parts-disk");
    TempDirScope fbin_dir("two-parts-fbin");

    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk.path.string(), "store/vsw_two_parts", "emb", /*dim=*/ 4, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 3, /*partition_value=*/ 1, makeVectors(3, 4, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, /*partition_value=*/ 2, makeVectors(2, 4, 2.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 2u);

    const auto fbin_path = (fbin_dir.path / "vectors.fbin").string();
    auto wbuf = std::make_unique<WriteBufferFromFile>(fbin_path, 4096);
    DiskANNFbinWriter fbin_writer(*wbuf, 4);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    VectorStreamWriter::Params p;
    p.vector_column_name = setup.vec_column_name;
    p.expected_dim = 4;
    p.storage = setup.storage.get();
    p.storage_snapshot = storage_snapshot;

    VectorStreamWriter writer(std::move(p), fbin_writer, getLogger("VectorStreamWriterTest"));
    writer.dumpFromParts(parts);
    wbuf->finalize();

    EXPECT_EQ(writer.getWrittenRows(), 5u);
    EXPECT_EQ(writer.coverage().partitionCount(), 2u);
}

TEST(VectorStreamWriterTest, RejectsDimensionMismatch)
{
    TempDirScope disk("dim-mismatch-disk");
    TempDirScope fbin_dir("dim-mismatch-fbin");

    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk.path.string(), "store/vsw_dim_mismatch", "emb", /*dim=*/ 4, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    /// Insert rows whose actual array length (3) does not match the configured `expected_dim` (4).
    std::vector<std::vector<float>> vectors;
    vectors.push_back({1.f, 2.f, 3.f});
    vectors.push_back({4.f, 5.f, 6.f});
    /// Directly bypass the harness dim check: the storage accepts arbitrary-length Array(Float32).
    {
        auto vec_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
        auto vec_col = vec_type->createColumn();
        auto & arr = assert_cast<ColumnArray &>(*vec_col);
        auto & nested_pod = assert_cast<ColumnFloat32 &>(arr.getData()).getData();
        auto & offsets = arr.getOffsets();
        for (const auto & v : vectors)
        {
            const size_t old = nested_pod.size();
            nested_pod.resize(old + v.size());
            std::memcpy(nested_pod.data() + old, v.data(), v.size() * sizeof(float));
            offsets.push_back(nested_pod.size());
        }
        auto pk_type = std::make_shared<DataTypeUInt32>();
        auto pk_col = pk_type->createColumn();
        assert_cast<ColumnUInt32 &>(*pk_col).getData().resize_fill(vectors.size(), 9);

        Block block;
        block.insert({std::move(vec_col), vec_type, setup.vec_column_name});
        block.insert({std::move(pk_col), pk_type, setup.partition_key_column});

        auto context = Context::createCopy(getContext().context);
        auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
        auto sink = setup.storage->write(nullptr, metadata_snapshot, context, false);
        auto header = sink->getInputs().front().getSharedHeader();
        Chain chain;
        chain.addSink(sink);
        chain.addSource(std::make_shared<AddDeduplicationInfoTransform>(header));
        QueryPipeline pipeline(std::move(chain));
        PushingPipelineExecutor executor(pipeline);
        executor.push(std::move(block));
        executor.finish();
    }

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 1u);

    const auto fbin_path = (fbin_dir.path / "vectors.fbin").string();
    auto wbuf = std::make_unique<WriteBufferFromFile>(fbin_path, 4096);
    DiskANNFbinWriter fbin_writer(*wbuf, 4);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    VectorStreamWriter::Params p;
    p.vector_column_name = setup.vec_column_name;
    p.expected_dim = 4;
    p.storage = setup.storage.get();
    p.storage_snapshot = storage_snapshot;

    VectorStreamWriter writer(std::move(p), fbin_writer, getLogger("VectorStreamWriterTest"));
    EXPECT_THROW(writer.dumpFromParts(parts), Exception);
}

TEST(VectorStreamWriterTest, RejectsArrayFloat64Column)
{
    /// Reuse the standard harness but swap the column type after construction: the table has an
    /// `Array(Float64)` column. The writer must reject it up-front in the constructor.
    TempDirScope disk("f64-col-disk");
    TempDirScope fbin_dir("f64-col-fbin");

    auto & ch = getMutableContext();
    auto context = Context::createCopy(ch.context);

    fs::create_directories(disk.path);
    const std::string disk_name = "test_disk_" + disk.path.filename().string();
    auto test_disk = context->getOrCreateDisk(disk_name, [&](const DisksMap &) -> DiskPtr {
        return std::make_shared<DiskLocal>(disk_name, disk.path.string() + "/");
    });
    test_disk->createDirectories("store/vsw_f64/");

    ColumnsDescription columns;
    columns.add(ColumnDescription("emb", std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>())));
    columns.add(ColumnDescription("pk", std::make_shared<DataTypeUInt32>()));

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    auto order_by = makeASTFunction("tuple");
    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(order_by, metadata.columns, context, {});
    metadata.primary_key = KeyDescription::getKeyFromAST(order_by, metadata.columns, context);
    metadata.primary_key.definition_ast = nullptr;
    metadata.partition_key = KeyDescription::getKeyFromAST(nullptr, metadata.columns, context);

    auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
    auto partition_key_list = metadata.partition_key.expression_list_ast->clone();
    metadata.minmax_count_projection.emplace(
        ProjectionDescription::getMinMaxCountProjection(columns, partition_key_list, minmax_columns,
            metadata.primary_key, &metadata.partition_key, context));

    auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    storage_settings->set("disk", Field(disk_name));

    auto storage = std::make_shared<StorageMergeTree>(
        StorageID("test_db", "f64_table"),
        std::string("store/vsw_f64/"),
        metadata,
        LoadingStrictnessLevel::CREATE,
        context,
        /*date_column_name=*/ "",
        MergeTreeData::MergingParams{},
        std::move(storage_settings));
    storage->startup();

    auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);

    const auto fbin_path = (fbin_dir.path / "vectors.fbin").string();
    auto wbuf = std::make_unique<WriteBufferFromFile>(fbin_path, 4096);
    DiskANNFbinWriter fbin_writer(*wbuf, 4);

    VectorStreamWriter::Params p;
    p.vector_column_name = "emb";
    p.expected_dim = 4;
    p.storage = storage.get();
    p.storage_snapshot = storage_snapshot;

    EXPECT_THROW(VectorStreamWriter(std::move(p), fbin_writer, getLogger("t")), Exception);

    storage->flushAndShutdown();
}

TEST(VectorStreamWriterTest, EmptyPartsList)
{
    TempDirScope disk("empty-parts-disk");
    TempDirScope fbin_dir("empty-parts-fbin");

    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk.path.string(), "store/vsw_empty", "emb", /*dim=*/ 4, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    const auto fbin_path = (fbin_dir.path / "vectors.fbin").string();
    auto wbuf = std::make_unique<WriteBufferFromFile>(fbin_path, 4096);
    DiskANNFbinWriter fbin_writer(*wbuf, 4);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    VectorStreamWriter::Params p;
    p.vector_column_name = setup.vec_column_name;
    p.expected_dim = 4;
    p.storage = setup.storage.get();
    p.storage_snapshot = storage_snapshot;

    VectorStreamWriter writer(std::move(p), fbin_writer, getLogger("VectorStreamWriterTest"));
    writer.dumpFromParts({});
    wbuf->finalize();

    EXPECT_EQ(writer.getWrittenRows(), 0u);
    EXPECT_EQ(writer.coverage().partitionCount(), 0u);

    /// The `.fbin` should still have a well-formed header with `npoints = 0`.
    auto bytes = readFileBytes(fbin_path);
    ASSERT_EQ(bytes.size(), 8u);
    uint32_t npoints = 0;
    uint32_t dim = 0;
    std::memcpy(&npoints, bytes.data() + 0, sizeof(npoints));
    std::memcpy(&dim, bytes.data() + 4, sizeof(dim));
    EXPECT_EQ(npoints, 0u);
    EXPECT_EQ(dim, 4u);
}
