#include <Storages/MutationCommands.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Storages/AlterCommands.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/RocksDB/RocksDBSettings.h>
#include <Storages/StorageFactory.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/castColumn.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupEntryReference.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IBackupEntriesLazyBatch.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Core/Settings.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/SharedLockGuard.h>
#include <Common/JSONBuilder.h>
#include <Common/Logger.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

#include <Disks/DiskLocal.h>
#include <IO/SharedThreadPools.h>
#include <base/sort.h>

#include <rocksdb/advanced_options.h>
#include <rocksdb/compression_type.h>
#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/db_ttl.h>

#include <cstddef>
#include <filesystem>
#include <memory>
#include <utility>

#include <fmt/ranges.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool optimize_trivial_approximate_count_query;
extern const SettingsUInt64 max_compress_block_size;
}

namespace RocksDBSetting
{
extern const RocksDBSettingsBool optimize_for_bulk_insert;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_RESTORE_TABLE;
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ROCKSDB_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int TABLE_IS_DROPPED;
extern const int TYPE_MISMATCH;
}

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using RocksDBOptions = std::unordered_map<std::string, std::string>;

static RocksDBOptions getOptionsFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    RocksDBOptions options;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        options[key] = config.getString(key_path);
    }

    return options;
}

class EmbeddedRocksDBSource final : public ISource
{
public:
    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SharedHeader header,
        FieldVectorPtr keys_,
        FieldVector::const_iterator begin_,
        FieldVector::const_iterator end_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , physical_header(storage_snapshot_->metadata->getSampleBlock())
        , keys(keys_)
        , begin(begin_)
        , end(end_)
        , it(begin)
        , max_block_size(max_block_size_)
    {
    }

    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SharedHeader header,
        std::unique_ptr<rocksdb::Iterator> iterator_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , physical_header(storage_snapshot_->metadata->getSampleBlock())
        , iterator(std::move(iterator_))
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return storage.getName(); }

    Chunk generate() override
    {
        Block block;
        if (keys)
            block = generateWithKeys();
        else
            block = generateFullScan();

        if (block.empty())
            return {};

        fillVirtualColumns(block);
        return Chunk(block.getColumns(), block.rows());
    }

    void fillVirtualColumns([[maybe_unused]] Block & block) const
    {
        auto virtual_columns = storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader);
        if (!virtual_columns.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported virtual columns {}", virtual_columns.getNames());
    }

    Block generateWithKeys()
    {
        if (it >= end)
        {
            it = {};
            return {};
        }
        auto raw_keys = serializeKeysToRawString(it, end, storage.getPrimaryKeyTypes(), max_block_size);
        return storage.getBySerializedKeys(raw_keys, nullptr, physical_header);
    }

    Block generateFullScan()
    {
        if (!iterator->Valid())
            return {};

        MutableColumns columns = physical_header.cloneEmptyColumns();
        for (size_t rows = 0; iterator->Valid() && rows < max_block_size; ++rows, iterator->Next())
        {
            fillColumns(iterator->key(), storage.getPrimaryKeyPos(), physical_header, columns);
            fillColumns(iterator->value(), storage.getValueColumnPos(), physical_header, columns);
        }

        if (!iterator->status().ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Engine {} got error while seeking key value data: {}",
                getName(),
                iterator->status().ToString());
        }

        return physical_header.cloneWithColumns(std::move(columns));
    }

private:
    const StorageEmbeddedRocksDB & storage;
    StorageSnapshotPtr storage_snapshot;
    Block physical_header;

    /// For key scan
    FieldVectorPtr keys = nullptr;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    /// For full scan
    std::unique_ptr<rocksdb::Iterator> iterator = nullptr;

    const size_t max_block_size;
};

VirtualColumnsDescription StorageEmbeddedRocksDB::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

StorageEmbeddedRocksDB::StorageEmbeddedRocksDB(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    LoadingStrictnessLevel mode,
    ContextPtr context_,
    std::unique_ptr<RocksDBSettings> settings_,
    Names primary_keys_,
    Int32 ttl_,
    String rocksdb_dir_,
    bool read_only_)
    : StorageWithCommonVirtualColumns(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(getLogger(fmt::format("StorageEmbeddedRocksDB ({})", getStorageID().getNameForLogs())))
    , primary_keys{std::move(primary_keys_)}
    , rocksdb_dir(std::move(rocksdb_dir_))
    , ttl(ttl_)
    , read_only(read_only_)
{
    setInMemoryMetadata(metadata_.withVirtuals(createVirtuals()));
    setSettings(std::move(settings_));

    if (rocksdb_dir.empty())
    {
        /// We create tables under the database directory by default and enforce user_files path check for explicitly declared paths
        rocksdb_dir = context_->getPath() + relative_data_path_;
    }
    else
    {
        bool is_local = context_->getApplicationType() == Context::ApplicationType::LOCAL;
        fs::path user_files_path = is_local ? "" : fs::canonical(getContext()->getUserFilesPath());
        if (fs::path(rocksdb_dir).is_relative())
            rocksdb_dir = user_files_path / rocksdb_dir;
        rocksdb_dir = fs::absolute(rocksdb_dir).lexically_normal();

        if (!is_local && !fileOrSymlinkPathStartsWith(fs::path(rocksdb_dir), user_files_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path must be inside user-files path: {}", user_files_path.string());
    }

    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        fs::create_directories(rocksdb_dir);
    }

    auto metadata_snapshot = getInMemoryMetadataPtr(context_, false);
    const auto sample_block = metadata_snapshot->getSampleBlock();
    primary_key_pos.reserve(primary_keys.size());
    primary_key_types.reserve(primary_keys.size());
    std::vector<bool> is_pk(sample_block.columns());
    for (const auto & key_name : primary_keys)
    {
        const size_t key_pos{sample_block.getPositionByName(key_name)};
        primary_key_pos.push_back(key_pos);
        is_pk[key_pos] = true;
        primary_key_types.push_back(sample_block.getByPosition(key_pos).type);
    }
    value_column_pos.reserve(primary_keys.size() - primary_key_pos.size());
    for (size_t i = 0; i < is_pk.size(); ++i)
    {
        if (!is_pk[i])
            value_column_pos.push_back(i);
    }

    initDB();
}

StorageEmbeddedRocksDB::~StorageEmbeddedRocksDB() = default;

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    std::lock_guard lock(rocksdb_ptr_mx);
    /// rocksdb_ptr may already be null if a previous truncate() emptied the directory and
    /// the following initDB() threw (e.g. a read_only table whose data was wiped).
    if (rocksdb_ptr)
    {
        rocksdb_ptr->Close();
        rocksdb_ptr = nullptr;
    }

    (void)fs::remove_all(rocksdb_dir);
    fs::create_directories(rocksdb_dir);
    initDB();
}

void StorageEmbeddedRocksDB::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    if (commands.empty())
        return;

    if (commands.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations cannot be combined for EmbeddedRocksDB");

    const auto command_type = commands.front().type;
    if (command_type != MutationCommand::Type::UPDATE && command_type != MutationCommand::Type::DELETE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only DELETE and UPDATE mutation supported for EmbeddedRocksDB");
}

void StorageEmbeddedRocksDB::mutate(const MutationCommands & commands, ContextPtr context_)
{
    if (commands.empty())
        return;

    chassert(commands.size() == 1);

    auto metadata_snapshot = getInMemoryMetadataPtr(context_, false);
    auto physical_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context_);

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        MutationsInterpreter::Settings mutation_settings(true);
        mutation_settings.return_all_columns = true;
        mutation_settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, physical_columns, context_, mutation_settings);

        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);


        Block block;
        while (executor.pull(block))
        {
            Columns columns;
            DataTypes types;
            columns.reserve(primary_key_pos.size());
            types.reserve(primary_key_pos.size());
            for (const auto pos : primary_key_pos)
            {
                auto & column_type_name = block.getByPosition(pos);
                columns.push_back(column_type_name.column);
                types.push_back(column_type_name.type);
            }

            const auto size = block.rows();
            rocksdb::WriteBatch batch;
            WriteBufferFromOwnString wb_key;
            for (size_t i = 0; i < size; ++i)
            {
                wb_key.restart();

                for (size_t j = 0; j < columns.size(); ++j)
                {
                    types[j]->getDefaultSerialization()->serializeBinary(*columns[j], i, wb_key, {});
                }
                auto status = batch.Delete(wb_key.str());
                if (!status.ok())
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
            }

            {
                SharedLockGuard lock(rocksdb_ptr_mx);
                if (!rocksdb_ptr)
                    throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table is dropped");
                auto status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
                if (!status.ok())
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
            }
        }

        return;
    }

    chassert(commands.front().type == MutationCommand::Type::UPDATE);
    auto alter = commands.front().ast();
    const auto column_to_update = getColumnToUpdateExpression(*alter);
    for (const auto & key_name : primary_keys)
    {
        if (column_to_update.contains(key_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key cannot be updated (cannot update column {})", key_name);
    }

    MutationsInterpreter::Settings mutation_settings(true);
    mutation_settings.return_all_columns = true;
    mutation_settings.return_mutated_rows = true;

    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, physical_columns, context_, mutation_settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    auto sink = std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);

    Block block;
    while (executor.pull(block))
    {
        auto chunk = Chunk(block.getColumns(), block.rows());
        sink->consume(chunk);
    }
}

void StorageEmbeddedRocksDB::drop()
{
    std::lock_guard lock(rocksdb_ptr_mx);
    /// rocksdb_ptr may be null if the handle was never opened or was released by a failed
    /// truncate(); dropping such a table must not dereference it.
    if (rocksdb_ptr)
    {
        rocksdb_ptr->Close();
        rocksdb_ptr = nullptr;
    }
}

bool StorageEmbeddedRocksDB::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & /* deduplicate_by_columns */,
    bool cleanup,
    ContextPtr /*context*/)
{
    if (partition)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition cannot be specified when optimizing table of type EmbeddedRocksDB");

    if (final)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FINAL cannot be specified when optimizing table of type EmbeddedRocksDB");

    if (deduplicate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DEDUPLICATE cannot be specified when optimizing table of type EmbeddedRocksDB");

    if (cleanup)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEANUP cannot be specified when optimizing table of type EmbeddedRocksDB");

    SharedLockGuard lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return true;
    rocksdb::CompactRangeOptions compact_options;
    auto status = rocksdb_ptr->CompactRange(compact_options, nullptr, nullptr);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Compaction failed: {}", status.ToString());
    return true;
}

/// Single file holding all key-value pairs of the table as (raw key, raw value) length-prefixed
/// binary records. RocksDB already stores keys/values as raw serialized strings, so no
/// (de)serialization is required - we copy the bytes verbatim.
///
/// The scan and the restore write both go through GetRootDB(): for a ttl > 0 table rocksdb_ptr is
/// a DBWithTTL whose iterator strips the trailing 4-byte creation timestamp and whose Write()
/// appends a fresh one, which would reset every row's expiration on restore. GetRootDB() gives the
/// underlying DB, so the timestamp suffix is copied verbatim in both directions and the original
/// expiration state is preserved. For a non-ttl table GetRootDB() is the DB itself, so the bytes
/// are identical to a plain scan/write.
static constexpr std::string_view rocksdb_backup_data_filename = "data.bin";

/// Small companion file recording the source table's ttl argument. The backed-up value bytes are
/// ttl-format-dependent (a ttl > 0 table is a DBWithTTL whose values carry a trailing 4-byte creation
/// timestamp; a ttl = 0 table has none), and even between two ttl > 0 tables the ttl sets each row's
/// expiration window. Restore compares this against the target table's ttl and rejects a mismatch, so the
/// RESTORE ... AS <writable_table> / allow_different_table_def path (which skips RestorerFromBackup's
/// create-query compatibility check) cannot silently replay incompatible bytes or shift every row's expiry.
static constexpr std::string_view rocksdb_backup_ttl_filename = "ttl.txt";

/// Flush the restore WriteBatch once it reaches this many bytes so a large backup does not require a
/// full in-RAM copy of the table (EmbeddedRocksDB is an on-disk engine).
static constexpr size_t rocksdb_restore_batch_flush_bytes = 64 * 1024 * 1024;

/// Lazily dumps all key-value pairs of a RocksDB table into a single compressed backup entry.
class EmbeddedRocksDBBackup : public IBackupEntriesLazyBatch, boost::noncopyable
{
public:
    EmbeddedRocksDBBackup(
        std::shared_ptr<const StorageEmbeddedRocksDB> storage_,
        const String & data_path_in_backup,
        TemporaryDataOnDiskScopePtr tmp_data_)
        : storage(std::move(storage_))
        , tmp_data(std::move(tmp_data_))
    {
        file_path = fs::path(data_path_in_backup) / rocksdb_backup_data_filename;
    }

private:
    size_t getSize() const override { return 1; }

    const String & getName(size_t i) const override
    {
        chassert(i == 0);
        return file_path;
    }

    BackupEntries generate() override
    {
        auto data_out = std::make_unique<TemporaryDataBuffer>(tmp_data);

        {
            /// A shared lock keeps the rocksdb handle alive for the whole scan without blocking
            /// concurrent inserts (they take a shared lock too); it only excludes drop/truncate.
            /// The iterator reads from an implicit snapshot, so the dump is consistent.
            SharedLockGuard lock(storage->rocksdb_ptr_mx);
            if (!storage->rocksdb_ptr)
                throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table is dropped");

            /// GetRootDB() bypasses the DBWithTTL wrapper so raw values keep their TTL timestamp suffix.
            std::unique_ptr<rocksdb::Iterator> iterator(storage->rocksdb_ptr->GetRootDB()->NewIterator(rocksdb::ReadOptions()));
            for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
            {
                writeStringBinary(iterator->key().ToStringView(), *data_out);
                writeStringBinary(iterator->value().ToStringView(), *data_out);
            }

            if (!iterator->status().ok())
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB iterator error: {}", iterator->status().ToString());
        }

        data_out->finishWriting();
        return {{file_path, std::make_shared<BackupEntryFromAppendOnlyFile>(std::move(data_out))}};
    }

    std::shared_ptr<const StorageEmbeddedRocksDB> storage;
    TemporaryDataOnDiskScopePtr tmp_data;
    String file_path;
};

/// Election id used to pick a single owner among tables sharing one rocksdb_dir. A writable table (there
/// is at most one, enforced by RocksDB's LOCK) always sorts above every read_only table (the leading
/// "1_rw"/"0_ro" tag dominates the comparison), so it is elected when present. Its handle also sees the
/// freshest data (including the unflushed memtable), which is what the backup must capture. The
/// fully-qualified table name keeps ids unique across distinct tables: unlike the storage uuid it is never
/// Nil for Ordinary-database tables (InterpreterCreateQuery clears the uuid there), so two unrelated
/// EmbeddedRocksDB tables never collide on one election znode.
String StorageEmbeddedRocksDB::backupElectionId() const
{
    return fmt::format("{}_{}", read_only ? "0_ro" : "1_rw", getStorageID().getFullTableName());
}

/// Canonical fingerprint of the on-disk byte layout. restoreDataImpl() replays raw serialized (key, value)
/// bytes verbatim, and fillColumns() later decodes them with the TARGET table's metadata: the key bytes are
/// the primary-key columns serialized in primary-key order, the value bytes are the remaining physical columns
/// serialized in physical order. So the bytes are only interpretable by a table with the same physical column
/// types in the same order and the same primary-key column set/order. A same-ttl target that differs in
/// PK/value column types or ordering would decode the bytes into a wrong (unreadable or silently incorrect)
/// table, and the RESTORE ... AS <writable_table> / allow_different_table_def workaround skips
/// RestorerFromBackup's create-query compatibility check, so this fingerprint is what catches it. Table name and
/// the read_only flag are intentionally excluded: neither affects the byte layout, and RESTORE ... AS restores
/// into a differently-named (and writable) table on purpose.
String StorageEmbeddedRocksDB::backupSchemaFingerprint() const
{
    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    const auto sample_block = metadata_snapshot->getSampleBlock();

    WriteBufferFromOwnString out;
    out << "key:";
    for (size_t i = 0; i < primary_key_pos.size(); ++i)
    {
        const auto & col = sample_block.getByPosition(primary_key_pos[i]);
        if (i)
            out << ',';
        out << col.name << ' ' << col.type->getName();
    }
    out << "\nvalue:";
    for (size_t i = 0; i < value_column_pos.size(); ++i)
    {
        const auto & col = sample_block.getByPosition(value_column_pos[i]);
        if (i)
            out << ',';
        out << col.name << ' ' << col.type->getName();
    }
    return out.str();
}

/// Companion file recording backupSchemaFingerprint(). Restore compares it against the target table's own
/// fingerprint and rejects a mismatch before replaying any bytes, so a same-ttl but structurally-different
/// target cannot silently decode the raw bytes into wrong data.
static constexpr std::string_view rocksdb_backup_schema_filename = "schema.txt";

/// The single-owner optimization (siblings reference the owner's data instead of dumping their own) is safe
/// only when the elected owner is the writable table: its live handle sees the freshest shared data, so its
/// one dump represents every sibling. An all-read_only group has no such common live view (each read_only
/// handle is an independent snapshot that may diverge), so those tables must not collapse onto one dump.
/// election_id encodes writability in its leading tag, so a writable owner's id starts with "1_rw_".
static bool isWritableElectionId(const String & election_id)
{
    return election_id.starts_with("1_rw_");
}

void StorageEmbeddedRocksDB::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /*partitions*/)
{
    /// Several tables (one writable plus any number of read_only) may share a single rocksdb_dir. Register
    /// this table so a writable owner backs up the shared RocksDB once and its read_only siblings only
    /// reference it. This avoids dumping the same directory multiple times from independent snapshots on
    /// different handles, whose contents could diverge if writes race between the dumps.
    auto coordination = backup_entries_collector.getBackupCoordination();
    coordination->addRocksDBTable(rocksdb_dir, backupElectionId(), data_path_in_backup);

    /// Runs after all tables have registered, so getRocksDBDataPath()/getRocksDBDataOwnerElectionId() see
    /// the whole group.
    auto post_collecting_task = [coordination, &backup_entries_collector, my_data_path_in_backup = data_path_in_backup, this]
    {
        /// Record this table's ttl and schema fingerprint so restore can reject an incompatible target: the
        /// value bytes are ttl-format-dependent, and the raw key/value bytes are only decodable by a table with
        /// the same physical-column layout. Both are per-table constants, so every table writes its own tiny
        /// ttl.txt / schema.txt even when it references a sibling's data.bin.
        backup_entries_collector.addBackupEntries(
            {{fs::path(my_data_path_in_backup) / rocksdb_backup_ttl_filename,
              std::make_shared<BackupEntryFromMemory>(toString(ttl))},
             {fs::path(my_data_path_in_backup) / rocksdb_backup_schema_filename,
              std::make_shared<BackupEntryFromMemory>(backupSchemaFingerprint())}});

        auto owner_election_id = coordination->getRocksDBDataOwnerElectionId(rocksdb_dir);
        auto owner_data_path = coordination->getRocksDBDataPath(rocksdb_dir);

        /// Reference the owner's data.bin instead of dumping again ONLY when the owner is the writable table
        /// (its live handle holds the freshest shared data). In an all-read_only group there is no common live
        /// view, so every table dumps its own snapshot rather than collapsing onto one that may not match.
        if (isWritableElectionId(owner_election_id) && owner_data_path != my_data_path_in_backup)
        {
            String source_path = fs::path(my_data_path_in_backup) / rocksdb_backup_data_filename;
            String target_path = fs::path(owner_data_path) / rocksdb_backup_data_filename;
            backup_entries_collector.addBackupEntries({{source_path, std::make_shared<BackupEntryReference>(std::move(target_path))}});
            return;
        }

        TemporaryDataOnDiskSettings tmp_data_settings;
        auto max_compress_block_size = backup_entries_collector.getContext()->getSettingsRef()[Setting::max_compress_block_size];
        tmp_data_settings.buffer_size = max_compress_block_size ? max_compress_block_size : DBMS_DEFAULT_BUFFER_SIZE;
        auto tmp_data = std::make_shared<TemporaryDataOnDiskScope>(backup_entries_collector.getContext()->getTempDataOnDisk(), tmp_data_settings);

        backup_entries_collector.addBackupEntries(
            std::make_shared<EmbeddedRocksDBBackup>(
                std::static_pointer_cast<const StorageEmbeddedRocksDB>(shared_from_this()), my_data_path_in_backup, std::move(tmp_data))
                ->getBackupEntries());
    };

    backup_entries_collector.addPostTask(post_collecting_task);
}

/// backupData() always writes data.bin (holding zero records for an empty table). Peeking at the
/// first record tells whether the backup actually carries any rows.
static bool backupHasRows(const BackupPtr & backup, const String & data_file)
{
    CompressedReadBufferFromFile compressed_in{backup->readFile(data_file)};
    return !compressed_in.eof();
}

void StorageEmbeddedRocksDB::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /*partitions*/)
{
    auto backup = restorer.getBackup();

    /// backupData() always writes data.bin, even for an empty table, so a data restore reaching here with
    /// no data.bin cannot be an "empty table" case. It means one of: (a) the backup was made with
    /// structure_only = true (metadata only, no data) and should be restored the same way, with
    /// SETTINGS structure_only = true; (b) the backup predates EmbeddedRocksDB backing up its data
    /// (see https://github.com/ClickHouse/ClickHouse/issues/109213); or (c) the backup is corrupted.
    /// Restoring it as data would silently recreate an empty table, so fail closed instead.
    String data_file = fs::path(data_path_in_backup) / rocksdb_backup_data_filename;
    if (!backup->fileExists(data_file))
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "Backup of table {} has no RocksDB data file {}. If this backup was created with structure_only = true, "
            "restore it with SETTINGS structure_only = true. Otherwise it predates EmbeddedRocksDB backing up its data "
            "(see https://github.com/ClickHouse/ClickHouse/issues/109213) or is corrupted; restoring its data would "
            "silently produce an empty table",
            getStorageID().getNameForLogs(), data_file);

    /// The backed-up value bytes are ttl-format-dependent: a ttl > 0 source is a DBWithTTL whose values
    /// carry a trailing 4-byte creation timestamp (backed up verbatim) while a ttl = 0 source has none, and
    /// two ttl > 0 tables interpret those timestamps against their own ttl window. Restoring across a ttl
    /// mismatch would replay incompatible bytes or silently shift every row's expiration. Enforce it here
    /// because the RESTORE ... AS <writable_table> / allow_different_table_def workaround for read_only
    /// tables skips RestorerFromBackup's create-query compatibility check, so nothing else catches it.
    String ttl_file = fs::path(data_path_in_backup) / rocksdb_backup_ttl_filename;
    if (backup->fileExists(ttl_file))
    {
        String backup_ttl_str;
        readStringUntilEOF(backup_ttl_str, *backup->readFile(ttl_file));
        Int32 backup_ttl = parse<Int32>(backup_ttl_str);
        if (backup_ttl != ttl)
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot restore EmbeddedRocksDB table {}: backup was taken from a table with ttl = {} but the "
                "target table has ttl = {}. The stored value bytes are ttl-format-dependent, so restore requires "
                "a matching ttl. Create the target table with ttl = {} and restore again",
                getStorageID().getNameForLogs(), backup_ttl, ttl, backup_ttl);
    }

    /// The raw key/value bytes are only decodable by a table with the same physical-column layout (key = PK
    /// columns in PK order, value = the remaining physical columns in physical order). A same-ttl target that
    /// differs in column types or ordering would silently decode the bytes into wrong data. Reject a mismatch
    /// before replaying anything; this is what guards the RESTORE ... AS <writable_table> /
    /// allow_different_table_def path, which skips RestorerFromBackup's create-query compatibility check.
    String schema_file = fs::path(data_path_in_backup) / rocksdb_backup_schema_filename;
    if (backup->fileExists(schema_file))
    {
        String backup_schema;
        readStringUntilEOF(backup_schema, *backup->readFile(schema_file));
        String target_schema = backupSchemaFingerprint();
        if (backup_schema != target_schema)
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot restore EmbeddedRocksDB table {}: the backup's column layout does not match the target "
                "table. The stored bytes are decoded with the target table's schema, so restore requires an "
                "identical physical-column layout and primary key. Backup layout [{}] but target layout [{}]. "
                "Create the target table with a matching schema and restore again",
                getStorageID().getNameForLogs(), backup_schema, target_schema);
    }

    /// Several tables (one writable plus any number of read_only) may share a single rocksdb_dir. When a
    /// writable table shares the directory it is the single owner that replays the shared RocksDB (a read_only
    /// handle rejects Write()), so a {rw, ro} pair restores through the writable table and the read_only
    /// sibling contributes no data restore. An all-read_only group has no writable owner and no common live
    /// view, so each read_only table restores its own backup independently.
    auto restore_coordination = restorer.getRestoreCoordination();
    restore_coordination->addRocksDBTable(rocksdb_dir, backupElectionId());

    /// The ownership decision and all writes run inside the data restore task, which executes after every
    /// table has registered (insertDataToTables() waits for all restoreDataFromBackup() calls before the
    /// data restore tasks run), so getRocksDBDataOwnerElectionId() sees the full set of siblings.
    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageEmbeddedRocksDB>(shared_from_this()),
         backup,
         data_path_in_backup,
         restore_coordination,
         my_election_id = backupElectionId(),
         allow_non_empty_tables = restorer.isNonEmptyTableAllowed()]
        {
            auto owner_election_id = restore_coordination->getRocksDBDataOwnerElectionId(storage->rocksdb_dir);
            /// Skip only when a writable owner (which will replay for the whole group) is some other table.
            /// If the owner is not writable (all-read_only group) every table replays its own backup, so a
            /// non-empty read_only backup still hits the read_only rejection in restoreDataOwner().
            if (isWritableElectionId(owner_election_id) && owner_election_id != my_election_id)
                return;
            storage->restoreDataOwner(backup, data_path_in_backup, allow_non_empty_tables);
        });
}

void StorageEmbeddedRocksDB::finalizeRestoreFromBackup()
{
    /// A read_only handle snapshots the RocksDB directory at open time. When tables share one rocksdb_dir,
    /// the read_only sibling's handle was opened during createAndCheckTables(), before the writable owner
    /// replayed the rows in a data restore task, so it still serves the pre-restore snapshot. finalizeTables()
    /// runs after every data restore task has completed, so reopen the read_only handle here to observe the
    /// restored data (the writable owner needs no reopen: it wrote through its own live handle).
    if (!read_only)
        return;

    std::lock_guard lock(rocksdb_ptr_mx);
    if (rocksdb_ptr)
    {
        rocksdb_ptr->Close();
        rocksdb_ptr = nullptr;
    }
    initDB();
}

void StorageEmbeddedRocksDB::restoreDataOwner(const BackupPtr & backup, const String & data_path_in_backup, bool allow_non_empty_tables)
{
    String data_file = fs::path(data_path_in_backup) / rocksdb_backup_data_filename;

    /// A read_only table opens its handle with OpenForReadOnly()/DBWithTTL::Open(..., read_only) over an
    /// externally-managed directory and rejects the Write() a data restore issues. If this read_only table
    /// is the elected owner (no writable sibling exists) and the backup carries rows, reject up front with a
    /// clear error instead of failing later with an opaque RocksDB write error. An empty backup writes
    /// nothing, so it is still allowed (subject to the non-empty-table guard below).
    bool backup_has_rows = backupHasRows(backup, data_file);
    if (read_only && backup_has_rows)
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "Cannot restore data into read_only EmbeddedRocksDB table {}. To restore the data, create a writable "
            "EmbeddedRocksDB table with the same schema and ttl, then run RESTORE ... AS <writable_table> "
            "SETTINGS allow_different_table_def = 1",
            getStorageID().getNameForLogs());

    /// Unless allow_non_empty_tables is set, restoring into a table that already holds rows is rejected.
    /// This guard applies to read_only tables too: an empty backup must not silently "succeed" and leave
    /// the stale rows of a pre-populated external directory in place.
    if (!allow_non_empty_tables)
    {
        bool empty = false;
        {
            SharedLockGuard lock(rocksdb_ptr_mx);
            if (!rocksdb_ptr)
                throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table is dropped");
            std::unique_ptr<rocksdb::Iterator> iterator(rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
            iterator->SeekToFirst();
            empty = !iterator->Valid();
            if (!iterator->status().ok())
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB iterator error: {}", iterator->status().ToString());
        }
        if (!empty)
            RestorerFromBackup::throwTableIsNotEmpty(getStorageID());
    }

    /// An empty backup has no rows to write; the metadata restore that recreated the table is all that
    /// is needed (this is also what makes an empty read_only backup restorable).
    if (!backup_has_rows)
        return;

    restoreDataImpl(backup, data_path_in_backup);
}

void StorageEmbeddedRocksDB::restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup)
{
    String data_file = fs::path(data_path_in_backup) / rocksdb_backup_data_filename;
    if (!backup->fileExists(data_file))
        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", data_file);

    CompressedReadBufferFromFile compressed_in{backup->readFile(data_file)};

    rocksdb::WriteBatch batch;

    /// GetRootDB() bypasses the DBWithTTL wrapper so the raw TTL timestamp suffix is written verbatim.
    const auto flush_batch = [&]
    {
        SharedLockGuard lock(rocksdb_ptr_mx);
        if (!rocksdb_ptr)
            throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Table is dropped");
        auto status = rocksdb_ptr->GetRootDB()->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
        batch.Clear();
    };

    String key;
    String value;
    while (!compressed_in.eof())
    {
        readStringBinary(key, compressed_in);
        readStringBinary(value, compressed_in);

        auto status = batch.Put(key, value);
        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());

        /// Flush periodically so restoring a large table does not buffer the whole batch in RAM.
        if (batch.GetDataSize() >= rocksdb_restore_batch_flush_bytes)
            flush_batch();
    }

    if (batch.Count() > 0)
        flush_batch();
}

static_assert(rocksdb::DEBUG_LEVEL == 0);
static_assert(rocksdb::HEADER_LEVEL == 5);
static constexpr std::array<std::pair<DB::LogsLevel, Poco::Message::Priority>, 6> rocksdb_logger_map = {
    std::make_pair(DB::LogsLevel::debug, Poco::Message::Priority::PRIO_DEBUG),
    std::make_pair(DB::LogsLevel::information, Poco::Message::Priority::PRIO_INFORMATION),
    std::make_pair(DB::LogsLevel::warning, Poco::Message::Priority::PRIO_WARNING),
    std::make_pair(DB::LogsLevel::error, Poco::Message::Priority::PRIO_ERROR),
    std::make_pair(DB::LogsLevel::fatal, Poco::Message::Priority::PRIO_FATAL),
    /// Same as default logger does for HEADER_LEVEL
    std::make_pair(DB::LogsLevel::information, Poco::Message::Priority::PRIO_INFORMATION),
};
class StorageEmbeddedRocksDBLogger : public rocksdb::Logger
{
public:
    explicit StorageEmbeddedRocksDBLogger(const rocksdb::InfoLogLevel log_level, LoggerRawPtr log_)
        : rocksdb::Logger(log_level)
        , log(log_)
    {
    }

    void Logv(const char * format, va_list ap) override __attribute__((format(printf, 2, 0)))
    {
        Logv(rocksdb::InfoLogLevel::DEBUG_LEVEL, format, ap);
    }

    void Logv(const rocksdb::InfoLogLevel log_level, const char * format, va_list ap) override __attribute__((format(printf, 3, 0)))
    {
        if (log_level < GetInfoLogLevel())
            return;

        auto level = rocksdb_logger_map[log_level];

        /// stack buffer was enough
        {
            va_list backup_ap;
            va_copy(backup_ap, ap);
            std::array<char, 1024> stack; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - written by `vsnprintf` before read
            if (vsnprintf(stack.data(), stack.size(), format, backup_ap) < static_cast<int>(stack.size()))
            {
                va_end(backup_ap);
                LOG_IMPL(log, level.first, level.second, "{}", stack.data());
                return;
            }
            va_end(backup_ap);
        }

        /// let's try with a bigger dynamic buffer (but not too huge, since
        /// some of rocksdb internal code has also such a limitation, i..e
        /// HdfsLogger)
        {
            va_list backup_ap;
            va_copy(backup_ap, ap);
            static constexpr int buffer_size = 30000;
            std::unique_ptr<char[]> buffer(new char[buffer_size]);
            if (vsnprintf(buffer.get(), buffer_size, format, backup_ap) >= buffer_size)
                buffer[buffer_size - 1] = 0;
            va_end(backup_ap);
            LOG_IMPL(log, level.first, level.second, "{}", buffer.get());
        }
    }

private:
    LoggerRawPtr log;
};

void StorageEmbeddedRocksDB::initDB()
{
    rocksdb::Status status;
    rocksdb::Options base;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;
    rocksdb::BlockBasedTableOptions table_options;

    const auto & config = getContext()->getConfigRef();
    if (config.has("rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap({}, merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir,
                status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap({}, merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Fail to merge rocksdb options from 'rocksdb.column_family_options' at: {}: {}",
                rocksdb_dir,
                status.ToString());
        }
    }
    if (config.has("rocksdb.block_based_table_options"))
    {
        auto block_based_table_options = getOptionsFromConfig(config, "rocksdb.block_based_table_options");
        status = rocksdb::GetBlockBasedTableOptionsFromMap({}, table_options, block_based_table_options, &table_options);
        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Fail to merge rocksdb options from 'rocksdb.block_based_table_options' at: {}: {}",
                rocksdb_dir,
                status.ToString());
        }
    }

    if (config.has("rocksdb.tables"))
    {
        auto table_name = getStorageID().getTableName();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("rocksdb.tables", keys);

        for (const auto & key : keys)
        {
            const String key_prefix = "rocksdb.tables." + key;
            if (config.getString(key_prefix + ".name") != table_name)
                continue;

            String config_key = key_prefix + ".options";
            if (config.has(config_key))
            {
                auto table_config_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetDBOptionsFromMap({}, merged, table_config_options, &merged);
                if (!status.ok())
                {
                    throw Exception(
                        ErrorCodes::ROCKSDB_ERROR,
                        "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key,
                        rocksdb_dir,
                        status.ToString());
                }
            }

            config_key = key_prefix + ".column_family_options";
            if (config.has(config_key))
            {
                auto table_column_family_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetColumnFamilyOptionsFromMap({}, merged, table_column_family_options, &merged);
                if (!status.ok())
                {
                    throw Exception(
                        ErrorCodes::ROCKSDB_ERROR,
                        "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key,
                        rocksdb_dir,
                        status.ToString());
                }
            }

            config_key = key_prefix + ".block_based_table_options";
            if (config.has(config_key))
            {
                auto block_based_table_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetBlockBasedTableOptionsFromMap({}, table_options, block_based_table_options, &table_options);
                if (!status.ok())
                {
                    throw Exception(
                        ErrorCodes::ROCKSDB_ERROR,
                        "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key,
                        rocksdb_dir,
                        status.ToString());
                }
            }
        }
    }

    merged.info_log = std::make_shared<StorageEmbeddedRocksDBLogger>(merged.info_log_level, log.get());
    merged.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    if (ttl > 0)
    {
        rocksdb::DBWithTTL * db = nullptr;
        status = rocksdb::DBWithTTL::Open(merged, rocksdb_dir, &db, ttl, read_only);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DBWithTTL>(db);
    }
    else
    {
        rocksdb::DB * db = nullptr;
        if (read_only)
            status = rocksdb::DB::OpenForReadOnly(merged, rocksdb_dir, &db);
        else
            status = rocksdb::DB::Open(merged, rocksdb_dir, &db);

        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());

        rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
    }
}

class ReadFromEmbeddedRocksDB : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromEmbeddedRocksDB"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;
    void describeActions(FormatSettings & format_settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

    ReadFromEmbeddedRocksDB(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        const StorageEmbeddedRocksDB & storage_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(std::move(sample_block), column_names_, query_info_, storage_snapshot_, context_)
        , storage(storage_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

private:
    const StorageEmbeddedRocksDB & storage;

    size_t max_block_size;
    size_t num_streams;

    FieldVectorPtr keys;
    bool all_scan = true;
};

void StorageEmbeddedRocksDB::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);

    auto reading = std::make_unique<ReadFromEmbeddedRocksDB>(
        column_names,
        query_info,
        storage_snapshot,
        context_,
        std::make_shared<const Block>(std::move(sample_block)),
        *this,
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromEmbeddedRocksDB::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & sample_block = getOutputHeader();
    if (all_scan)
    {
        std::unique_ptr<rocksdb::Iterator> iterator;
        {
            SharedLockGuard lock(storage.rocksdb_ptr_mx);
            if (!storage.rocksdb_ptr)
            {
                pipeline.init(Pipe(std::make_shared<NullSource>(sample_block)));
                return;
            }
            iterator.reset(storage.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        }
        iterator->SeekToFirst();
        auto source = std::make_shared<EmbeddedRocksDBSource>(storage, storage_snapshot, sample_block, std::move(iterator), max_block_size);
        source->setStorageLimits(query_info.storage_limits);
        pipeline.init(Pipe(std::move(source)));
        return;
    }

    if (keys->empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(sample_block)));
        return;
    }

    ::sort(keys->begin(), keys->end());
    keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

    Pipes pipes;

    size_t num_keys = keys->size();
    size_t num_threads = std::min<size_t>(num_streams, keys->size());

    chassert(num_keys <= std::numeric_limits<uint32_t>::max());
    chassert(num_threads <= std::numeric_limits<uint32_t>::max());

    for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
    {
        size_t begin = num_keys * thread_idx / num_threads;
        size_t end = num_keys * (thread_idx + 1) / num_threads;

        auto source = std::make_shared<EmbeddedRocksDBSource>(
            storage, storage_snapshot, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size);
        source->setStorageLimits(query_info.storage_limits);
        pipes.emplace_back(std::move(source));
    }
    pipeline.init(Pipe::unitePipes(std::move(pipes)));
}

void ReadFromEmbeddedRocksDB::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));
    std::tie(keys, all_scan) = getFilterKeys(storage.getPrimaryKey(), storage.getPrimaryKeyTypes(), filter_actions_dag.get(), context);
}

void ReadFromEmbeddedRocksDB::describeActions(FormatSettings & format_settings) const
{
    const std::string & prefix = format_settings.detail_prefix;
    if (!all_scan)
    {
        format_settings.out << prefix << "ReadType: GetKeys\n";
        format_settings.out << prefix << "Keys: " << keys->size() << '\n';
    }
    else
        format_settings.out << prefix << "ReadType: FullScan\n";
}

void ReadFromEmbeddedRocksDB::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!all_scan)
    {
        map.add("Read Type", "GetKeys");
        map.add("Keys", keys->size());
    }
    else
        map.add("Read Type", "FullScan");
}

SinkToStoragePtr StorageEmbeddedRocksDB::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context, bool /*async_insert*/)
{
    if (getSettings()[RocksDBSetting::optimize_for_bulk_insert])
    {
        LOG_DEBUG(log, "Using bulk insert");
        return std::make_shared<EmbeddedRocksDBBulkSink>(query_context, *this, metadata_snapshot);
    }

    LOG_DEBUG(log, "Using regular insert");
    return std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    auto engine_args = args.engine_args;
    if (engine_args.size() > 3)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Engine {} requires at most 3 parameters. "
            "({} given). Correct usage: EmbeddedRocksDB([ttl, rocksdb_dir, read_only])",
            args.engine_name,
            engine_args.size());
    }

    Int32 ttl{0};
    String rocksdb_dir;
    bool read_only{false};
    if (!engine_args.empty())
        ttl = static_cast<Int32>(checkAndGetLiteralArgument<UInt64>(engine_args[0], "ttl"));
    if (engine_args.size() > 1)
        rocksdb_dir = checkAndGetLiteralArgument<String>(engine_args[1], "rocksdb_dir");
    if (engine_args.size() > 2)
        read_only = checkAndGetLiteralArgument<bool>(engine_args[2], "read_only");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);
    metadata.setComment(args.comment);

    if (!args.storage_def->primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageEmbeddedRocksDB requires at least one column in primary key");

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, {}, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    for (const auto & primary_key_name : primary_key_names)
    {
        if (metadata.getColumns().hasSubcolumn(GetColumnsOptions::All, primary_key_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageEmbeddedRocksDB doesn't support subcolumns in primary key");
    }

    auto settings = std::make_unique<RocksDBSettings>();
    settings->loadFromQuery(*args.storage_def);
    if (args.storage_def->settings)
        metadata.settings_changes = args.storage_def->settings->ptr();
    else
    {
        /// A workaround because embedded rocksdb doesn't have default immutable settings
        /// But InterpreterAlterQuery requires settings_changes to be set to run ALTER MODIFY
        /// SETTING queries. So we just add a setting with its default value.
        auto settings_changes = make_intrusive<ASTSetQuery>();
        settings_changes->is_standalone = false;
        settings_changes->changes.insertSetting("optimize_for_bulk_insert", (*settings)[RocksDBSetting::optimize_for_bulk_insert].value);
        metadata.settings_changes = settings_changes;
    }
    return std::make_shared<StorageEmbeddedRocksDB>(
        args.table_id,
        args.relative_data_path,
        metadata,
        args.mode,
        args.getContext(),
        std::move(settings),
        primary_key_names,
        ttl,
        std::move(rocksdb_dir),
        read_only);
}

std::shared_ptr<rocksdb::Statistics> StorageEmbeddedRocksDB::getRocksDBStatistics() const
{
    SharedLockGuard lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return nullptr;
    return rocksdb_ptr->GetOptions().statistics;
}

std::vector<rocksdb::Status>
StorageEmbeddedRocksDB::multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const
{
    SharedLockGuard lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    return rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
}

Chunk StorageEmbeddedRocksDB::getByKeys(
    const ColumnsWithTypeAndName & keys,
    const Names &,
    PaddedPODArray<UInt8> & null_map,
    IColumn::Offsets & /* out_offsets */) const
{
    if (keys.size() != primary_keys.size())
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR, "Key column number mismatch, expected {}, got {}.", primary_keys.size(), keys.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        // Remove Nullable and LowCardinality wrappers for comparison
        DataTypePtr key_type = removeNullable(recursiveRemoveLowCardinality(keys[i].type));
        DataTypePtr primary_key_type = removeNullable(recursiveRemoveLowCardinality(primary_key_types[i]));

        if (!key_type->equals(*primary_key_type))
            throw DB::Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Primary key type mismatch, expected {}, got {}.",
                primary_key_types[i]->getName(),
                keys[i].type->getName());
    }

    const size_t num_rows{keys[0].column->size()};
    null_map.clear();
    null_map.resize_fill(num_rows, 1);

    std::vector<std::string> raw_keys;
    raw_keys.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        std::string & serialized_key = raw_keys.emplace_back();
        WriteBufferFromString wb(serialized_key);
        for (const auto & key : keys)
        {
            Field field;
            key.column->get(i, field);
            if (field.isNull())
            {
                null_map[i] = 0;
                break;
            }
            key.type->getDefaultSerialization()->serializeBinary(field, wb, {});
        }
        wb.finalize();
    }

    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    auto block = getBySerializedKeys(raw_keys, &null_map, metadata_snapshot->getSampleBlock());
    return Chunk(block.getColumns(), block.rows());
}

Block StorageEmbeddedRocksDB::getSampleBlock(const Names &) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    return metadata_snapshot->getSampleBlock();
}

Block StorageEmbeddedRocksDB::getBySerializedKeys(const std::vector<std::string> & keys, PaddedPODArray<UInt8> * in_out_null_map, const Block & sample_block) const
{
    std::vector<String> values;

    MutableColumns columns = sample_block.cloneEmptyColumns();

    /// Convert from vector of string to vector of string refs (rocksdb::Slice), because multiGet api expects them.
    std::vector<rocksdb::Slice> slices_keys;
    slices_keys.reserve(keys.size());
    for (const auto & key : keys)
        slices_keys.emplace_back(key);

    auto statuses = multiGet(slices_keys, values);
    for (size_t i = 0; i < statuses.size(); ++i)
    {
        if (in_out_null_map && !(*in_out_null_map)[i])
        {
            for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
            {
                columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());
            }
            continue;
        }

        if (statuses[i].ok())
        {
            fillColumns(slices_keys[i], getPrimaryKeyPos(), sample_block, columns);
            fillColumns(values[i], getValueColumnPos(), sample_block, columns);
            continue;
        }

        if (statuses[i].IsNotFound())
        {
            if (in_out_null_map)
            {
                (*in_out_null_map)[i] = 0;
                for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
                {
                    columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());
                }
            }
        }
        else
        {
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "rocksdb error {}", statuses[i].ToString());
        }
    }

    return sample_block.cloneWithColumns(std::move(columns));
}

std::optional<UInt64> StorageEmbeddedRocksDB::totalRows(ContextPtr query_context) const
{
    if (!query_context->getSettingsRef()[Setting::optimize_trivial_approximate_count_query])
        return {};
    SharedLockGuard lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    UInt64 estimated_rows = 0;
    if (!rocksdb_ptr->GetIntProperty("rocksdb.estimate-num-keys", &estimated_rows))
        return {};
    return estimated_rows;
}

std::optional<UInt64> StorageEmbeddedRocksDB::totalBytes(ContextPtr) const
{
    SharedLockGuard lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    UInt64 estimated_bytes = 0;
    if (!rocksdb_ptr->GetAggregatedIntProperty("rocksdb.estimate-live-data-size", &estimated_bytes))
        return {};
    return estimated_bytes;
}

void StorageEmbeddedRocksDB::alter(const AlterCommands & params, ContextPtr query_context, AlterLockHolder & holder)
{
    IStorage::alter(params, query_context, holder);
    auto new_metadata = getInMemoryMetadataPtr(query_context, false);
    if (new_metadata->settings_changes)
    {
        const auto & settings_changes = new_metadata->settings_changes->as<const ASTSetQuery &>();
        auto new_settings = std::make_unique<RocksDBSettings>();
        new_settings->applyChanges(settings_changes.changes);
        setSettings(std::move(new_settings));
    }
}

void registerStorageEmbeddedRocksDB(StorageFactory & factory);
void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
        .has_builtin_setting_fn = RocksDBSettings::hasBuiltin,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features, Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# EmbeddedRocksDB table engine

<CloudNotSupportedBadge />

This engine allows integrating ClickHouse with [RocksDB](http://rocksdb.org/).

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

Engine parameters:

- `ttl` - time to live for values. TTL is accepted in seconds. If TTL is 0, regular RocksDB instance is used (without TTL).
- `rocksdb_dir` - path to the directory of an existed RocksDB or the destination path of the created RocksDB. Open the table with the specified `rocksdb_dir`.
- `read_only` - when `read_only` is set to true, read-only mode is used. For storage with TTL, compaction will not be triggered (neither manual nor automatic), so no expired entries are removed.
- `primary_key_name` – any column name in the column list.
- `primary key` must be specified, it supports only one column in the primary key. The primary key will be serialized in binary as a `rocksdb key`.
- columns other than the primary key will be serialized in binary as `rocksdb` value in corresponding order.
- queries with key `equals` or `in` filtering will be optimized to multi keys lookup from `rocksdb`.

Engine settings:

- `optimize_for_bulk_insert` – Table is optimized for bulk insertions (insert pipeline will create SST files and import to rocksdb database instead of writing to memtables); default value: `1`.
- `bulk_insert_block_size` - Minimum size of SST files (in term of rows) created by bulk insertion; default value: `1048449`.

Example:

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

## Metrics {#metrics}

There is also `system.rocksdb` table, that expose rocksdb statistics:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

## Configuration {#configuration}

You can also change any [rocksdb options](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) using config:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

By default trivial approximate count optimization is turned off, which might affect the performance `count()` queries. To enable this
optimization set up `optimize_trivial_approximate_count_query = 1`. Also, this setting affects `system.tables` for EmbeddedRocksDB engine,
turn on the settings to see approximate values for `total_rows` and `total_bytes`.

## Supported operations {#supported-operations}

### Inserts {#inserts}

When new rows are inserted into `EmbeddedRocksDB`, if the key already exists, the value will be updated, otherwise a new key is created.

Example:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

### Deletes {#deletes}

Rows can be deleted using `DELETE` query or `TRUNCATE`.

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

### Updates {#updates}

Values can be updated using the `ALTER TABLE` query. The primary key cannot be updated.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

### Joins {#joins}

A special `direct` join with EmbeddedRocksDB tables is supported.
This direct join avoids forming a hash table in memory and accesses
the data directly from the EmbeddedRocksDB.

With large joins you may see much lower memory usage with direct joins
because the hash table is not created.

To enable direct joins:
```sql
SET join_algorithm = 'direct, hash'
```

:::tip
When the `join_algorithm` is set to `direct, hash`, direct joins will be used
when possible, and hash otherwise.
:::

#### Example {#example}

##### Create and populate an EmbeddedRocksDB table {#create-and-populate-an-embeddedrocksdb-table}
```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

##### Create and populate a table to join with table `rdb` {#create-and-populate-a-table-to-join-with-table-rdb}

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

##### Set the join algorithm to `direct`{#set-the-join-algorithm-to-direct}

```sql
SET join_algorithm = 'direct'
```

##### An INNER JOIN {#an-inner-join}
```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```
```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

### More information on Joins {#more-information-on-joins}
- [`join_algorithm` setting](/operations/settings/settings.md#join_algorithm)
- [JOIN clause](/sql-reference/statements/select/join.md)
)DOCS_MD",
        .syntax = "ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(key)",
        .related = {"Redis"}});
}

void StorageEmbeddedRocksDB::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && !command.isSettingsAlter())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());

        /// Validate setting values before `IStorage::alter` persists the metadata file,
        /// otherwise an invalid value blocks attach on the next restart. See issue #88443.
        if (command.type == AlterCommand::MODIFY_SETTING)
        {
            for (const auto & change : command.settings_changes)
                RocksDBSettings::checkCanSet(change.name, change.value);
        }
    }
}

}
