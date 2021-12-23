#include <Storages/StorageJoin.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSet.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <Poco/String.h> /// toLower


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageJoin::StorageJoin(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const Names & key_names_,
    bool use_nulls_,
    SizeLimits limits_,
    ASTTableJoin::Kind kind_,
    ASTTableJoin::Strictness strictness_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool overwrite_,
    bool persistent_)
    : StorageSetOrJoinBase{disk_, relative_path_, table_id_, columns_, constraints_, comment, persistent_}
    , key_names(key_names_)
    , use_nulls(use_nulls_)
    , limits(limits_)
    , kind(kind_)
    , strictness(strictness_)
    , overwrite(overwrite_)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    for (const auto & key : key_names)
        if (!metadata_snapshot->getColumns().hasPhysical(key))
            throw Exception{"Key column (" + key + ") does not exist in table declaration.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE};

    table_join = std::make_shared<TableJoin>(limits, use_nulls, kind, strictness, key_names);
    join = std::make_shared<HashJoin>(table_join, metadata_snapshot->getSampleBlock().sortColumns(), overwrite);
    restore();
}

RWLockImpl::LockHolder StorageJoin::tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef().lock_acquire_timeout : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return tryLockTimed(lock, type, query_id, acquire_timeout);
}

BlockOutputStreamPtr StorageJoin::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    std::lock_guard mutate_lock(mutate_mutex);
    return StorageSetOrJoinBase::write(query, metadata_snapshot, context);
}

void StorageJoin::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, TableExclusiveLockHolder &)
{
    std::lock_guard mutate_lock(mutate_mutex);
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    disk->removeRecursive(path);
    disk->createDirectories(path);
    disk->createDirectories(path + "tmp/");

    increment = 0;
    join = std::make_shared<HashJoin>(table_join, metadata_snapshot->getSampleBlock().sortColumns(), overwrite);
}

void StorageJoin::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    for (const auto & command : commands)
        if (command.type != MutationCommand::DELETE)
            throw Exception("Table engine Join supports only DELETE mutations", ErrorCodes::NOT_IMPLEMENTED);
}

void StorageJoin::mutate(const MutationCommands & commands, ContextPtr context)
{
    /// Firstly acquire lock for mutation, that locks changes of data.
    /// We cannot acquire rwlock here, because read lock is needed
    /// for execution of mutation interpreter.
    std::lock_guard mutate_lock(mutate_mutex);

    constexpr auto tmp_backup_file_name = "tmp/mut.bin";
    auto metadata_snapshot = getInMemoryMetadataPtr();

    auto backup_buf = disk->writeFile(path + tmp_backup_file_name);
    auto compressed_backup_buf = CompressedWriteBuffer(*backup_buf);
    auto backup_stream = NativeBlockOutputStream(compressed_backup_buf, 0, metadata_snapshot->getSampleBlock());

    auto new_data = std::make_shared<HashJoin>(table_join, metadata_snapshot->getSampleBlock().sortColumns(), overwrite);

    // New scope controls lifetime of InputStream.
    {
        auto storage_ptr = DatabaseCatalog::instance().getTable(getStorageID(), context);
        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context, true);
        auto in = interpreter->execute();
        in->readPrefix();

        while (const Block & block = in->read())
        {
            new_data->addJoinedBlock(block, true);
            if (persistent)
                backup_stream.write(block);
        }

        in->readSuffix();
    }

    /// Now acquire exclusive lock and modify storage.
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    join = std::move(new_data);
    increment = 1;

    if (persistent)
    {
        backup_stream.flush();
        compressed_backup_buf.next();
        backup_buf->next();
        backup_buf->finalize();

        std::vector<std::string> files;
        disk->listFiles(path, files);
        for (const auto & file_name: files)
        {
            if (file_name.ends_with(".bin"))
                disk->removeFileIfExists(path + file_name);
        }

        disk->replaceFile(path + tmp_backup_file_name, path + std::to_string(increment) + ".bin");
    }
}

HashJoinPtr StorageJoin::getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, ContextPtr context) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (!analyzed_join->sameStrictnessAndKind(strictness, kind))
        throw Exception("Table " + getStorageID().getNameForLogs() + " has incompatible type of JOIN.", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);

    if ((analyzed_join->forceNullableRight() && !use_nulls) ||
        (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls))
        throw Exception("Table " + getStorageID().getNameForLogs() + " needs the same join_use_nulls setting as present in LEFT or FULL JOIN.",
                        ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);

    /// TODO: check key columns

    /// Set names qualifiers: table.column -> column
    /// It's required because storage join stores non-qualified names
    /// Qualifies will be added by join implementation (HashJoin)
    analyzed_join->setRightKeys(key_names);

    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, metadata_snapshot->getSampleBlock().sortColumns());

    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    join_clone->setLock(holder);
    join_clone->reuseJoinedData(*join);

    return join_clone;
}


void StorageJoin::insertBlock(const Block & block, ContextPtr context)
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);
    join->addJoinedBlock(block, true);
}

size_t StorageJoin::getSize(ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return join->getTotalRowCount();
}

std::optional<UInt64> StorageJoin::totalRows(const Settings &settings) const
{
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings.lock_acquire_timeout);
    return join->getTotalRowCount();
}

std::optional<UInt64> StorageJoin::totalBytes(const Settings &settings) const
{
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings.lock_acquire_timeout);
    return join->getTotalByteCount();
}

DataTypePtr StorageJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    return join->joinGetCheckAndGetReturnType(data_types, column_name, or_null);
}

ColumnWithTypeAndName StorageJoin::joinGet(const Block & block, const Block & block_with_columns_to_add, ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return join->joinGet(block, block_with_columns_to_add);
}

void registerStorageJoin(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        /// Join(ANY, LEFT, k1, k2, ...)

        ASTs & engine_args = args.engine_args;

        const auto & settings = args.getContext()->getSettingsRef();

        auto join_use_nulls = settings.join_use_nulls;
        auto max_rows_in_join = settings.max_rows_in_join;
        auto max_bytes_in_join = settings.max_bytes_in_join;
        auto join_overflow_mode = settings.join_overflow_mode;
        auto join_any_take_last_row = settings.join_any_take_last_row;
        auto old_any_join = settings.any_join_distinct_right_table_keys;
        bool persistent = true;
        String disk_name = "default";

        if (args.storage_def && args.storage_def->settings)
        {
            for (const auto & setting : args.storage_def->settings->changes)
            {
                if (setting.name == "join_use_nulls")
                    join_use_nulls = setting.value;
                else if (setting.name == "max_rows_in_join")
                    max_rows_in_join = setting.value;
                else if (setting.name == "max_bytes_in_join")
                    max_bytes_in_join = setting.value;
                else if (setting.name == "join_overflow_mode")
                    join_overflow_mode = setting.value;
                else if (setting.name == "join_any_take_last_row")
                    join_any_take_last_row = setting.value;
                else if (setting.name == "any_join_distinct_right_table_keys")
                    old_any_join = setting.value;
                else if (setting.name == "disk")
                    disk_name = setting.value.get<String>();
                else if (setting.name == "persistent")
                {
                    auto join_settings = std::make_unique<JoinSettings>();
                    join_settings->loadFromQuery(*args.storage_def);
                    persistent = join_settings->persistent;
                }
                else
                    throw Exception("Unknown setting " + setting.name + " for storage " + args.engine_name, ErrorCodes::BAD_ARGUMENTS);
            }
        }

        DiskPtr disk = args.getContext()->getDisk(disk_name);

        if (engine_args.size() < 3)
            throw Exception(
                "Storage Join requires at least 3 parameters: Join(ANY|ALL|SEMI|ANTI, LEFT|INNER|RIGHT, keys...).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTTableJoin::Strictness strictness = ASTTableJoin::Strictness::Unspecified;
        ASTTableJoin::Kind kind = ASTTableJoin::Kind::Comma;

        if (auto opt_strictness_id = tryGetIdentifierName(engine_args[0]))
        {
            const String strictness_str = Poco::toLower(*opt_strictness_id);

            if (strictness_str == "any")
            {
                if (old_any_join)
                    strictness = ASTTableJoin::Strictness::RightAny;
                else
                    strictness = ASTTableJoin::Strictness::Any;
            }
            else if (strictness_str == "all")
                strictness = ASTTableJoin::Strictness::All;
            else if (strictness_str == "semi")
                strictness = ASTTableJoin::Strictness::Semi;
            else if (strictness_str == "anti")
                strictness = ASTTableJoin::Strictness::Anti;
        }

        if (strictness == ASTTableJoin::Strictness::Unspecified)
            throw Exception("First parameter of storage Join must be ANY or ALL or SEMI or ANTI (without quotes).",
                            ErrorCodes::BAD_ARGUMENTS);

        if (auto opt_kind_id = tryGetIdentifierName(engine_args[1]))
        {
            const String kind_str = Poco::toLower(*opt_kind_id);

            if (kind_str == "left")
                kind = ASTTableJoin::Kind::Left;
            else if (kind_str == "inner")
                kind = ASTTableJoin::Kind::Inner;
            else if (kind_str == "right")
                kind = ASTTableJoin::Kind::Right;
            else if (kind_str == "full")
            {
                if (strictness == ASTTableJoin::Strictness::Any)
                    strictness = ASTTableJoin::Strictness::RightAny;
                kind = ASTTableJoin::Kind::Full;
            }
        }

        if (kind == ASTTableJoin::Kind::Comma)
            throw Exception("Second parameter of storage Join must be LEFT or INNER or RIGHT or FULL (without quotes).",
                            ErrorCodes::BAD_ARGUMENTS);

        Names key_names;
        key_names.reserve(engine_args.size() - 2);
        for (size_t i = 2, size = engine_args.size(); i < size; ++i)
        {
            auto opt_key = tryGetIdentifierName(engine_args[i]);
            if (!opt_key)
                throw Exception("Parameter №" + toString(i + 1) + " of storage Join don't look like column name.", ErrorCodes::BAD_ARGUMENTS);

            key_names.push_back(*opt_key);
        }

        return StorageJoin::create(
            disk,
            args.relative_data_path,
            args.table_id,
            key_names,
            join_use_nulls,
            SizeLimits{max_rows_in_join, max_bytes_in_join, join_overflow_mode},
            kind,
            strictness,
            args.columns,
            args.constraints,
            args.comment,
            join_any_take_last_row,
            persistent);
    };

    factory.registerStorage("Join", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}

template <typename T>
static const char * rawData(T & t)
{
    return reinterpret_cast<const char *>(&t);
}
template <typename T>
static size_t rawSize(T &)
{
    return sizeof(T);
}
template <>
const char * rawData(const StringRef & t)
{
    return t.data;
}
template <>
size_t rawSize(const StringRef & t)
{
    return t.size;
}

class JoinSource : public SourceWithProgress
{
public:
    JoinSource(HashJoinPtr join_, TableLockHolder lock_holder_, UInt64 max_block_size_, Block sample_block_)
        : SourceWithProgress(sample_block_)
        , join(join_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
    {
        column_indices.resize(sample_block.columns());

        auto & saved_block = join->getJoinedData()->sample_block;

        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            auto & [_, type, name] = sample_block.getByPosition(i);
            if (join->right_table_keys.has(name))
            {
                key_pos = i;
                const auto & column = join->right_table_keys.getByName(name);
                restored_block.insert(column);
            }
            else
            {
                size_t pos = saved_block.getPositionByName(name);
                column_indices[i] = pos;

                const auto & column = saved_block.getByPosition(pos);
                restored_block.insert(column);
            }
        }
    }

    String getName() const override { return "Join"; }

protected:
    Chunk generate() override
    {
        if (join->data->blocks.empty())
            return {};

        Chunk chunk;
        if (!joinDispatch(join->kind, join->strictness, join->data->maps,
                [&](auto kind, auto strictness, auto & map) { chunk = createChunk<kind, strictness>(map); }))
            throw Exception("Logical error: unknown JOIN strictness", ErrorCodes::LOGICAL_ERROR);
        return chunk;
    }

private:
    HashJoinPtr join;
    TableLockHolder lock_holder;

    UInt64 max_block_size;
    Block sample_block;
    Block restored_block; /// sample_block with parent column types

    ColumnNumbers column_indices;
    std::optional<size_t> key_pos;

    std::unique_ptr<void, std::function<void(void *)>> position; /// type erasure


    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Chunk createChunk(const Maps & maps)
    {
        MutableColumns mut_columns = restored_block.cloneEmpty().mutateColumns();

        size_t rows_added = 0;

        switch (join->data->type)
        {
#define M(TYPE)                                           \
    case HashJoin::Type::TYPE:                                \
        rows_added = fillColumns<KIND, STRICTNESS>(*maps.TYPE, mut_columns); \
        break;
            APPLY_FOR_JOIN_VARIANTS_LIMITED(M)
#undef M

            default:
                throw Exception("Unsupported JOIN keys in StorageJoin. Type: " + toString(static_cast<UInt32>(join->data->type)),
                                ErrorCodes::UNSUPPORTED_JOIN_KEYS);
        }

        if (!rows_added)
            return {};

        Columns columns;
        columns.reserve(mut_columns.size());
        for (auto & col : mut_columns)
            columns.emplace_back(std::move(col));

        /// Correct nullability and LowCardinality types
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & src = restored_block.getByPosition(i);
            const auto & dst = sample_block.getByPosition(i);

            if (!src.type->equals(*dst.type))
            {
                auto arg = src;
                arg.column = std::move(columns[i]);
                columns[i] = castColumn(arg, dst.type);
            }
        }

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns)
    {
        size_t rows_added = 0;

        if (!position)
            position = decltype(position)(
                static_cast<void *>(new typename Map::const_iterator(map.begin())), //-V572
                [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

        auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
        auto end = map.end();

        for (; it != end; ++it)
        {
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::RightAny)
            {
                fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
            {
                fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                if constexpr (KIND == ASTTableJoin::Kind::Left || KIND == ASTTableJoin::Kind::Inner)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == ASTTableJoin::Kind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Semi)
            {
                if constexpr (KIND == ASTTableJoin::Kind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == ASTTableJoin::Kind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == ASTTableJoin::Strictness::Anti)
            {
                if constexpr (KIND == ASTTableJoin::Kind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == ASTTableJoin::Kind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else
                throw Exception("This JOIN is not implemented yet", ErrorCodes::NOT_IMPLEMENTED);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }

    template <typename Map>
    static void fillOne(MutableColumns & columns, const ColumnNumbers & column_indices, typename Map::const_iterator & it,
                        const std::optional<size_t> & key_pos, size_t & rows_added)
    {
        for (size_t j = 0; j < columns.size(); ++j)
            if (j == key_pos)
                columns[j]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
            else
                columns[j]->insertFrom(*it->getMapped().block->getByPosition(column_indices[j]).column.get(), it->getMapped().row_num);
        ++rows_added;
    }

    template <typename Map>
    static void fillAll(MutableColumns & columns, const ColumnNumbers & column_indices, typename Map::const_iterator & it,
                        const std::optional<size_t> & key_pos, size_t & rows_added)
    {
        for (auto ref_it = it->getMapped().begin(); ref_it.ok(); ++ref_it)
        {
            for (size_t j = 0; j < columns.size(); ++j)
                if (j == key_pos)
                    columns[j]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
                else
                    columns[j]->insertFrom(*ref_it->block->getByPosition(column_indices[j]).column.get(), ref_it->row_num);
            ++rows_added;
        }
    }
};


// TODO: multiple stream read and index read
Pipe StorageJoin::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    Block source_sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return Pipe(std::make_shared<JoinSource>(join, std::move(holder), max_block_size, source_sample_block));
}

}
