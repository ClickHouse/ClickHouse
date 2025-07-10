#include <Storages/StorageJoin.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSet.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Interpreters/JoinUtils.h>
#include <Formats/NativeWriter.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Poco/String.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool any_join_distinct_right_table_keys;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsOverflowMode join_overflow_mode;
    extern const SettingsBool join_use_nulls;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_rows_in_join;
    extern const SettingsUInt64 max_bytes_in_join;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DEADLOCK_AVOIDED;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_JOIN_KEYS;
}

StorageJoin::StorageJoin(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const Names & key_names_,
    bool use_nulls_,
    SizeLimits limits_,
    JoinKind kind_,
    JoinStrictness strictness_,
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
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Key column ({}) does not exist in table declaration.", key);

    table_join = std::make_shared<TableJoin>(limits, use_nulls, kind, strictness, key_names);
    join = std::make_shared<HashJoin>(table_join, getRightSampleBlock(), overwrite);
    restore();
    optimizeUnlocked();
}

RWLockImpl::LockHolder StorageJoin::tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef()[Setting::lock_acquire_timeout] : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return tryLockTimed(lock, type, query_id, acquire_timeout);
}

RWLockImpl::LockHolder StorageJoin::tryLockForCurrentQueryTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context)
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef()[Setting::lock_acquire_timeout] : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return lock->getLock(type, query_id, acquire_timeout, false);
}

SinkToStoragePtr StorageJoin::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    std::lock_guard mutate_lock(mutate_mutex);
    return StorageSetOrJoinBase::write(query, metadata_snapshot, context, /*async_insert=*/false);
}

bool StorageJoin::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & /* deduplicate_by_columns */,
    bool cleanup,
    ContextPtr context)
{

    if (partition)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition cannot be specified when optimizing table of type Join");

    if (final)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FINAL cannot be specified when optimizing table of type Join");

    if (deduplicate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DEDUPLICATE cannot be specified when optimizing table of type Join");

    if (cleanup)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEANUP cannot be specified when optimizing table of type Join");

    std::lock_guard mutate_lock(mutate_mutex);
    TableLockHolder lock_holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    optimizeUnlocked();
    return true;
}

void StorageJoin::optimizeUnlocked()
{
    size_t current_bytes = join->getTotalByteCount();
    size_t dummy = current_bytes;
    join->shrinkStoredBlocksToFit(dummy, true);

    size_t optimized_bytes = join->getTotalByteCount();
    if (current_bytes > optimized_bytes)
        LOG_INFO(getLogger("StorageJoin"), "Optimized Join storage from {} to {} bytes", current_bytes, optimized_bytes);
}

void StorageJoin::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, TableExclusiveLockHolder &)
{
    std::lock_guard mutate_lock(mutate_mutex);
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    if (disk->existsDirectory(path))
        disk->removeRecursive(path);
    else
        LOG_INFO(getLogger("StorageJoin"), "Path {} is already removed from disk {}", path, disk->getName());

    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    increment = 0;
    join = std::make_shared<HashJoin>(table_join, getRightSampleBlock(), overwrite);
}

void StorageJoin::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    for (const auto & command : commands)
        if (command.type != MutationCommand::DELETE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine Join supports only DELETE mutations");
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
    auto backup_stream = NativeWriter(compressed_backup_buf, 0, metadata_snapshot->getSampleBlock());

    auto new_data = std::make_shared<HashJoin>(table_join, getRightSampleBlock(), overwrite);

    // New scope controls lifetime of pipeline.
    {
        auto storage_ptr = DatabaseCatalog::instance().getTable(getStorageID(), context);
        MutationsInterpreter::Settings settings(true);
        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
        {
            new_data->addBlockToJoin(block, true);
            if (persistent)
                backup_stream.write(block);
        }
    }

    /// Now acquire exclusive lock and modify storage.
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Write, context);

    join = std::move(new_data);
    increment = 1;

    if (persistent)
    {
        backup_stream.flush();
        compressed_backup_buf.finalize();
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
    else
    {
        compressed_backup_buf.cancel();
        backup_buf->cancel();
    }
}

HashJoinPtr StorageJoin::getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, String query_id, std::chrono::milliseconds acquire_timeout, const Names & required_columns_names) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    if (!analyzed_join->sameStrictnessAndKind(strictness, kind))
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "Table '{}' has incompatible type of JOIN", getStorageID().getNameForLogs());

    if ((analyzed_join->forceNullableRight() && !use_nulls) ||
        (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Table {} needs the same join_use_nulls setting as present in LEFT or FULL JOIN",
            getStorageID().getNameForLogs());

    if (analyzed_join->getClauses().size() != 1)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "JOIN keys should match to the Join engine keys [{}]",
                        fmt::join(getKeyNames(), ", "));

    const auto & join_on = analyzed_join->getOnlyClause();
    if (join_on.on_filter_condition_left || join_on.on_filter_condition_right)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "ON section of JOIN with filter conditions is not implemented");

    const auto & key_names_right = join_on.key_names_right;
    const auto & key_names_left = join_on.key_names_left;
    if (key_names.size() != key_names_right.size() || key_names.size() != key_names_left.size())
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Number of keys in JOIN ON section ({}) doesn't match number of keys in Join engine ({})",
            key_names_right.size(), key_names.size());

    /* Resort left keys according to right keys order in StorageJoin
     * We can't change the order of keys in StorageJoin
     * because the hash table was already built with tuples serialized in the order of key_names.
     * If we try to use the same hash table with different order of keys,
     * then calculated hashes and the result of the comparison will be wrong.
     *
     * Example:
     * ```
     * CREATE TABLE t_right (a UInt32, b UInt32) ENGINE = Join(ALL, INNER, a, b);
     * SELECT * FROM t_left JOIN t_right ON t_left.y = t_right.b AND t_left.x = t_right.a;
     * ```
     * In that case right keys should still be (a, b), need to change the order of the left keys to (x, y).
     */
    Names left_key_names_resorted;
    for (const auto & key_name : key_names)
    {
        const auto & renamed_key = analyzed_join->renamedRightColumnNameWithAlias(key_name);
        /// find position of renamed_key in key_names_right
        auto it = std::find(key_names_right.begin(), key_names_right.end(), renamed_key);
        if (it == key_names_right.end())
            throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
                "Key '{}' not found in JOIN ON section. Join engine key{} '{}' have to be used",
                key_name, key_names.size() > 1 ? "s" : "", fmt::join(key_names, ", "));
        const size_t key_position = std::distance(key_names_right.begin(), it);
        left_key_names_resorted.push_back(key_names_left[key_position]);
    }

    /// Set qualified identifiers to original names (table.column -> column).
    /// It's required because storage join stores non-qualified names.
    /// Qualifies will be added by join implementation (TableJoin contains a rename mapping).
    analyzed_join->setRightKeys(key_names);
    analyzed_join->setLeftKeys(left_key_names_resorted);
    Block right_sample_block;
    for (const auto & name : required_columns_names)
        right_sample_block.insert(getRightSampleBlock().getByName(name));
    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, right_sample_block);

    RWLockImpl::LockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, query_id, acquire_timeout);
    join_clone->setLock(holder);
    join_clone->reuseJoinedData(*join);

    return join_clone;
}

HashJoinPtr StorageJoin::getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, ContextPtr context, const Names & required_columns_names) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? context->getSettingsRef()[Setting::lock_acquire_timeout] : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);

    return getJoinLocked(analyzed_join, query_id, acquire_timeout, required_columns_names);
}

void StorageJoin::insertBlock(const Block & block, ContextPtr context)
{
    Block block_to_insert = block;
    convertRightBlock(block_to_insert);
    TableLockHolder holder = tryLockForCurrentQueryTimedWithContext(rwlock, RWLockImpl::Write, context);

    /// Protection from `INSERT INTO test_table_join SELECT * FROM test_table_join`
    if (!holder)
        throw Exception(ErrorCodes::DEADLOCK_AVOIDED, "StorageJoin: cannot insert data because current query tries to read from this storage");

    join->addBlockToJoin(block_to_insert, true);
}

size_t StorageJoin::getSize(ContextPtr context) const
{
    TableLockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return join->getTotalRowCount();
}

std::optional<UInt64> StorageJoin::totalRows(ContextPtr query_context) const
{
    const auto & settings = query_context->getSettingsRef();
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings[Setting::lock_acquire_timeout]);
    return join->getTotalRowCount();
}

std::optional<UInt64> StorageJoin::totalBytes(ContextPtr query_context) const
{
    const auto & settings = query_context->getSettingsRef();
    TableLockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, RWLockImpl::NO_QUERY, settings[Setting::lock_acquire_timeout]);
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

void StorageJoin::convertRightBlock(Block & block) const
{
    bool need_covert = use_nulls && isLeftOrFull(kind);
    if (!need_covert)
        return;

    for (auto & col : block)
        JoinCommon::convertColumnToNullable(col);
}

void registerStorageJoin(StorageFactory & factory)
{
    auto has_builtin_fn = [](std::string_view name)
    {
        static const std::unordered_set<std::string_view> valid_settings
            = {"join_use_nulls",
               "max_rows_in_join",
               "max_bytes_in_join",
               "join_overflow_mode",
               "join_any_take_last_row",
               "any_join_distinct_right_table_keys",
               "disk",
               "persistent"};
        return valid_settings.contains(name);
    };

    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        /// Join(ANY, LEFT, k1, k2, ...)

        ASTs & engine_args = args.engine_args;

        const auto & settings = args.getContext()->getSettingsRef();

        auto join_use_nulls = settings[Setting::join_use_nulls];
        auto max_rows_in_join = settings[Setting::max_rows_in_join];
        auto max_bytes_in_join = settings[Setting::max_bytes_in_join];
        auto join_overflow_mode = settings[Setting::join_overflow_mode];
        auto join_any_take_last_row = settings[Setting::join_any_take_last_row];
        auto old_any_join = settings[Setting::any_join_distinct_right_table_keys];
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
                    disk_name = setting.value.safeGet<String>();
                else if (setting.name == "persistent")
                {
                    persistent = setting.value.safeGet<bool>();
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting {} for storage {}", setting.name, args.engine_name);
            }
        }

        DiskPtr disk = args.getContext()->getDisk(disk_name);

        if (engine_args.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Join requires at least 3 parameters: "
                            "Join(ANY|ALL|SEMI|ANTI, LEFT|INNER|RIGHT, keys...).");

        JoinStrictness strictness = JoinStrictness::Unspecified;
        JoinKind kind = JoinKind::Comma;

        if (auto opt_strictness_id = tryGetIdentifierName(engine_args[0]))
        {
            const String strictness_str = Poco::toLower(*opt_strictness_id);

            if (strictness_str == "any")
            {
                if (old_any_join)
                    strictness = JoinStrictness::RightAny;
                else
                    strictness = JoinStrictness::Any;
            }
            else if (strictness_str == "all")
                strictness = JoinStrictness::All;
            else if (strictness_str == "semi")
                strictness = JoinStrictness::Semi;
            else if (strictness_str == "anti")
                strictness = JoinStrictness::Anti;
        }

        if (strictness == JoinStrictness::Unspecified)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First parameter of storage Join must be ANY or ALL or SEMI or ANTI (without quotes).");

        if (auto opt_kind_id = tryGetIdentifierName(engine_args[1]))
        {
            const String kind_str = Poco::toLower(*opt_kind_id);

            if (kind_str == "left")
                kind = JoinKind::Left;
            else if (kind_str == "inner")
                kind = JoinKind::Inner;
            else if (kind_str == "right")
                kind = JoinKind::Right;
            else if (kind_str == "full")
            {
                if (strictness == JoinStrictness::Any)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ANY FULL JOINs are not implemented");
                kind = JoinKind::Full;
            }
        }

        if ((strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti) && (kind != JoinKind::Left && kind != JoinKind::Right))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, " SEMI|ANTI JOIN should be LEFT or RIGHT");

        if (kind == JoinKind::Comma)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter of storage Join must be LEFT or INNER or RIGHT or FULL (without quotes).");

        Names key_names;
        key_names.reserve(engine_args.size() - 2);
        for (size_t i = 2, size = engine_args.size(); i < size; ++i)
        {
            auto opt_key = tryGetIdentifierName(engine_args[i]);
            if (!opt_key)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter №{} of storage Join don't look like column name.", i + 1);

            key_names.push_back(*opt_key);
        }

        return std::make_shared<StorageJoin>(
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

    factory.registerStorage(
        "Join",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .has_builtin_setting_fn = has_builtin_fn,
        });
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

class JoinSource : public ISource
{
public:
    JoinSource(HashJoinPtr join_, TableLockHolder lock_holder_, UInt64 max_block_size_, Block sample_block_)
        : ISource(sample_block_)
        , join(join_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
    {
        if (!join->getTableJoin().oneDisjunct())
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin does not support OR for keys in JOIN ON section");

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
        if (join->data->columns.empty())
            return {};

        Chunk chunk;
        if (!joinDispatch(
                join->kind,
                join->strictness,
                join->data->maps.front(),
                join->table_join->getMixedJoinExpression() != nullptr,
                [&](auto kind, auto strictness, auto & map) { chunk = createChunk<kind, strictness>(map); }))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness");
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


    template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
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
                throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys of type {} in StorageJoin", join->data->type);
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

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns)
    {
        size_t rows_added = 0;

        if (!position)
            position = decltype(position)(
                static_cast<void *>(new typename Map::const_iterator(map.begin())),
                [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

        auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
        auto end = map.end();

        for (; it != end; ++it)
        {
            if constexpr (STRICTNESS == JoinStrictness::RightAny)
            {
                fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::All)
            {
                fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Any)
            {
                if constexpr (KIND == JoinKind::Left || KIND == JoinKind::Inner)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Semi)
            {
                if constexpr (KIND == JoinKind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Anti)
            {
                if constexpr (KIND == JoinKind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This JOIN is not implemented yet");

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
                columns[j]->insertFrom(*(*it->getMapped().columns)[column_indices[j]], it->getMapped().row_num);
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
                    columns[j]->insertFrom(*(*ref_it->columns)[column_indices[j]], ref_it->row_num);
            ++rows_added;
        }
    }
};


// TODO: multiple stream read and index read
Pipe StorageJoin::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block source_sample_block = storage_snapshot->getSampleBlockForColumns(column_names);
    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return Pipe(std::make_shared<JoinSource>(join, std::move(holder), max_block_size, source_sample_block));
}

}
