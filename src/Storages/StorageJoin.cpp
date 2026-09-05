#include <Storages/StorageJoin.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSet.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
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
#include <Common/CurrentThread.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/BaseSettings.h>
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
#include <numeric>
#include <unordered_set>


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
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    for (const auto & key : key_names)
        if (!metadata_snapshot->getColumns().hasPhysical(key))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Key column ({}) does not exist in table declaration.", key);

    table_join = std::make_shared<TableJoin>(limits, use_nulls, kind, strictness, key_names);
    join = std::make_shared<HashJoin>(table_join, std::make_shared<const Block>(getRightSampleBlock()), overwrite);
    restore();
    optimizeUnlocked();
}

RWLockImpl::LockHolder StorageJoin::tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()) : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
    return tryLockTimed(lock, type, query_id, Poco::Timespan(acquire_timeout.count() * 1000));
}

RWLockImpl::LockHolder StorageJoin::tryLockForCurrentQueryTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context)
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()) : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);
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
    join = std::make_shared<HashJoin>(table_join, std::make_shared<const Block>(getRightSampleBlock()), overwrite);
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
    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);

    auto backup_buf = disk->writeFile(path + tmp_backup_file_name);
    auto compressed_backup_buf = CompressedWriteBuffer(*backup_buf);
    auto backup_stream = NativeWriter(compressed_backup_buf, 0, std::make_shared<const Block>(metadata_snapshot->getSampleBlock()));

    auto new_data = std::make_shared<HashJoin>(table_join, std::make_shared<const Block>(getRightSampleBlock()), overwrite);

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
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
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

    /// The prebuilt join is reused as is (see reuseJoinedData below), so it cannot serve an
    /// expression the query derived: the names are unqualified, the saved block has a different
    /// layout and the maps variant may differ.
    if (analyzed_join->getMixedJoinExpression())
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "ON section of JOIN with a condition involving columns from both tables is not implemented for the Join table engine");

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
    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, std::make_shared<const Block>(std::move(right_sample_block)));

    RWLockImpl::LockHolder holder = tryLockTimed(rwlock, RWLockImpl::Read, query_id, Poco::Timespan(acquire_timeout.count() * 1000));
    join_clone->setLock(holder);
    join_clone->reuseJoinedData(*join);

    return join_clone;
}

HashJoinPtr StorageJoin::getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, ContextPtr context, const Names & required_columns_names) const
{
    const String query_id = context ? context->getInitialQueryId() : RWLockImpl::NO_QUERY;
    const std::chrono::milliseconds acquire_timeout
        = context ? std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()) : std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC);

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

void registerStorageJoin(StorageFactory & factory);
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
                /// These settings are read here rather than applied to a `BaseSettings`, so there is no
                /// settings schema to check the value-less form `SETTINGS name` against - it stands for
                /// `name = true` and is only meaningful for the Bool ones. Without this check
                /// `SETTINGS max_rows_in_join` would silently become `max_rows_in_join = 1`.
                if (setting.shorthand && setting.name != "join_use_nulls" && setting.name != "join_any_take_last_row"
                    && setting.name != "any_join_distinct_right_table_keys" && setting.name != "persistent")
                    BaseSettingsHelpers::throwValuelessSettingIsNotBool(setting.name);

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
        },
        Documentation{
            .description = R"DOCS_MD(
Optional prepared data structure for usage in [JOIN](/reference/statements/select/join) operations.

:::note
In ClickHouse Cloud, if your service was created with a version earlier than 25.4, you will need to set the compatibility to at least 25.4 using  `SET compatibility=25.4`.
:::

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

See the detailed description of the [CREATE TABLE](/reference/statements/create/table) query.

## Engine parameters {#engine-parameters}

### `join_strictness` {#join_strictness}

`join_strictness` – [JOIN strictness](/reference/statements/select/join#supported-types-of-join).

### `join_type` {#join_type}

`join_type` – [JOIN type](/reference/statements/select/join#supported-types-of-join).

### Key columns {#key-columns}

`k1[, k2, ...]` – Key columns from the `USING` clause that the `JOIN` operation is made with.

Enter `join_strictness` and `join_type` parameters without quotes, for example, `Join(ANY, LEFT, col1)`. They must match the `JOIN` operation that the table will be used for. If the parameters do not match, ClickHouse does not throw an exception and may return incorrect data.

## Specifics and recommendations {#specifics-and-recommendations}

### Data storage {#data-storage}

`Join` table data is always located in the RAM. When inserting rows into a table, ClickHouse writes data blocks to the directory on the disk so that they can be restored when the server restarts.

If the server restarts incorrectly, the data block on the disk might get lost or damaged. In this case, you may need to manually delete the file with damaged data.

### Selecting and Inserting Data {#selecting-and-inserting-data}

You can use `INSERT` queries to add data to the `Join`-engine tables. If the table was created with the `ANY` strictness, data for duplicate keys are ignored. With the `ALL` strictness, all rows are added.

Main use-cases for `Join`-engine tables are following:

- Place the table to the right side in a `JOIN` clause.
- Call the [joinGet](/reference/functions/regular-functions/other-functions#joinGet) function, which lets you extract data from the table the same way as from a dictionary.

### Deleting data {#deleting-data}

`ALTER DELETE` queries for `Join`-engine tables are implemented as [mutations](/reference/statements/alter#mutations). `DELETE` mutation reads filtered data and overwrites data of memory and disk.

### Limitations and settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

#### `join_use_nulls` {#join_use_nulls}

[join_use_nulls](/reference/settings/session-settings/join#join_use_nulls)

#### `max_rows_in_join` {#max_rows_in_join}

[max_rows_in_join](/reference/settings/session-settings/max-rows#max_rows_in_join)

#### `max_bytes_in_join` {#max_bytes_in_join}

[max_bytes_in_join](/reference/settings/session-settings/max-bytes#max_bytes_in_join)

#### `join_overflow_mode` {#join_overflow_mode}

[join_overflow_mode](/reference/settings/session-settings/join#join_overflow_mode)

#### `join_any_take_last_row` {#join_any_take_last_row}

[join_any_take_last_row](/reference/settings/session-settings/join#join_any_take_last_row)
#### `join_use_nulls` {#join_use_nulls-1}

#### Persistent {#persistent}

Disables persistency for the Join and [Set](/reference/engines/table-engines/special/set) table engines.

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.

The `Join`-engine tables can't be used in `GLOBAL JOIN` operations.

The `Join`-engine allows to specify [join_use_nulls](/reference/settings/session-settings/join#join_use_nulls) setting in the `CREATE TABLE` statement. [SELECT](/reference/statements/select/index) query should have the same `join_use_nulls` value.

## Usage examples {#example}

Creating the left-side table:

```sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog;
```

```sql
INSERT INTO id_val VALUES (1,11), (2,12), (3,13);
```

Creating the right-side `Join` table:

```sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);
```

```sql
INSERT INTO id_val_join VALUES (1,21), (1,22), (3,23);
```

Joining the tables:

```sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id);
```

```text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │               0 │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

As an alternative, you can retrieve data from the `Join` table, specifying the join key value:

```sql
SELECT joinGet('id_val_join', 'val', toUInt32(1));
```

```text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

Deleting a row from the `Join` table:

```sql
ALTER TABLE id_val_join DELETE WHERE id = 3;
```

```text
┌─id─┬─val─┐
│  1 │  21 │
└────┴─────┘
```
)DOCS_MD",
            .syntax = "ENGINE = Join(join_strictness, join_type, k1[, k2, ...])",
            .related = {"Set"}});
}

namespace
{

template <typename T>
const char * rawData(const T & t)
{
    return reinterpret_cast<const char *>(&t);
}

template <typename T>
size_t rawSize(const T &)
{
    return sizeof(T);
}

template <>
const char * rawData(const std::string_view & t)
{
    /// We must return a non-null pointer for empty strings because ColumnNullable::insertData
    /// treats nullptr as NULL. Empty string_views used as "zero keys" in hash tables have
    /// data() == nullptr, but they represent empty strings, not NULLs.
    static constexpr char empty_string[] = "";
    return t.data() ? t.data() : empty_string;
}

template <>
size_t rawSize(const std::string_view & t)
{
    return t.size();
}

/// Byte range of one key column inside a packed map key, plus the output column it belongs to.
struct PackedKeyColumn
{
    size_t output_pos;
    size_t offset;
    size_t width;
};

/// How output key columns are recovered from a map key: the whole key for the single-key maps, or
/// one byte range per key column for the keysN maps, which pack all key columns into one blob.
struct KeyLayout
{
    std::optional<size_t> whole_key_pos;
    std::vector<PackedKeyColumn> packed;
    /// Indexed by output column position, so the fill loops stay a constant-time test per column.
    std::vector<bool> is_key_column;
};

/// HashMethodKeysFixed is the only join key getter that packs key columns into one blob, and the only
/// one declaring shuffleKeyColumns, so this selects exactly the keysN maps.
template <typename KeyGetter>
concept PacksKeysIntoBlob = requires(std::vector<IColumn *> & columns, const Sizes & sizes) {
    { KeyGetter::shuffleKeyColumns(columns, sizes) } -> std::same_as<std::optional<Sizes>>;
};

/// Order in which key slots occupy bytes of the packed key. `packed_sizes` is what
/// HashMethodKeysFixed::shuffleKeyColumns reports: the widths in packed order, or nothing for plain
/// clause order. Slots of one width keep their clause order there, which makes the mapping unique.
std::vector<size_t> packedKeyOrder(const Sizes & clause_sizes, const std::optional<Sizes> & packed_sizes)
{
    std::vector<size_t> order(clause_sizes.size());
    if (!packed_sizes)
    {
        std::iota(order.begin(), order.end(), 0);
        return order;
    }

    if (packed_sizes->size() != clause_sizes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "StorageJoin packed key has {} components but the join has {} keys",
            packed_sizes->size(),
            clause_sizes.size());

    std::vector<bool> taken(clause_sizes.size(), false);
    for (size_t position = 0; position < packed_sizes->size(); ++position)
    {
        auto slot = clause_sizes.size();
        for (size_t i = 0; i < clause_sizes.size(); ++i)
        {
            if (!taken[i] && clause_sizes[i] == (*packed_sizes)[position])
            {
                slot = i;
                break;
            }
        }
        if (slot == clause_sizes.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "StorageJoin has no key of size {} left to pack", (*packed_sizes)[position]);
        taken[slot] = true;
        order[position] = slot;
    }
    return order;
}

}

class JoinSource final : public ISource
{
public:
    JoinSource(HashJoinPtr join_, TableLockHolder lock_holder_, UInt64 max_block_size_, SharedHeader sample_block_)
        : ISource(sample_block_)
        , join(join_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
    {
        const auto & table_join = join->getTableJoin();
        if (!table_join.oneDisjunct())
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin does not support OR for keys in JOIN ON section");

        column_indices.resize(sample_block->columns());

        auto & saved_block = join->getJoinedData()->sample_block;
        std::unordered_map<String, size_t> key_output_positions;

        for (size_t i = 0; i < sample_block->columns(); ++i)
        {
            const auto & [_, type, name] = sample_block->getByPosition(i);
            if (join->right_table_keys.has(name))
            {
                key_pos = i;
                key_output_positions.emplace(name, i);
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

        /// Key slots of a packed map key, in engine-clause order. They come from the clause and not
        /// from right_table_keys, which deduplicates a repeated key name while the packed key does not.
        const auto & key_names_right = table_join.getOnlyClause().key_names_right;
        const auto & key_sizes = join->getKeySizes().at(0);
        if (key_names_right.size() != key_sizes.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "StorageJoin has {} key names but {} key sizes",
                key_names_right.size(),
                key_sizes.size());

        key_slots.reserve(key_names_right.size());
        for (size_t slot = 0; slot < key_names_right.size(); ++slot)
        {
            auto it = key_output_positions.find(key_names_right[slot]);
            size_t output_pos = it == key_output_positions.end() ? not_selected : it->second;
            key_slots.push_back({output_pos, 0, key_sizes[slot]});
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
                join->getMapsKind(),
                [&](auto kind, auto strictness, auto & map)
                {
                    /// `StorageJoin` reads the right rows back out of the maps, so it never stores them
                    /// in a map that keeps none.
                    if constexpr (SetJoinMaps<decltype(map)>)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageJoin cannot read rows from a set map");
                    else
                        chunk = createChunk<kind, strictness>(map);
                }))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness");
        return chunk;
    }

private:
    HashJoinPtr join;
    TableLockHolder lock_holder;

    UInt64 max_block_size;
    SharedHeader sample_block;
    Block restored_block; /// sample_block with parent column types

    static constexpr size_t not_selected = std::numeric_limits<size_t>::max();

    ColumnNumbers column_indices;
    std::optional<size_t> key_pos;
    /// One entry per engine key argument, in clause order; `offset` is filled per map variant.
    std::vector<PackedKeyColumn> key_slots;

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
        rows_added = fillColumns<KIND, STRICTNESS, HashJoin::Type::TYPE>(*maps.TYPE, mut_columns); \
        break;
            APPLY_FOR_JOIN_VARIANTS_LIMITED(M)
#undef M

            default:
                throw Exception(
                    ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                    "Cannot read a Join table whose keys are stored as {}: the key values are not recoverable "
                    "from the map. Read it with a JOIN or joinGet instead",
                    join->data->type);
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
            const auto & dst = sample_block->getByPosition(i);

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

    template <JoinKind KIND, JoinStrictness STRICTNESS, HashJoin::Type TYPE, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns)
    {
        size_t rows_added = 0;
        const StoredBlock * const * stored_columns = join->getJoinedData()->stored_columns_index->blocksData();
        const KeyLayout layout = makeKeyLayout<TYPE, Map>();

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
                fillOne<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
            }
            else if constexpr (STRICTNESS == JoinStrictness::All)
            {
                fillAll<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Any)
            {
                if constexpr (KIND == JoinKind::Left || KIND == JoinKind::Inner)
                    fillOne<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Semi)
            {
                if constexpr (KIND == JoinKind::Left)
                    fillOne<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Anti)
            {
                if constexpr (KIND == JoinKind::Left)
                    fillOne<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, layout, rows_added, stored_columns);
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

    /// Byte ranges of the key columns inside this map's key. Empty `packed` means the map key is a
    /// single key value inserted whole, which is how every non-keysN variant behaves.
    template <HashJoin::Type TYPE, typename Map>
    KeyLayout makeKeyLayout() const
    {
        KeyLayout layout;
        layout.whole_key_pos = key_pos;
        layout.is_key_column.assign(sample_block->columns(), false);
        if (key_pos)
            layout.is_key_column[*key_pos] = true;

        /// Note key32 and keys32 (likewise key64/keys64) share one map type, so the variant has to come
        /// from the enum: only the keysN ones pack several key columns into the map key.
        using KeyGetter = typename KeyGetterForType<TYPE, std::remove_cvref_t<Map>>::Type;
        if constexpr (PacksKeysIntoBlob<KeyGetter>)
        {
            Sizes clause_sizes;
            clause_sizes.reserve(key_slots.size());
            for (const auto & slot : key_slots)
                clause_sizes.push_back(slot.width);

            /// Aggregation reports the packed order the same way (Aggregator.cpp: shuffleKeyColumns
            /// feeding the sizes to insertKeyIntoColumns), so the two cannot disagree about the layout.
            std::vector<IColumn *> unused_columns(key_slots.size(), nullptr);
            const auto order = packedKeyOrder(clause_sizes, KeyGetter::shuffleKeyColumns(unused_columns, clause_sizes));

            size_t offset = 0;
            std::unordered_set<size_t> emitted;
            for (size_t slot : order)
            {
                auto entry = key_slots[slot];
                entry.offset = offset;
                offset += entry.width;
                /// A key name repeated in the engine arguments (Join(ALL, INNER, a, a, b)) occupies one
                /// slot per argument, all holding the same value, but has one output column.
                if (entry.output_pos != not_selected && emitted.insert(entry.output_pos).second)
                {
                    layout.packed.push_back(entry);
                    layout.is_key_column[entry.output_pos] = true;
                }
            }

            if (offset > sizeof(typename Map::key_type))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "StorageJoin keys occupy {} bytes but the map key holds {}",
                    offset,
                    sizeof(typename Map::key_type));

            layout.whole_key_pos.reset();
        }
        return layout;
    }

    template <typename Map>
    static void insertKey(MutableColumns & columns, const KeyLayout & layout, typename Map::const_iterator & it)
    {
        if (layout.whole_key_pos)
            columns[*layout.whole_key_pos]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
        else
            for (const auto & slot : layout.packed)
                columns[slot.output_pos]->insertData(rawData(it->getKey()) + slot.offset, slot.width);
    }

    template <typename Map>
    static void fillOne(MutableColumns & columns, const ColumnNumbers & column_indices, typename Map::const_iterator & it,
                        const KeyLayout & layout, size_t & rows_added, const StoredBlock * const * stored_columns)
    {
        /// The mapped value of MapsOne is a single encoded ref; the mapped value of MapsAll
        /// (RightAny under preferUseMapsAll) is a tagged ref list whose first element is taken.
        const UInt64 ref_word = firstRefWord(it->getMapped());
        const StoredBlock * block = stored_columns[refWordBlockNo(ref_word)];
        for (size_t j = 0; j < columns.size(); ++j)
            if (!layout.is_key_column[j])
                columns[j]->insertFrom(*block->columns[column_indices[j]], refWordRowNo(ref_word));
        insertKey<Map>(columns, layout, it);
        ++rows_added;
    }

    template <typename Map>
    static void fillAll(MutableColumns & columns, const ColumnNumbers & column_indices, typename Map::const_iterator & it,
                        const KeyLayout & layout, size_t & rows_added, const StoredBlock * const * stored_columns)
    {
        for (auto ref_it = it->getMapped().begin(); ref_it.ok(); ++ref_it)
        {
            const UInt64 ref_word = *ref_it;
            const StoredBlock * block = stored_columns[refWordBlockNo(ref_word)];
            for (size_t j = 0; j < columns.size(); ++j)
                if (!layout.is_key_column[j])
                    columns[j]->insertFrom(*block->columns[column_indices[j]], refWordRowNo(ref_word));
            insertKey<Map>(columns, layout, it);
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

    auto source_sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    RWLockImpl::LockHolder holder = tryLockTimedWithContext(rwlock, RWLockImpl::Read, context);
    return Pipe(std::make_shared<JoinSource>(join, std::move(holder), max_block_size, source_sample_block));
}

}
