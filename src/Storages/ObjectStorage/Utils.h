#pragma once
#include <functional>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/NumberedFileName.h>
#include <Storages/StorageFactory.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class IObjectStorage;

/// The keys that one insert has generated for the objects it is writing, held for as long as the insert
/// lasts, so that a concurrent insert into the same table cannot generate the same key while the object
/// is not there yet - see `StorageObjectStorageConfiguration::tryReservePathForWrite`. The reservations
/// are released when the object is destroyed, which is when the sink of the insert is gone: by then every
/// key of the insert either names a committed object, or names nothing at all because the insert failed.
class WrittenPathReservations
{
public:
    explicit WrittenPathReservations(StorageObjectStorageConfigurationPtr configuration_)
        : configuration(std::move(configuration_))
    {
    }

    WrittenPathReservations(const WrittenPathReservations &) = delete;
    WrittenPathReservations & operator=(const WrittenPathReservations &) = delete;

    ~WrittenPathReservations()
    {
        for (const auto & path : reserved)
            configuration->releasePathReservedForWrite(path);
    }

    /// Returns false when another insert into the same table is already writing this key.
    bool tryReserve(const std::string & path)
    {
        if (!configuration->tryReservePathForWrite(path))
            return false;
        reserved.push_back(path);
        return true;
    }

    /// The same for the key the insert starts with, which is a part of the table already,
    /// see `StorageObjectStorageConfiguration::tryReserveStartingPathForWrite`.
    bool tryReserveStartingPath(const std::string & path)
    {
        if (!configuration->tryReserveStartingPathForWrite(path))
            return false;
        reserved.push_back(path);
        return true;
    }

private:
    StorageObjectStorageConfigurationPtr configuration;
    std::vector<std::string> reserved;
};

using WrittenPathReservationsPtr = std::shared_ptr<WrittenPathReservations>;

/// Checks whether the insert can write into the object with the given key, and reserves the key it is going
/// to write. If the object exists and `*_create_new_file_on_insert` is enabled, returns the first free key
/// of `numbered_keys` starting from `sequence_number`, which is advanced past the returned key so that an
/// insert split by size continues the numbering from it. A key that another insert into the same table has
/// reserved is taken as well - also the starting key itself: its object is not there until the insert writing
/// it is committed, and without the reservation every concurrent insert would start writing the same key.
std::optional<std::string> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageQuerySettings & settings,
    const std::string & key,
    const NumberedFileNames & numbered_keys,
    size_t & sequence_number,
    WrittenPathReservations & reservations);

/// Returns the key of the next object to write when the data is split by size (see `*_split_on_write_by_size_bytes`).
/// `sequence_number` is advanced past the returned key. If the generated key is already taken, either the number is
/// skipped (when `create_new_file_on_insert` is enabled) or an exception is thrown. A key reserved by another insert
/// into the same table is skipped whatever the settings are: the object is not there yet, but it is being written.
std::string getNextKeyForSplittingBySize(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageQuerySettings & settings,
    const NumberedFileNames & numbered_keys,
    size_t & sequence_number,
    WrittenPathReservations & reservations);

/// A truncating insert overwrites the whole dataset of the table. If the previous insert has produced
/// more objects than the current one, the leftovers have to be deleted - otherwise the stale data will be
/// still visible both for the readers of this table and for the readers of the glob pattern over the prefix.
///
/// This is the precise variant, for a table that has written these objects itself and still remembers them:
/// exactly they are deleted, even if the previous insert had to skip some of the numbers because the keys
/// were taken by someone else.
/// `on_removed` is called for every key that is no longer there, right after it is gone, so that the caller can
/// retire it from the list of the paths of the table one by one. A cleanup that throws in the middle then leaves
/// the table reading exactly the objects that still exist, instead of the ones it has already removed.
/// Every removal is written to `log`: not every object storage logs the objects it deletes itself.
/// A key that another insert into the table is still writing (see `StorageObjectStorageConfiguration::isPathReservedForWrite`)
/// is left alone, and stays in the list: it is not a leftover of a previous insert but a part of one that is not over yet.
void removeStaleSplitObjects(
    IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const std::vector<std::string> & stale_keys,
    const std::function<void(const std::string &)> & on_removed,
    const LoggerPtr & log);

/// The same for a table that does not know the keys of the objects of the previous insert - an `INSERT` into
/// a table function, or a table that was reloaded since then. The objects are written with consecutive numbers
/// starting from `numbered_keys.start_sequence_number`, so the removal stops at the first missing number.
/// Nothing is removed if `create_new_file_on_insert` is enabled - see the comment in the implementation.
/// A key that another insert into the table is still writing is skipped, like in `removeStaleSplitObjects`.
void removeStaleSplitObjectsByNumber(
    IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const NumberedFileNames & numbered_keys,
    bool create_new_file_on_insert,
    const LoggerPtr & log);

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    std::string & sample_path,
    const ContextPtr & context);

void validateSupportedColumns(
    ColumnsDescription & columns,
    const StorageObjectStorageConfiguration & configuration);

/// An empty column name has no identifier to render it with, so it cannot survive analysis.
void validateLakeSchemaColumnNames(const NamesAndTypesList & schema, std::string_view lake_name);

std::unique_ptr<ReadBufferFromFileBase> createReadBuffer(
    RelativePathWithMetadata & object_info,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context_,
    const LoggerPtr & log,
    const std::optional<ReadSettings> & read_settings = std::nullopt,
    bool allow_page_cache = true);

/// Joins an object's path under a storage prefix (a namespace, or a data source description).
/// A leading separator is dropped only when there is a prefix to join under, since `fs::path`
/// would otherwise treat the path as absolute and discard the prefix. An empty prefix leaves the
/// path as written: on a filesystem-backed storage that separator is what makes a path absolute.
std::string joinPathUnderPrefix(const std::string & prefix, const std::string & path);

/// Inverse of `joinPathUnderPrefix` under the same prefix. An empty prefix again needs care, for
/// the opposite reason: `fs::relative` of an absolute path against an empty base is the empty
/// path, which would lose the value rather than leave it.
std::string relativizePathUnderPrefix(const std::string & prefix, const std::string & path);

std::string formatObjectPath(
    const StorageObjectStorageConfiguration & configuration, const std::string & path, bool include_connection_info);
/// `joinPathUnderPrefix` is not injective under a non-empty prefix: a key with a leading separator
/// and the same key without it render to the same `_path` value, so `relativizePathUnderPrefix`
/// alone cannot tell which of them produced a given value. Returns every key that could have, so
/// that a caller which needs the original key can see when the answer is not unique.
Strings candidateKeysUnderPrefix(const std::string & prefix, const std::string & path);

ASTs::iterator getFirstKeyValueArgument(ASTs & args);
std::unordered_map<std::string, Field> parseKeyValueArguments(const ASTs & function_args, ContextPtr context);

template <typename T>
std::optional<T> getFromPositionOrKeyValue(
    const std::string & key,
    const ASTs & args,
    const std::unordered_map<std::string_view, size_t> & engine_args_to_idx,
    const std::unordered_map<std::string, Field> & key_value_args)
{
    if (auto arg_it = key_value_args.find(key); arg_it != key_value_args.end())
        return arg_it->second.safeGet<T>();

    if (auto arg_it = engine_args_to_idx.find(key); arg_it != engine_args_to_idx.end())
        return checkAndGetLiteralArgument<T>(args[arg_it->second], key);

    return std::nullopt;
};

struct ParseFromDiskResult
{
    String path_suffix;
    std::optional<String> format;
    std::optional<String> structure;
    std::optional<String> compression_method;
};

ParseFromDiskResult parseFromDisk(ASTs args, bool with_structure, ContextPtr context, const fs::path & prefix);

void expandPaimonKeeperMacrosIfNeeded(
    const StorageFactory::Arguments & args,
    const DataLakeStorageSettingsPtr & storage_settings);


}
