#pragma once

#include <atomic>

#include <Storages/IStorage.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <base/scope_guard.h>


namespace DB
{

struct DictionaryStructure;
class TableFunctionDictionary;
class IDictionary;

class StorageDictionary final : public IStorage, public WithContext
{
friend class TableFunctionDictionary;

public:
    /// Specifies where the table is located relative to the dictionary.
    enum class Location : uint8_t
    {
        /// Table was created automatically as an element of a database with the Dictionary engine.
        DictionaryDatabase,

        /// Table was created automatically along with a dictionary
        /// and has the same database and name as the dictionary.
        /// It provides table-like access to the dictionary.
        /// User cannot drop that table.
        SameDatabaseAndNameAsDictionary,

        /// Table was created explicitly by a statement like
        /// CREATE TABLE ... ENGINE=Dictionary
        /// User chose the table's database and name and can drop that table.
        Custom,
    };

    StorageDictionary(
        const StorageID & table_id_,
        const String & dictionary_name_,
        const ColumnsDescription & columns_,
        const String & comment,
        Location location_,
        ContextPtr context_);

    StorageDictionary(
        const StorageID & table_id_,
        const String & dictionary_name_,
        const DictionaryStructure & dictionary_structure,
        const String & comment,
        Location location_,
        ContextPtr context_);

    StorageDictionary(
        const StorageID & table_id_,
        LoadablesConfigurationPtr dictionary_configuration_,
        ContextPtr context_);

    std::string getName() const override { return "Dictionary"; }

    ~StorageDictionary() override;

    void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const override;
    void checkTableCanBeDetached() const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t threads) override;

    /// FIXME: processing after reading from dictionaries are not parallelized due to some bug:
    /// count() can return wrong result, see test_dictionaries_redis/test_long.py::test_redis_dict_long
    bool parallelizeOutputAfterReading(ContextPtr) const override { return false; }

    std::shared_ptr<const IDictionary> getDictionary() const;

    static NamesAndTypesList getNamesAndTypes(const DictionaryStructure & dictionary_structure, bool validate_id_type);

    bool isDictionary() const override { return true; }
    void shutdown(bool is_drop) override;
    void startup() override;

    void renameInMemory(const StorageID & new_table_id) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const override;

    void alter(const AlterCommands & params, ContextPtr alter_context, AlterLockHolder &) override;

    LoadablesConfigurationPtr getConfiguration() const;

    String getDictionaryName() const { return dictionary_name; }

private:
    String dictionary_name;
    const Location location;

    mutable std::mutex dictionary_config_mutex;
    LoadablesConfigurationPtr configuration TSA_GUARDED_BY(dictionary_config_mutex);

    scope_guard remove_repository_callback;

    void removeDictionaryConfigurationFromRepository();
};

}
