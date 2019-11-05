#include <Databases/DatabaseWithDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDictionary.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ALREADY_EXISTS;
}


void DatabaseWithDictionaries::attachDictionary(const String & dictionary_name, const Context & context)
{
    String full_name = getDatabaseName() + "." + dictionary_name;
    {
        std::lock_guard lock(mutex);
        if (!dictionaries.emplace(dictionary_name).second)
            throw Exception("Dictionary " + full_name + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);
    }

    /// ExternalLoader::reloadConfig() will find out that the dictionary's config has been added
    /// and in case `dictionaries_lazy_load == false` it will load the dictionary.
    const auto & external_loader = context.getExternalDictionariesLoader();
    external_loader.reloadConfig(getDatabaseName(), full_name);
}

void DatabaseWithDictionaries::detachDictionary(const String & dictionary_name, const Context & context)
{
    String full_name = getDatabaseName() + "." + dictionary_name;
    {
        std::lock_guard lock(mutex);
        auto it = dictionaries.find(dictionary_name);
        if (it == dictionaries.end())
            throw Exception("Dictionary " + full_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        dictionaries.erase(it);
    }

    /// ExternalLoader::reloadConfig() will find out that the dictionary's config has been removed
    /// and therefore it will unload the dictionary.
    const auto & external_loader = context.getExternalDictionariesLoader();
    external_loader.reloadConfig(getDatabaseName(), full_name);

}

StoragePtr DatabaseWithDictionaries::tryGetTable(const Context & context, const String & table_name) const
{
    if (auto table_ptr = DatabaseWithOwnTablesBase::tryGetTable(context, table_name))
        return table_ptr;

    if (isDictionaryExist(context, table_name))
        /// We don't need lock database here, because database doesn't store dictionary itself
        /// just metadata
        return getDictionaryStorage(context, table_name);

    return {};
}

DatabaseTablesIteratorPtr DatabaseWithDictionaries::getTablesWithDictionaryTablesIterator(const Context & context, const FilterByNameFunction & filter_by_name)
{
    /// NOTE: it's not atomic
    auto tables_it = getTablesIterator(context, filter_by_name);
    auto dictionaries_it = getDictionariesIterator(context, filter_by_name);

    Tables result;
    while (tables_it && tables_it->isValid())
    {
        result.emplace(tables_it->name(), tables_it->table());
        tables_it->next();
    }

    while (dictionaries_it && dictionaries_it->isValid())
    {
        auto table_name = dictionaries_it->name();
        auto table_ptr = getDictionaryStorage(context, table_name);
        if (table_ptr)
            result.emplace(table_name, table_ptr);
        dictionaries_it->next();
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(result);
}

DatabaseDictionariesIteratorPtr DatabaseWithDictionaries::getDictionariesIterator(const Context & /*context*/, const FilterByNameFunction & filter_by_dictionary_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_dictionary_name)
        return std::make_unique<DatabaseDictionariesSnapshotIterator>(dictionaries);

    Dictionaries filtered_dictionaries;
    for (const auto & dictionary_name : dictionaries)
        if (filter_by_dictionary_name(dictionary_name))
            filtered_dictionaries.emplace(dictionary_name);
    return std::make_unique<DatabaseDictionariesSnapshotIterator>(std::move(filtered_dictionaries));
}

bool DatabaseWithDictionaries::isDictionaryExist(const Context & /*context*/, const String & dictionary_name) const
{
    std::lock_guard lock(mutex);
    return dictionaries.find(dictionary_name) != dictionaries.end();
}

StoragePtr DatabaseWithDictionaries::getDictionaryStorage(const Context & context, const String & table_name) const
{
    auto dict_name = database_name + "." + table_name;
    const auto & external_loader = context.getExternalDictionariesLoader();
    auto dict_ptr = external_loader.tryGetDictionary(dict_name);
    if (dict_ptr)
    {
        const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
        auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
        return StorageDictionary::create(database_name, table_name, ColumnsDescription{columns}, context, true, dict_name);
    }
    return nullptr;
}

}
