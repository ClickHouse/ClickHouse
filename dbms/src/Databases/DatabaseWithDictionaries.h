#include <Databases/DatabasesCommon.h>

namespace DB
{


class DatabaseWithDictionaries : public DatabaseWithOwnTablesBase
{
public:
    void attachDictionary(const String & name, const Context & context) override;

    void detachDictionary(const String & name, const Context & context) override;

    StoragePtr tryGetTable(const Context & context, const String & table_name) const override;

    DatabaseTablesIteratorPtr getTablesWithDictionaryTablesIterator(const Context & context, const FilterByNameFunction & filter_by_dictionary_name = {}) override;

    DatabaseDictionariesIteratorPtr getDictionariesIterator(const Context & context, const FilterByNameFunction & filter_by_dictionary_name = {}) override;

    bool isDictionaryExist(const Context & context, const String & dictionary_name) const override;

protected:
    DatabaseWithDictionaries(String name) : DatabaseWithOwnTablesBase(std::move(name)) {}

    StoragePtr getDictionaryStorage(const Context & context, const String & table_name) const;

};

}
