#include <Databases/DatabaseOnDisk.h>
#include <ext/scope_guard.h>

namespace DB
{


class DatabaseWithDictionaries : public DatabaseOnDisk
{
public:
    void attachDictionary(const String & name, const Context & context) override;

    void detachDictionary(const String & name, const Context & context) override;

    void createDictionary(const Context & context,
                          const String & dictionary_name,
                          const ASTPtr & query) override;

    void removeDictionary(const Context & context, const String & dictionary_name) override;

    StoragePtr tryGetTable(const Context & context, const String & table_name) const override;

    DatabaseTablesIteratorPtr getTablesWithDictionaryTablesIterator(const Context & context, const FilterByNameFunction & filter_by_dictionary_name = {}) override;

    DatabaseDictionariesIteratorPtr getDictionariesIterator(const Context & context, const FilterByNameFunction & filter_by_dictionary_name = {}) override;

    bool isDictionaryExist(const Context & context, const String & dictionary_name) const override;

    void shutdown() override;

    ~DatabaseWithDictionaries() override;

protected:
    DatabaseWithDictionaries(const String & name, const String & metadata_path_, const String & logger)
        : DatabaseOnDisk(name, metadata_path_, logger) {}

    void attachToExternalDictionariesLoader(Context & context);
    void detachFromExternalDictionariesLoader();

    StoragePtr getDictionaryStorage(const Context & context, const String & table_name) const;

    ASTPtr getCreateDictionaryQueryImpl(const Context & context,
                                        const String & dictionary_name,
                                        bool throw_on_error) const override;

private:
    ext::scope_guard database_as_config_repo_for_external_loader;
};

}
