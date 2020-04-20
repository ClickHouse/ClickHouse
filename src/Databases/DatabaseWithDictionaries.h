#include <Databases/DatabaseOnDisk.h>
#include <ext/scope_guard.h>

namespace DB
{


class DatabaseWithDictionaries : public DatabaseOnDisk
{
public:
    void attachDictionary(const String & dictionary_name, const DictionaryAttachInfo & attach_info) override;

    void detachDictionary(const String & dictionary_name) override;

    void createDictionary(const Context & context,
                          const String & dictionary_name,
                          const ASTPtr & query) override;

    void removeDictionary(const Context & context, const String & dictionary_name) override;

    bool isDictionaryExist(const Context & context, const String & dictionary_name) const override;

    DatabaseDictionariesIteratorPtr getDictionariesIterator(const FilterByNameFunction & filter_by_dictionary_name) override;

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> getDictionaryConfiguration(const String & /*name*/) const override;

    time_t getObjectMetadataModificationTime(const String & object_name) const override;

    bool empty(const Context & context) const override;

    void shutdown() override;

    ~DatabaseWithDictionaries() override;

protected:
    DatabaseWithDictionaries(const String & name, const String & metadata_path_, const String & data_path_, const String & logger, const Context & context)
        : DatabaseOnDisk(name, metadata_path_, data_path_, logger, context) {}

    void attachToExternalDictionariesLoader(Context & context);
    void detachFromExternalDictionariesLoader();

    void detachDictionaryImpl(const String & dictionary_name, DictionaryAttachInfo & attach_info);

    ASTPtr getCreateDictionaryQueryImpl(const Context & context,
                                        const String & dictionary_name,
                                        bool throw_on_error) const override;

    std::unordered_map<String, DictionaryAttachInfo> dictionaries;

private:
    ExternalDictionariesLoader * external_loader = nullptr;
    ext::scope_guard database_as_config_repo_for_external_loader;
};

}
