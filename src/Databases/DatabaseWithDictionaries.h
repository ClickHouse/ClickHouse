#include <Databases/DatabaseOnDisk.h>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <ext/scope_guard.h>

namespace DB
{

class Context;
class ExternalDictionariesLoader;


class DatabaseWithDictionaries : public DatabaseOnDisk
{
public:
    void attachDictionary(const String & dictionary_name, const DictionaryAttachInfo & attach_info) override;

    void detachDictionary(const String & dictionary_name) override;

    void createDictionary(const Context & context,
                          const String & dictionary_name,
                          const ASTPtr & query) override;

    void removeDictionary(const Context & context, const String & dictionary_name) override;

    bool isDictionaryExist(const String & dictionary_name) const override;

    DatabaseDictionariesIteratorPtr getDictionariesIterator(const FilterByNameFunction & filter_by_dictionary_name) override;

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> getDictionaryConfiguration(const String & /*name*/) const override;

    time_t getObjectMetadataModificationTime(const String & object_name) const override;

    bool empty() const override;

    void shutdown() override;

    ~DatabaseWithDictionaries() override;

protected:
    DatabaseWithDictionaries(const String & name, const String & metadata_path_, const String & data_path_, const String & logger, const Context & context);

    ASTPtr getCreateDictionaryQueryImpl(const String & dictionary_name, bool throw_on_error) const override;

    std::unordered_map<String, DictionaryAttachInfo> dictionaries;

private:
    void detachDictionaryImpl(const String & dictionary_name, DictionaryAttachInfo & attach_info);
    void reloadDictionaryConfig(const String & full_name);

    const ExternalDictionariesLoader & external_loader;
    boost::atomic_shared_ptr<ext::scope_guard> database_as_config_repo_for_external_loader;
};

}
