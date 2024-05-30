#include <Common/NamedCollections/NamedCollections.h>
#include <Common/logger_useful.h>

namespace DB
{
class ASTCreateNamedCollectionQuery;
class ASTDropNamedCollectionQuery;
class ASTAlterNamedCollectionQuery;

class NamedCollectionFactory : boost::noncopyable
{
public:
    static NamedCollectionFactory & instance();

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(const std::string & collection_name) const;

    NamedCollectionPtr tryGet(const std::string & collection_name) const;

    MutableNamedCollectionPtr getMutable(const std::string & collection_name) const;

    void add(const std::string & collection_name, MutableNamedCollectionPtr collection);

    void add(NamedCollectionsMap collections);

    void update(NamedCollectionsMap collections);

    void remove(const std::string & collection_name);

    void removeIfExists(const std::string & collection_name);

    void removeById(NamedCollection::SourceId id);

    NamedCollectionsMap getAll() const;

    void loadIfNot();

    void reloadFromConfig(const Poco::Util::AbstractConfiguration & config);

    void createFromSQL(const ASTCreateNamedCollectionQuery & query, ContextPtr context);

    void removeFromSQL(const ASTDropNamedCollectionQuery & query, ContextPtr context);

    void updateFromSQL(const ASTAlterNamedCollectionQuery & query, ContextPtr context);

    /// This method is public only for unit tests.
    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);

private:
    bool existsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    MutableNamedCollectionPtr tryGetUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    void addUnlocked(
        const std::string & collection_name,
        MutableNamedCollectionPtr collection,
        std::lock_guard<std::mutex> & lock);

    bool removeIfExistsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock);

    mutable NamedCollectionsMap loaded_named_collections;

    LoggerPtr log = getLogger("NamedCollectionFactory");
    mutable std::mutex mutex;
    bool is_initialized = false;
    bool loaded = false;

    void loadFromSQL(const ContextPtr & context);
};

}
