#pragma once

#include <Databases/IDatabase.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{

class Context;

/**
  * DatabaseURL allows to query data located at an arbitrary URL as if it were a table,
  * treating the table name as a URL. The name is resolved against the optional base URL
  * given as an engine argument, per RFC 3986 (see `StorageURL::resolveURLBase`).
  * When no base URL is configured, only names that are themselves URLs with a scheme
  * (e.g. `https://example.com/data.csv`) are recognized as tables of this database.
  *
  * The work is delegated to the `url` table function, which dispatches by the URL scheme
  * to the matching engine (`file://` -> File, `s3://` -> S3, `az://` -> AzureBlobStorage,
  * `hdfs://` -> HDFS, `http://`, `https://` and the rest -> URL), so files, web and object
  * storage URLs are handled uniformly.
  *
  * Used in clickhouse-local (inside the Overlay database) to allow queries like
  * `SELECT * FROM 'https://example.com/data.csv'`.
  */
class DatabaseURL final : public IDatabase, protected WithContext
{
public:
    DatabaseURL(const String & name_, const String & base_url_, ContextPtr context_);

    String getEngineName() const override { return "URL"; }

    bool isTableExist(const String & name, ContextPtr context) const override;

    StoragePtr getTable(const String & name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override { return true; }

    bool isReadOnly() const override { return true; }

    void shutdown() override {}

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const override;

protected:
    ASTPtr getCreateDatabaseQueryImpl() const override TSA_REQUIRES(mutex);

private:
    /// Resolve a table name to a URL against the base URL.
    /// Returns an empty string when the name does not denote a URL
    /// (it has no scheme, and there is no base URL to resolve it against).
    String getTableURL(const String & name) const;

    /// For `file://` URLs the existence of the file can be checked cheaply, so a name is only
    /// claimed as a table of this database when the file exists (like in DatabaseFilesystem).
    /// For remote schemes the check would require a network request, so any name that resolves
    /// to a URL is claimed, and access errors propagate from the delegate engine.
    bool checkFileURLExists(const String & url, ContextPtr context, bool throw_on_error) const;

    StoragePtr getTableImpl(const String & name, ContextPtr context, bool throw_on_error) const;

    String base_url;
};

}
