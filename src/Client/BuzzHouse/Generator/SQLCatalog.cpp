#include <Client/BuzzHouse/Generator/SQLCatalog.h>

namespace BuzzHouse
{
void SQLDatabase::finishDatabaseSpecification(DatabaseEngine * dspec) const
{
    if (isReplicatedDatabase())
    {
        dspec->add_params()->set_svalue("/test/db" + std::to_string(zoo_path_counter));
        dspec->add_params()->set_svalue("s1");
        dspec->add_params()->set_svalue("r1");
    }
}

bool SQLBase::isNotTruncableEngine() const
{
    return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
        || isMongoDBEngine() || isAnyS3Engine() || isAnyAzureEngine() || isHudiEngine() || isAnyDeltaLakeEngine() || isAnyIcebergEngine()
        || isMergeEngine() || isDistributedEngine() || isDictionaryEngine() || isGenerateRandomEngine() || isMaterializedPostgreSQLEngine()
        || isExternalDistributedEngine();
}

bool SQLBase::isEngineReplaceable() const
{
    return isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine()
        || isAnyAzureEngine() || isFileEngine() || isURLEngine() || isRedisEngine() || isMongoDBEngine() || isDictionaryEngine();
}

bool SQLBase::isAnotherRelationalDatabaseEngine() const
{
    return isMySQLEngine() || isPostgreSQLEngine() || isMaterializedPostgreSQLEngine() || isSQLiteEngine() || isExternalDistributedEngine();
}

bool SQLBase::hasDatabasePeer() const
{
    chassert(is_deterministic || peer_table == PeerTableDatabase::None);
    return peer_table != PeerTableDatabase::None;
}

bool SQLBase::isAttached() const
{
    return (!db || db->isAttached()) && attached == DetachStatus::ATTACHED;
}

bool SQLBase::isDettached() const
{
    return (db && db->attached != DetachStatus::ATTACHED) || attached != DetachStatus::ATTACHED;
}

String SQLBase::getTablePath(const FuzzConfig & fc, const bool client) const
{
    if (isIcebergS3Engine() || isDeltaLakeS3Engine() || isAnyS3Engine())
    {
        const Catalog * cat = nullptr;
        const ServerCredentials & sc = fc.minio_server.value();

        switch (catalog)
        {
            case CatalogTable::Glue:
                cat = &sc.glue_catalog.value();
                break;
            case CatalogTable::Hive:
                cat = &sc.hive_catalog.value();
                break;
            case CatalogTable::REST:
                cat = &sc.rest_catalog.value();
                break;
            default:
                break;
        }
        if (cat)
        {
            return fmt::format("http://{}:{}/{}/cat{}/", client ? sc.client_hostname : sc.server_hostname, sc.port, cat->endpoint, tname);
        }
        return fmt::format(
            "http://{}:{}{}/file{}{}",
            client ? sc.client_hostname : sc.server_hostname,
            sc.port,
            sc.database,
            tname,
            isS3Engine() ? "" : "/");
    }
    if (isIcebergAzureEngine() || isDeltaLakeAzureEngine() || isAnyAzureEngine())
    {
        return fmt::format("file{}{}", tname, isAzureEngine() ? "" : "/");
    }
    if (isIcebergLocalEngine() || isDeltaLakeLocalEngine() || isFileEngine())
    {
        const std::filesystem::path & fpath = client ? fc.client_file_path : fc.server_file_path;

        return fmt::format("{}/data{}{}{}", fpath.generic_string(), isFileEngine() ? "file" : "lakefile", tname, isFileEngine() ? "" : "/");
    }
    if (isURLEngine())
    {
        const ServerCredentials & sc = fc.http_server.value();

        return fmt::format("http://{}:{}/file{}", client ? sc.client_hostname : sc.server_hostname, sc.port, tname);
    }
    if (isKeeperMapEngine())
    {
        return fmt::format("/kfile{}", tname);
    }
    chassert(0);
    return "";
}

String SQLBase::getMetadataPath(const FuzzConfig & fc, const bool client) const
{
    const std::filesystem::path & fpath = client ? fc.client_file_path : fc.server_file_path;

    return has_metadata ? fmt::format("{}/metadatat{}", fpath.generic_string(), tname) : "";
}

size_t SQLTable::numberOfInsertableColumns() const
{
    size_t res = 0;

    for (const auto & entry : cols)
    {
        res += entry.second.canBeInserted() ? 1 : 0;
    }
    return res;
}

String SQLTable::getTableName() const
{
    return "t" + std::to_string(tname);
}

String SQLTable::getFullName(const bool setdbname) const
{
    String res;

    if (db || setdbname)
    {
        res += "d" + (db ? std::to_string(db->dname) : "efault") + ".";
    }
    res += "t" + std::to_string(tname);
    return res;
}

String ColumnPathChain::columnPathRef() const
{
    String res = "`";

    for (size_t i = 0; i < path.size(); i++)
    {
        if (i != 0)
        {
            res += ".";
        }
        res += path[i].cname;
    }
    res += "`";
    return res;
}
}
