#include <string>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>

namespace BuzzHouse
{
void SQLDatabase::finishDatabaseSpecification(DatabaseEngine * de) const
{
    if (isReplicatedDatabase())
    {
        chassert(de->params_size() == 0);
        de->add_params()->set_svalue("/clickhouse/path/" + this->getName());
        de->add_params()->set_svalue("{shard}");
        de->add_params()->set_svalue("{replica}");
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
        || isAnyAzureEngine() || isFileEngine() || isURLEngine() || isRedisEngine() || isMongoDBEngine() || isDictionaryEngine()
        || isNullEngine() || isGenerateRandomEngine() || isArrowFlightEngine();
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

String SQLBase::getDatabaseName() const
{
    return "d" + (db ? std::to_string(db->dname) : "efault");
}

static const constexpr String PARTITION_STR = "{_partition_id}";

void SQLBase::setTablePath(RandomGenerator & rg, const FuzzConfig & fc)
{
    chassert(!bucket_path.has_value() && catalog == CatalogTable::None);
    if (isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine() || isAnyAzureEngine())
    {
        /// Set catalog first if possible
        const bool canUseCatalog = fc.minio_server.has_value() && (isDeltaLakeS3Engine() || isIcebergS3Engine());
        const uint32_t glue_cat = 5 * static_cast<uint32_t>(canUseCatalog && fc.minio_server.value().glue_catalog.has_value());
        const uint32_t hive_cat = 5 * static_cast<uint32_t>(canUseCatalog && fc.minio_server.value().hive_catalog.has_value());
        const uint32_t rest_cat = 5 * static_cast<uint32_t>(canUseCatalog && fc.minio_server.value().rest_catalog.has_value());
        const uint32_t no_cat = 15;
        const uint32_t prob_space = glue_cat + hive_cat + rest_cat + no_cat;
        std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
        const uint32_t nopt = next_dist(rg.generator);
        String next_bucket_path;

        if (glue_cat && (nopt < glue_cat + 1))
        {
            catalog = CatalogTable::Glue;
        }
        else if (hive_cat && (nopt < glue_cat + hive_cat + 1))
        {
            catalog = CatalogTable::Hive;
        }
        else if (rest_cat && (nopt < glue_cat + hive_cat + rest_cat + 1))
        {
            catalog = CatalogTable::REST;
        }

        if (isS3QueueEngine() || isAzureQueueEngine())
        {
            next_bucket_path = fmt::format("{}queue{}/", rg.nextBool() ? "subdir/" : "", tname);
        }
        else if (isAnyIcebergEngine() || isAnyDeltaLakeEngine() || catalog != CatalogTable::None)
        {
            const Catalog * cat = nullptr;

            switch (catalog)
            {
                case CatalogTable::Glue:
                    cat = &fc.minio_server.value().glue_catalog.value();
                    break;
                case CatalogTable::Hive:
                    cat = &fc.minio_server.value().hive_catalog.value();
                    break;
                case CatalogTable::REST:
                    cat = &fc.minio_server.value().rest_catalog.value();
                    break;
                default:
                    break;
            }
            next_bucket_path = fmt::format("{}{}t{}/", cat ? cat->endpoint : "", cat ? "/" : "", tname);
        }
        else
        {
            /// S3 and Azure engines point to files
            bool used_partition = false;
            const bool add_before = rg.nextBool();

            chassert(isS3Engine() || isAzureEngine());
            if (rg.nextBool())
            {
                /// Use a subdirectory
                next_bucket_path += "subdir";
                next_bucket_path += rg.nextBool() ? std::to_string(tname) : "";
                if (has_partition_by && rg.nextBool())
                {
                    next_bucket_path += PARTITION_STR;
                    used_partition = true;
                }
                next_bucket_path += "/";
            }
            next_bucket_path += "file";
            next_bucket_path += add_before ? std::to_string(tname) : "";
            if (has_partition_by && !used_partition && rg.nextBool())
            {
                next_bucket_path += PARTITION_STR;
            }
            next_bucket_path += !add_before ? std::to_string(tname) : "";
            if (rg.nextBool())
            {
                next_bucket_path += ".data";
            }
        }
        bucket_path = next_bucket_path;
    }
    else
    {
        chassert(0);
    }
}

String SQLBase::getTablePath(RandomGenerator & rg, const FuzzConfig & fc, const bool no_change) const
{
    if (isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine() || isAnyAzureEngine())
    {
        String res = bucket_path.value();

        if (!isS3QueueEngine() && !isAzureQueueEngine() && catalog == CatalogTable::None && !no_change && rg.nextSmallNumber() < 8)
        {
            /// Replace PARTITION BY str
            const size_t partition_pos = res.find(PARTITION_STR);
            if (partition_pos != std::string::npos && rg.nextMediumNumber() < 81)
            {
                res.replace(
                    partition_pos,
                    PARTITION_STR.length(),
                    rg.nextBool() ? std::to_string(rg.randomInt<uint32_t>(0, 100)) : rg.nextString("", true, rg.nextStrlen()));
            }
            /// Use globs
            const size_t slash_pos = res.rfind('/');
            if (slash_pos != std::string::npos && rg.nextMediumNumber() < 81)
            {
                res.replace(slash_pos + 1, std::string::npos, rg.nextBool() ? "*" : "**");
            }
        }
        return res;
    }
    if (isFileEngine())
    {
        return fmt::format("{}/file{}", fc.server_file_path.generic_string(), tname);
    }
    if (isURLEngine())
    {
        const ServerCredentials & sc = fc.http_server.value();

        return fmt::format("http://{}:{}/file{}", sc.server_hostname, sc.port, tname);
    }
    if (isKeeperMapEngine())
    {
        return fmt::format("/kfile{}", tname);
    }
    if (isArrowFlightEngine())
    {
        return fmt::format("/aflight{}", tname);
    }
    chassert(0);
    return "";
}

String SQLBase::getMetadataPath(const FuzzConfig & fc) const
{
    return has_metadata ? fmt::format("{}/metadatat{}", fc.server_file_path.generic_string(), tname) : "";
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
        res += getDatabaseName() + ".";
    }
    res += getTableName();
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
