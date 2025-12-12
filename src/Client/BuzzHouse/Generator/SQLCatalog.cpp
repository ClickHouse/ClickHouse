#include <Client/BuzzHouse/Generator/SQLCatalog.h>

namespace BuzzHouse
{

String SQLColumn::getColumnName() const
{
    return "c" + std::to_string(cname);
}

void SQLDatabase::finishDatabaseSpecification(DatabaseEngine * de)
{
    if (isReplicatedDatabase())
    {
        de->add_params()->set_svalue(this->keeper_path);
        de->add_params()->set_svalue(this->shard_name);
        de->add_params()->set_svalue(this->replica_name);
    }
}

void SQLDatabase::setDatabasePath(RandomGenerator & rg, const FuzzConfig & fc)
{
    if (isDataLakeCatalogDatabase())
    {
        const uint32_t glue_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().glue_catalog.has_value());
        const uint32_t hive_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().hive_catalog.has_value());
        const uint32_t rest_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().rest_catalog.has_value());
        const uint32_t unit_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().unity_catalog.has_value());
        const uint32_t prob_space = glue_cat + hive_cat + rest_cat + unit_cat;
        chassert(prob_space > 0);
        std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
        const uint32_t nopt = next_dist(rg.generator);

        if (glue_cat && (nopt < glue_cat + 1))
        {
            catalog = LakeCatalog::Glue;
        }
        else if (hive_cat && (nopt < glue_cat + hive_cat + 1))
        {
            catalog = LakeCatalog::Hive;
        }
        else if (rest_cat && (nopt < glue_cat + hive_cat + rest_cat + 1))
        {
            catalog = LakeCatalog::REST;
        }
        else if (unit_cat && (nopt < glue_cat + hive_cat + rest_cat + unit_cat + 1))
        {
            catalog = LakeCatalog::Unity;
        }

        integration = IntegrationCall::Dolor; /// Has to use La Casa Del Dolor
        format = (catalog == LakeCatalog::REST || catalog == LakeCatalog::Hive || catalog == LakeCatalog::Glue) ? LakeFormat::Iceberg
                                                                                                                : LakeFormat::DeltaLake;
        storage = LakeStorage::S3; /// What ClickHouse supports now
    }
}

String SQLDatabase::getSparkCatalogName() const
{
    chassert(isDataLakeCatalogDatabase());
    /// DeltaLake tables on Spark must be on the `spark_catalog` :(
    return (catalog == LakeCatalog::None && format == LakeFormat::DeltaLake) ? "spark_catalog" : getName();
}

bool SQLBase::isNotTruncableEngine() const
{
    return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
        || isMongoDBEngine() || isHudiEngine() || isMergeEngine() || isDistributedEngine() || isDictionaryEngine()
        || isGenerateRandomEngine() || isMaterializedPostgreSQLEngine() || isExternalDistributedEngine();
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

String SQLBase::getTableName(const bool full) const
{
    String res;

    if (full && getLakeCatalog() != LakeCatalog::None)
    {
        res += "test.";
    }
    res += this->prefix + std::to_string(tname);
    return res;
}

String SQLBase::getFullName(const bool setdbname) const
{
    String res;

    if (db || setdbname)
    {
        res += getDatabaseName() + ".";
    }
    res += getTableName();
    return res;
}

String SQLBase::getSparkCatalogName() const
{
    chassert(isAnyIcebergEngine() || isAnyDeltaLakeEngine());
    if (getLakeCatalog() == LakeCatalog::None)
    {
        /// DeltaLake tables on Spark must be on the `spark_catalog` :(
        return isAnyIcebergEngine() ? getTableName(false) : "spark_catalog";
    }
    return db->getSparkCatalogName();
}

static const constexpr String PARTITION_STR = "{_partition_id}";

void SQLBase::setTablePath(RandomGenerator & rg, const FuzzConfig & fc, const bool has_dolor)
{
    chassert(
        !bucket_path.has_value() && !file_format.has_value() && !file_comp.has_value() && !partition_strategy.has_value()
        && !partition_columns_in_data_file.has_value() && !storage_class_name.has_value());
    has_partition_by = (isRedisEngine() || isKeeperMapEngine() || isMaterializedPostgreSQLEngine() || isAnyIcebergEngine()
                        || isAzureEngine() || isS3Engine())
        && rg.nextSmallNumber() < 4;
    has_order_by = isAnyIcebergEngine() && rg.nextSmallNumber() < 4;
    if (isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine() || isAnyAzureEngine())
    {
        /// Set bucket path first if possible
        String next_bucket_path;

        /// Set integration call to use, sometimes create tables in ClickHouse, others also in Spark
        if (has_dolor && (isAnyIcebergEngine() || isAnyDeltaLakeEngine()) && rg.nextBool())
        {
            integration = IntegrationCall::Dolor;
        }
        else if (isOnS3())
        {
            integration = IntegrationCall::MinIO;
        }
        else if (isOnAzure())
        {
            integration = IntegrationCall::Azurite;
        }

        if (isAnyIcebergEngine() || isAnyDeltaLakeEngine())
        {
            const LakeCatalog catalog = getLakeCatalog();

            if (catalog == LakeCatalog::None)
            {
                /// DeltaLake tables on Spark must be on the `spark_catalog` :(
                next_bucket_path = fmt::format(
                    "{}{}{}{}t{}",
                    isOnLocal() ? fc.lakes_path.generic_string() : "",
                    isOnLocal() ? "/" : "",
                    (integration == IntegrationCall::Dolor) ? getSparkCatalogName() : "",
                    (integration == IntegrationCall::Dolor) ? "/test/" : "",
                    tname);
            }
            else
            {
                const Catalog * cat = nullptr;
                const ServerCredentials & sc = fc.dolor_server.value();

                chassert(isOnS3()); /// What is supported at the moment
                switch (catalog)
                {
                    case LakeCatalog::Glue:
                        cat = &sc.glue_catalog.value();
                        break;
                    case LakeCatalog::Hive:
                        cat = &sc.hive_catalog.value();
                        break;
                    case LakeCatalog::REST:
                        cat = &sc.rest_catalog.value();
                        break;
                    case LakeCatalog::Unity:
                        cat = &sc.unity_catalog.value();
                        break;
                    default:
                        UNREACHABLE();
                }
                next_bucket_path = fmt::format(
                    "http://{}:{}/{}/t{}/", fc.minio_server.value().server_hostname, fc.minio_server.value().port, cat->warehouse, tname);
            }
        }
        else if (isS3QueueEngine() || isAzureQueueEngine())
        {
            next_bucket_path = fmt::format("{}queue{}/", rg.nextBool() ? "subdir/" : "", tname);
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
    if (isAnyIcebergEngine() && rg.nextMediumNumber() < 91)
    {
        /// Iceberg supports 3 formats
        static const std::vector<InOutFormat> & formats = {InOutFormat::INOUT_ORC, InOutFormat::INOUT_Avro, InOutFormat::INOUT_Parquet};

        file_format = rg.pickRandomly(formats);
    }
    else if (isAnyDeltaLakeEngine() && rg.nextMediumNumber() < 91)
    {
        /// What Delta Lake supports
        file_format = INOUT_Parquet;
    }
    else if (isAnyS3Engine() || isAnyAzureEngine() || isFileEngine() || isURLEngine())
    {
        /// Set other parameters
        if (isFileEngine() || rg.nextMediumNumber() < 91)
        {
            std::uniform_int_distribution<uint32_t> inout_range(1, static_cast<uint32_t>(InOutFormat_MAX));

            file_format = static_cast<InOutFormat>(inout_range(rg.generator));
        }
        if (rg.nextMediumNumber() < 51)
        {
            file_comp = rg.pickRandomly(compressionMethods);
        }
    }
    if ((isS3Engine() || isAzureEngine()) && rg.nextMediumNumber() < 21)
    {
        partition_strategy = rg.nextBool() ? "wildcard" : "hive";
    }
    if ((isS3Engine() || isAzureEngine()) && rg.nextMediumNumber() < 21)
    {
        partition_columns_in_data_file = rg.nextBool() ? "1" : "0";
    }
    if (isS3Engine() && rg.nextMediumNumber() < 21)
    {
        storage_class_name = rg.nextBool() ? "STANDARD" : "INTELLIGENT_TIERING";
    }
    if (isExternalDistributedEngine())
    {
        integration = (sub == PostgreSQL) ? IntegrationCall::PostgreSQL : IntegrationCall::MySQL;
    }
    else if (isMySQLEngine())
    {
        integration = IntegrationCall::MySQL;
    }
    else if (isPostgreSQLEngine() || isMaterializedPostgreSQLEngine())
    {
        integration = IntegrationCall::PostgreSQL;
    }
    else if (isSQLiteEngine())
    {
        integration = IntegrationCall::SQLite;
    }
    else if (isMongoDBEngine())
    {
        integration = IntegrationCall::MongoDB;
    }
    else if (isRedisEngine())
    {
        integration = IntegrationCall::Redis;
    }
    else if (isURLEngine())
    {
        integration = IntegrationCall::HTTP;
    }
}

String SQLBase::getTablePath(const FuzzConfig & fc) const
{
    if (isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine() || isAnyAzureEngine())
    {
        return bucket_path.value();
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

    UNREACHABLE();
}

String SQLBase::getTablePath(RandomGenerator & rg, const FuzzConfig & fc, const bool allow_not_deterministic) const
{
    if ((isS3Engine() || isAzureEngine()) && allow_not_deterministic && rg.nextSmallNumber() < 8)
    {
        String res = bucket_path.value();
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
        return res;
    }
    return getTablePath(fc);
}

String SQLBase::getMetadataPath(const FuzzConfig & fc) const
{
    return has_metadata ? fmt::format("{}/metadatat{}", fc.server_file_path.generic_string(), tname) : "";
}

size_t SQLTable::numberOfInsertableColumns(const bool all) const
{
    size_t res = 0;

    for (const auto & entry : cols)
    {
        res += entry.second.canBeInserted() || all ? 1 : 0;
    }
    return res;
}

String ColumnPathChain::columnPathRef(const String & quote) const
{
    String res = quote;

    for (size_t i = 0; i < path.size(); i++)
    {
        if (i != 0)
        {
            res += ".";
        }
        res += path[i].cname;
    }
    res += quote;
    return res;
}

}
