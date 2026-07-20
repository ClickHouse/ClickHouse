#include <Client/BuzzHouse/Generator/SQLCatalog.h>

namespace BuzzHouse
{

bool SQLColumn::canBeInserted() const
{
    return !dmod.has_value() || dmod.value() == DModifier::DEF_DEFAULT;
}

String SQLColumn::getColumnName() const
{
    return cname;
}

void SQLDatabase::setRandomDatabase(RandomGenerator & rg, SQLDatabase & d)
{
    d.random_engine = rg.nextMediumNumber() < 4;
}

void SQLDatabase::setName(SQLIdentifier * db, const String & n)
{
    db->set_value(n);
}

bool SQLDatabase::isAtomicDatabase() const
{
    return deng == DatabaseEngineValues::DAtomic;
}

bool SQLDatabase::isMemoryDatabase() const
{
    return deng == DatabaseEngineValues::DMemory;
}

bool SQLDatabase::isReplicatedDatabase() const
{
    return deng == DatabaseEngineValues::DReplicated;
}

bool SQLDatabase::isSharedDatabase() const
{
    return deng == DatabaseEngineValues::DShared;
}

bool SQLDatabase::isBackupDatabase() const
{
    return deng == DatabaseEngineValues::DBackup;
}

bool SQLDatabase::isOrdinaryDatabase() const
{
    return deng == DatabaseEngineValues::DOrdinary;
}

bool SQLDatabase::isDataLakeCatalogDatabase() const
{
    return deng == DatabaseEngineValues::DDataLakeCatalog;
}

bool SQLDatabase::isReplicatedOrSharedDatabase() const
{
    return isReplicatedDatabase() || isSharedDatabase();
}

bool SQLDatabase::isAttached() const
{
    return attached == DetachStatus::ATTACHED;
}

bool SQLDatabase::isDettached() const
{
    return attached != DetachStatus::ATTACHED;
}

void SQLDatabase::setName(SQLIdentifier * db) const
{
    db->set_value(getName());
}

String SQLDatabase::getName() const
{
    return name;
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
    if (!random_engine && isDataLakeCatalogDatabase() && fc.dolor_server.has_value())
    {
        const uint32_t glue_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().glue_catalog.has_value());
        const uint32_t hive_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().hive_catalog.has_value());
        const uint32_t rest_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().rest_catalog.has_value());
        const uint32_t unit_cat = 5 * static_cast<uint32_t>(fc.dolor_server.value().unity_catalog.has_value());

        chassert(glue_cat + hive_cat + rest_cat + unit_cat > 0);
        rg.pickWeighted(
            {{glue_cat, [&] { catalog = LakeCatalog::Glue; }},
             {hive_cat, [&] { catalog = LakeCatalog::Hive; }},
             {rest_cat, [&] { catalog = LakeCatalog::REST; }},
             {unit_cat, [&] { catalog = LakeCatalog::Unity; }}});

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

bool TableEngineDescriptor::isMergeTreeFamily() const
{
    return value >= TableEngineValues::MergeTree && value <= TableEngineValues::GraphiteMergeTree;
}

bool TableEngineDescriptor::isLogFamily() const
{
    return value >= TableEngineValues::StripeLog && value <= TableEngineValues::TinyLog;
}

bool TableEngineDescriptor::isShared() const
{
    return option.has_value() && option.value() == TableEngineOption::TShared;
}

bool TableEngineDescriptor::isReplicated() const
{
    return option.has_value() && option.value() == TableEngineOption::TReplicated;
}

bool TableEngineDescriptor::supportsFinal() const
{
    return value >= TableEngineValues::ReplacingMergeTree && value <= TableEngineValues::GraphiteMergeTree;
}

bool TableEngineDescriptor::hasSignColumn() const
{
    return value >= TableEngineValues::CollapsingMergeTree && value <= TableEngineValues::VersionedCollapsingMergeTree;
}

bool TableEngineDescriptor::hasVersionColumn() const
{
    return value == TableEngineValues::VersionedCollapsingMergeTree;
}

bool TableEngineDescriptor::areInsertsAppends() const
{
    return value == TableEngineValues::MergeTree || isLogFamily() || value == TableEngineValues::Memory || value == TableEngineValues::MySQL
        || value == TableEngineValues::PostgreSQL || value == TableEngineValues::MaterializedPostgreSQL
        || value == TableEngineValues::SQLite || value == TableEngineValues::MongoDB || value == TableEngineValues::Redis
        || value == TableEngineValues::Hudi || (value >= TableEngineValues::DeltaLakeS3 && value <= TableEngineValues::DeltaLakeLocal)
        || (value >= TableEngineValues::IcebergS3 && value <= TableEngineValues::IcebergLocal)
        || (value >= TableEngineValues::PaimonS3 && value <= TableEngineValues::PaimonLocal) || value == TableEngineValues::Dictionary;
}

void SQLBase::setDeterministic(const FuzzConfig & fc, RandomGenerator & rg, SQLBase & b)
{
    b.engine.is_deterministic = rg.nextMediumNumber() <= fc.deterministic_prob;
    b.random_engine = !b.engine.is_deterministic && rg.nextMediumNumber() < 6;
}

bool SQLBase::isDeterministic() const
{
    return engine.isDeterministic() && (!subengine.has_value() || subengine->isDeterministic());
}

bool SQLBase::isMergeTreeFamily(const bool as_alias) const
{
    return engine.isMergeTreeFamily() || (as_alias && isAliasEngine() && subengine.has_value() && subengine->isMergeTreeFamily());
}

bool SQLBase::isLogFamily(const bool as_alias) const
{
    return engine.isLogFamily() || (as_alias && isAliasEngine() && subengine.has_value() && subengine->isLogFamily());
}

bool SQLBase::isSharedMergeTree(const bool as_alias) const
{
    return (isMergeTreeFamily() && engine.isShared())
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->isMergeTreeFamily() && subengine->isShared());
}

bool SQLBase::isReplicatedMergeTree(const bool as_alias) const
{
    return (isMergeTreeFamily() && engine.isReplicated())
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->isMergeTreeFamily() && subengine->isReplicated());
}

bool SQLBase::isReplicatedOrSharedMergeTree(const bool as_alias) const
{
    return isReplicatedMergeTree(as_alias) || isSharedMergeTree(as_alias);
}

bool SQLBase::isShared(const bool as_alias) const
{
    return engine.isShared() || (as_alias && isAliasEngine() && subengine.has_value() && subengine->isShared());
}

bool SQLBase::isFileEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::File
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::File);
}

bool SQLBase::isJoinEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Join
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Join);
}

bool SQLBase::isNullEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Null
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Null);
}

bool SQLBase::isSetEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Set
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Set);
}

bool SQLBase::isBufferEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Buffer
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Buffer);
}

bool SQLBase::isRocksEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::EmbeddedRocksDB
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::EmbeddedRocksDB);
}

bool SQLBase::isMemoryEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Memory
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Memory);
}

bool SQLBase::isMySQLEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::MySQL
        || (isExternalDistributedEngine() && subengine.has_value() && subengine->value == TableEngineValues::MySQL)
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::MySQL);
}

bool SQLBase::isPostgreSQLEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::PostgreSQL || engine.value == TableEngineValues::MaterializedPostgreSQL
        || (isExternalDistributedEngine() && subengine.has_value()
            && (subengine->value == TableEngineValues::PostgreSQL || subengine->value == TableEngineValues::MaterializedPostgreSQL))
        || (as_alias && isAliasEngine() && subengine.has_value()
            && (subengine->value == TableEngineValues::PostgreSQL || subengine->value == TableEngineValues::MaterializedPostgreSQL));
}

bool SQLBase::isSQLiteEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::SQLite
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::SQLite);
}

bool SQLBase::isMongoDBEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::MongoDB
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::MongoDB);
}

bool SQLBase::isRedisEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Redis
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Redis);
}

bool SQLBase::isS3Engine(const bool as_alias) const
{
    return engine.value == TableEngineValues::S3
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::S3);
}

bool SQLBase::isS3QueueEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::S3Queue
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::S3Queue);
}

bool SQLBase::isAnyS3Engine(const bool as_alias) const
{
    return isS3Engine(as_alias) || isS3QueueEngine(as_alias);
}

bool SQLBase::isAzureEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::AzureBlobStorage
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::AzureBlobStorage);
}

bool SQLBase::isAzureQueueEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::AzureQueue
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::AzureQueue);
}

bool SQLBase::isAnyAzureEngine(const bool as_alias) const
{
    return isAzureEngine(as_alias) || isAzureQueueEngine(as_alias);
}

bool SQLBase::isAnyQueueEngine(const bool as_alias) const
{
    return isS3QueueEngine(as_alias) || isAzureQueueEngine(as_alias);
}

bool SQLBase::isHudiEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Hudi
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Hudi);
}

bool SQLBase::isDeltaLakeS3Engine(const bool as_alias) const
{
    return engine.value == TableEngineValues::DeltaLakeS3
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::DeltaLakeS3);
}

bool SQLBase::isDeltaLakeAzureEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::DeltaLakeAzure
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::DeltaLakeAzure);
}

bool SQLBase::isDeltaLakeLocalEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::DeltaLakeLocal
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::DeltaLakeLocal);
}

bool SQLBase::isAnyDeltaLakeEngine(const bool as_alias) const
{
    return isDeltaLakeS3Engine(as_alias) || isDeltaLakeAzureEngine(as_alias) || isDeltaLakeLocalEngine(as_alias);
}

bool SQLBase::isIcebergS3Engine(const bool as_alias) const
{
    return engine.value == TableEngineValues::IcebergS3
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::IcebergS3);
}

bool SQLBase::isIcebergAzureEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::IcebergAzure
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::IcebergAzure);
}

bool SQLBase::isIcebergLocalEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::IcebergLocal
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::IcebergLocal);
}

bool SQLBase::isAnyIcebergEngine(const bool as_alias) const
{
    return isIcebergS3Engine(as_alias) || isIcebergAzureEngine(as_alias) || isIcebergLocalEngine(as_alias);
}

bool SQLBase::isPaimonS3Engine(const bool as_alias) const
{
    return engine.value == TableEngineValues::PaimonS3
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::PaimonS3);
}

bool SQLBase::isPaimonAzureEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::PaimonAzure
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::PaimonAzure);
}

bool SQLBase::isPaimonLocalEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::PaimonLocal
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::PaimonLocal);
}

bool SQLBase::isAnyPaimonEngine(const bool as_alias) const
{
    return isPaimonS3Engine(as_alias) || isPaimonAzureEngine(as_alias) || isPaimonLocalEngine(as_alias);
}

bool SQLBase::isAnyLakeEngine(const bool as_alias) const
{
    return isAnyIcebergEngine(as_alias) || isAnyDeltaLakeEngine(as_alias) || isAnyPaimonEngine(as_alias);
}

bool SQLBase::isOnS3() const
{
    return isIcebergS3Engine() || isDeltaLakeS3Engine() || isPaimonS3Engine() || isAnyS3Engine();
}

bool SQLBase::isOnAzure() const
{
    return isIcebergAzureEngine() || isDeltaLakeAzureEngine() || isPaimonAzureEngine() || isAnyAzureEngine();
}

bool SQLBase::isOnLocal() const
{
    return isIcebergLocalEngine() || isDeltaLakeLocalEngine() || isPaimonLocalEngine();
}

bool SQLBase::isMergeEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Merge
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Merge);
}

bool SQLBase::isDistributedEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Distributed
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Distributed);
}

bool SQLBase::isDictionaryEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Dictionary
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Dictionary);
}

bool SQLBase::isGenerateRandomEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::GenerateRandom
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::GenerateRandom);
}

bool SQLBase::isURLEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::URL
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::URL);
}

bool SQLBase::isKeeperMapEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::KeeperMap
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::KeeperMap);
}

bool SQLBase::isExternalDistributedEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::ExternalDistributed
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::ExternalDistributed);
}

bool SQLBase::isMaterializedPostgreSQLEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::MaterializedPostgreSQL
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::MaterializedPostgreSQL);
}

bool SQLBase::isArrowFlightEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::ArrowFlight
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::ArrowFlight);
}

bool SQLBase::isAliasEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Alias
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Alias);
}

bool SQLBase::isKafkaEngine(const bool as_alias) const
{
    return engine.value == TableEngineValues::Kafka
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->value == TableEngineValues::Kafka);
}

bool SQLBase::isNotTruncableEngine() const
{
    return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
        || isMongoDBEngine() || isHudiEngine() || isMergeEngine() || isDistributedEngine() || isDictionaryEngine()
        || isGenerateRandomEngine() || isMaterializedPostgreSQLEngine();
}

bool SQLBase::isEngineReplaceable() const
{
    return isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isAnyLakeEngine() || isAnyS3Engine() || isAnyAzureEngine()
        || isFileEngine() || isURLEngine() || isRedisEngine() || isMongoDBEngine() || isDictionaryEngine() || isNullEngine()
        || isGenerateRandomEngine() || isArrowFlightEngine();
}

bool SQLBase::isAnotherRelationalDatabaseEngine() const
{
    return isMySQLEngine() || isPostgreSQLEngine() || isMaterializedPostgreSQLEngine() || isSQLiteEngine();
}

bool SQLBase::hasDatabasePeer() const
{
    chassert(isDeterministic() || peer_table == PeerTableDatabase::None);
    return peer_table != PeerTableDatabase::None;
}

bool SQLBase::hasMySQLPeer() const
{
    return peer_table == PeerTableDatabase::MySQL;
}

bool SQLBase::hasPostgreSQLPeer() const
{
    return peer_table == PeerTableDatabase::PostgreSQL;
}

bool SQLBase::hasSQLitePeer() const
{
    return peer_table == PeerTableDatabase::SQLite;
}

bool SQLBase::hasClickHousePeer() const
{
    return peer_table == PeerTableDatabase::ClickHouse;
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
    return db ? db->getName() : "default";
}

String SQLBase::getBaseName(const bool full) const
{
    String res;

    if (full && getLakeCatalog() != LakeCatalog::None)
    {
        res += "test.";
    }
    res += name;
    return res;
}

String SQLBase::getFullName(const bool setdbname) const
{
    String res;

    if (db || setdbname)
    {
        res += getDatabaseName() + ".";
    }
    res += getBaseName();
    return res;
}

String SQLBase::getSparkCatalogName() const
{
    chassert(isAnyLakeEngine());
    if (getLakeCatalog() == LakeCatalog::None)
    {
        /// DeltaLake tables on Spark must be on the `spark_catalog` :(
        return isAnyDeltaLakeEngine() ? "spark_catalog" : getBaseName(false);
    }
    return db->getSparkCatalogName();
}

static const constexpr String PARTITION_STR = "{_partition_id}";
static const constexpr String SCHEMA_HASH_STR = "{_schema_hash}";

/// Returns the placeholder suffix to append to an S3/Azure path component.
/// Both placeholders may appear in either order, chosen randomly.
static String placeholders(RandomGenerator & rg, bool want_partition, bool want_hash)
{
    if (want_partition && want_hash)
        return rg.nextBool() ? PARTITION_STR + SCHEMA_HASH_STR : SCHEMA_HASH_STR + PARTITION_STR;
    if (want_partition)
        return PARTITION_STR;
    if (want_hash)
        return SCHEMA_HASH_STR;
    return {};
}

void SQLBase::setTablePath(RandomGenerator & rg, const FuzzConfig & fc, const bool has_dolor)
{
    chassert(
        !bucket_path.has_value() && !file_format.has_value() && !file_comp.has_value() && !partition_strategy.has_value()
        && !partition_columns_in_data_file.has_value() && !storage_class_name.has_value());
    has_partition_by = (isRedisEngine() || isKeeperMapEngine() || isMaterializedPostgreSQLEngine() || isAnyIcebergEngine()
                        || isAzureEngine() || isS3Engine())
        && rg.nextSmallNumber() < 3;
    has_order_by = isAnyIcebergEngine() && rg.nextSmallNumber() < 4;
    if (isAnyLakeEngine() || isAnyS3Engine() || isAnyAzureEngine())
    {
        /// Set bucket path first if possible
        String next_bucket_path;
        const String bname = rg.nextSmallNumber() < 4 ? name : ("t" + std::to_string(counter));

        /// Set integration call to use, sometimes create tables in ClickHouse, others also in Spark
        if (has_dolor && isAnyLakeEngine() && rg.nextBool())
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

        if (isAnyLakeEngine())
        {
            const LakeCatalog catalog = getLakeCatalog();

            if (catalog == LakeCatalog::None)
            {
                /// DeltaLake tables on Spark must be on the `spark_catalog` :(
                /// Paimon uses `.db` suffix for database directories (e.g. test.db/)
                next_bucket_path = fmt::format(
                    "{}{}{}{}{}{}",
                    isOnLocal() ? fc.lakes_path.generic_string() : "",
                    isOnLocal() ? "/" : "",
                    (integration == IntegrationCall::Dolor) ? getSparkCatalogName() : "",
                    (integration == IntegrationCall::Dolor) ? (!isAnyIcebergEngine() ? "/test.db/" : "/test/") : "",
                    bname,
                    rg.nextBool() ? "/" : "");
            }
            else if (fc.dolor_server.has_value() && fc.minio_server.has_value())
            {
                const Catalog * cat = nullptr;
                const ServerCredentials & sc = fc.dolor_server.value();

                chassert(isOnS3()); /// What is supported at the moment
                switch (catalog)
                {
                    case LakeCatalog::Glue: cat = &sc.glue_catalog.value(); break;
                    case LakeCatalog::Hive: cat = &sc.hive_catalog.value(); break;
                    case LakeCatalog::REST: cat = &sc.rest_catalog.value(); break;
                    case LakeCatalog::Unity: cat = &sc.unity_catalog.value(); break;
                    default: UNREACHABLE();
                }
                next_bucket_path = fmt::format(
                    "http://{}:{}/{}/{}{}",
                    fc.minio_server.value().server_hostname,
                    fc.minio_server.value().port,
                    cat->warehouse,
                    bname,
                    rg.nextBool() ? "/" : "");
            }
        }
        else
        {
            /// S3 and Azure engines point to files
            bool used_partition = false;

            chassert(isAnyS3Engine() || isAnyAzureEngine());
            bool used_schema_hash = false;

            if (rg.nextBool())
            {
                /// Use a subdirectory
                next_bucket_path += "subdir";
                next_bucket_path += rg.nextBool() ? bname : "";
                const bool want_partition = has_partition_by && rg.nextBool();
                const bool want_hash = rg.nextBool();
                next_bucket_path += placeholders(rg, want_partition, want_hash);
                used_partition |= want_partition;
                used_schema_hash |= want_hash;
                next_bucket_path += "/";
            }
            if (rg.nextBool())
            {
                const bool add_before = rg.nextBool();

                next_bucket_path += "file";
                next_bucket_path += add_before ? bname : "";
                next_bucket_path
                    += placeholders(rg, has_partition_by && !used_partition && rg.nextBool(), !used_schema_hash && rg.nextBool());
                next_bucket_path += !add_before ? bname : "";
                if ((isS3QueueEngine() || isAzureQueueEngine()) && rg.nextMediumNumber() < 81)
                {
                    next_bucket_path += "/";
                }
            }
            if (rg.nextBool())
            {
                next_bucket_path += "*";
            }
            if (rg.nextBool())
            {
                /// Either a generic .data extension or a compression-recognized extension
                /// (exercises ClickHouse's extension-based compression auto-detection)
                static const DB::Strings comp_extensions
                    = {"gz", "gzip", "bz2", "lz4", "xz", "zst", "zstd", "lzma", "br", "brotli", "deflate", "snappy", "7z"};
                next_bucket_path += rg.nextSmallNumber() < 4 ? ".data" : ("." + rg.pickRandomly(comp_extensions));
            }
        }
        bucket_path = std::move(next_bucket_path);
    }
    else if (isKeeperMapEngine() || isArrowFlightEngine() || isFileEngine() || (isURLEngine() && fc.http_server.has_value()))
    {
        /// Nasty strings give bad URLs, so use table counter
        const String bname = rg.nextSmallNumber() < 4 ? name : ("t" + std::to_string(counter));

        if (isKeeperMapEngine() || isArrowFlightEngine())
        {
            bucket_path = fmt::format("/{}", bname);
        }
        else if (isFileEngine())
        {
            bucket_path = fmt::format("{}/{}", fc.server_file_path.generic_string(), bname);
        }
        else if (isURLEngine() && fc.http_server.has_value())
        {
            const ServerCredentials & sc = fc.http_server.value();

            bucket_path = fmt::format("http://{}:{}/{}", sc.server_hostname, sc.port, bname);
        }
    }

    if (isAnyIcebergEngine() && rg.nextMediumNumber() < 91)
    {
        /// Iceberg supports 3 formats
        static const DB::Strings formats = {"ORC", "Avro", "Parquet"};

        file_format = rg.nextMediumNumber() < 91 ? rg.pickRandomly(formats) : rg.pickRandomly(fc.in_out_formats);
    }
    else if (isAnyDeltaLakeEngine() && rg.nextMediumNumber() < 91)
    {
        /// What Delta Lake supports
        file_format = rg.nextMediumNumber() < 91 ? "Parquet" : rg.pickRandomly(fc.in_out_formats);
    }
    else if (isAnyPaimonEngine() && rg.nextMediumNumber() < 91)
    {
        static const DB::Strings formats = {"ORC", "Parquet"};
        file_format = rg.nextMediumNumber() < 91 ? rg.pickRandomly(formats) : rg.pickRandomly(fc.in_out_formats);
    }
    else if (isFileEngine() || ((isAnyS3Engine() || isAnyAzureEngine() || isURLEngine() || isKafkaEngine()) && rg.nextMediumNumber() < 91))
    {
        /// At the moment give more preference for Parquet
        file_format = rg.nextMediumNumber() < 26 ? "Parquet" : rg.pickRandomly(fc.in_out_formats);
    }
    if ((isAnyLakeEngine() || isAnyS3Engine() || isAnyAzureEngine() || isFileEngine() || isURLEngine()) && rg.nextMediumNumber() < 41)
    {
        file_comp = rg.pickRandomly(compressionMethods);
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
    if (isMySQLEngine())
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
    else if (has_dolor && isKafkaEngine())
    {
        integration = IntegrationCall::Dolor;
        topic = "t" + std::to_string(rg.randomInt<uint32_t>(0, 19));
        group = "g" + std::to_string(rg.randomInt<uint32_t>(0, 19));
    }
}

String SQLBase::getTablePath() const
{
    /// Only engines that own a `bucket_path` should be calling this. Object-storage queues
    /// (S3Queue, AzureQueue) are covered by isAnyS3Engine()/isAnyAzureEngine().
    chassert(
        isAnyLakeEngine() || isAnyS3Engine() || isAnyAzureEngine() || isKeeperMapEngine() || isArrowFlightEngine() || isFileEngine()
        || isURLEngine());
    return bucket_path.has_value() ? bucket_path.value() : "test";
}

String SQLBase::getTablePath(RandomGenerator & rg, const bool allow_not_deterministic) const
{
    if ((isAnyS3Engine() || isAnyAzureEngine()) && allow_not_deterministic && rg.nextSmallNumber() < 8)
    {
        String res = bucket_path.has_value() ? bucket_path.value() : "test";
        /// Replace PARTITION BY str
        const size_t partition_pos = res.find(PARTITION_STR);
        if (partition_pos != std::string::npos && rg.nextMediumNumber() < 81)
        {
            res.replace(
                partition_pos,
                PARTITION_STR.length(),
                rg.nextBool() ? std::to_string(rg.randomInt<uint32_t>(0, 100)) : rg.nextString("", true, rg.nextStrlen()));
        }
        /// Replace schema hash str
        const size_t schema_hash_pos = res.find(SCHEMA_HASH_STR);
        if (schema_hash_pos != std::string::npos && rg.nextMediumNumber() < 81)
        {
            res.replace(
                schema_hash_pos,
                SCHEMA_HASH_STR.length(),
                rg.nextBool() ? std::to_string(rg.randomInt<uint32_t>(0, 100)) : rg.nextString("", true, rg.nextStrlen()));
        }
        /// Replace glob for number
        const size_t glob_pos = res.rfind('*');
        if (glob_pos != std::string::npos && rg.nextMediumNumber() < 81)
        {
            res.replace(glob_pos, 1, std::to_string(rg.randomInt<uint32_t>(0, 100)));
        }
        /// Use globs
        const size_t slash_pos = res.rfind('/');
        if (slash_pos != std::string::npos && rg.nextMediumNumber() < 81)
        {
            res.replace(slash_pos + 1, std::string::npos, rg.nextBool() ? "*" : "**");
        }
        return res;
    }
    if (isAnyLakeEngine() && allow_not_deterministic && rg.nextSmallNumber() < 4)
    {
        /// Add or remove '/'
        String res = bucket_path.has_value() ? bucket_path.value() : "test";

        if (endsWith(res, "/"))
        {
            while (endsWith(res, "/"))
            {
                res.pop_back();
            }
        }
        else
        {
            res += "/";
        }
        return res;
    }
    return getTablePath();
}

LakeCatalog SQLBase::getLakeCatalog() const
{
    return db ? db->catalog : LakeCatalog::None;
}

LakeStorage SQLBase::getPossibleLakeStorage() const
{
    return db ? db->storage : LakeStorage::All;
}

LakeFormat SQLBase::getPossibleLakeFormat() const
{
    return db ? db->format : LakeFormat::All;
}

void SQLBase::setName(ExprSchemaTable * est, const String & n, const bool setdbname, std::shared_ptr<SQLDatabase> database)
{
    String res;

    if (database || setdbname)
    {
        est->mutable_database()->set_value(database ? database->getName() : "default");
    }
    if (database && database->catalog != LakeCatalog::None)
    {
        res += "test.";
    }
    res += n;
    est->mutable_table()->set_value(std::move(res));
}

void SQLBase::setName(ExprSchemaTable * est, const bool setdbname) const
{
    if (db || setdbname)
    {
        est->mutable_database()->set_value(getDatabaseName());
    }
    est->mutable_table()->set_value(getBaseName(true));
}

void SQLBase::setName(TableEngine * te) const
{
    te->add_params()->mutable_database()->set_value(getDatabaseName());
    te->add_params()->mutable_table()->set_value(getBaseName());
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

bool SQLTable::supportsFinal(const bool as_alias) const
{
    return engine.supportsFinal() || isBufferEngine() || (isDistributedEngine() && subengine.has_value() && subengine->supportsFinal())
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->supportsFinal());
}

bool SQLTable::hasSignColumn(const bool as_alias) const
{
    return engine.hasSignColumn() || (as_alias && isAliasEngine() && subengine.has_value() && subengine->hasSignColumn());
}

bool SQLTable::hasVersionColumn(const bool as_alias) const
{
    return engine.hasVersionColumn() || (as_alias && isAliasEngine() && subengine.has_value() && subengine->hasVersionColumn());
}

bool SQLTable::areInsertsAppends(const bool as_alias) const
{
    return engine.areInsertsAppends() || isMySQLEngine() || isPostgreSQLEngine()
        || (as_alias && isAliasEngine() && subengine.has_value() && subengine->areInsertsAppends());
}

bool SQLView::supportsFinal() const
{
    return !this->is_materialized;
}

bool SQLDictionary::supportsFinal() const
{
    return false;
}

void WithCluster::setName(SQLIdentifier * f) const
{
    f->set_value(name);
}

const String & ColumnPathChain::getBottomName() const
{
    chassert(!path.empty());
    return path[path.size() - 1].cname;
}

String ColumnPathChain::getBottomNameSQL() const
{
    return "`" + escapeSQLString(getBottomName(), '`') + "`";
}

SQLType * ColumnPathChain::getBottomType() const
{
    chassert(!path.empty());
    return path[path.size() - 1].tp;
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
