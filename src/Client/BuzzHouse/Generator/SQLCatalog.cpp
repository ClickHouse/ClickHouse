#include <Client/BuzzHouse/Generator/SQLCatalog.h>

namespace BuzzHouse
{

const std::vector<std::vector<OutFormat>> outFormats
    = {{OutFormat::OUT_Arrow},
       {OutFormat::OUT_Avro},
       {OutFormat::OUT_BSONEachRow},
       {OutFormat::OUT_Buffers},
       {OutFormat::OUT_CSV, OutFormat::OUT_CSVWithNames, OutFormat::OUT_CSVWithNamesAndTypes},
       {OutFormat::OUT_CustomSeparated, OutFormat::OUT_CustomSeparatedWithNames, OutFormat::OUT_CustomSeparatedWithNamesAndTypes},
       {OutFormat::OUT_JSON,
        OutFormat::OUT_JSONColumns,
        OutFormat::OUT_JSONColumnsWithMetadata,
        OutFormat::OUT_JSONCompact,
        OutFormat::OUT_JSONCompactColumns,
        OutFormat::OUT_JSONCompactEachRow,
        OutFormat::OUT_JSONCompactEachRowWithNames,
        OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes,
        OutFormat::OUT_JSONCompactStringsEachRow,
        OutFormat::OUT_JSONCompactStringsEachRowWithNames,
        OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes,
        OutFormat::OUT_JSONEachRow,
        OutFormat::OUT_JSONLines,
        OutFormat::OUT_JSONObjectEachRow,
        OutFormat::OUT_JSONStringsEachRow},
       {OutFormat::OUT_LineAsString},
       {OutFormat::OUT_MsgPack},
       {OutFormat::OUT_Native},
       {OutFormat::OUT_ORC},
       {OutFormat::OUT_Parquet},
       {OutFormat::OUT_Protobuf, OutFormat::OUT_ProtobufSingle},
       {OutFormat::OUT_RawBLOB},
       {OutFormat::OUT_RowBinary, OutFormat::OUT_RowBinaryWithNames, OutFormat::OUT_RowBinaryWithNamesAndTypes},
       {OutFormat::OUT_TabSeparated,
        OutFormat::OUT_TabSeparatedRaw,
        OutFormat::OUT_TabSeparatedRawWithNames,
        OutFormat::OUT_TabSeparatedRawWithNamesAndTypes,
        OutFormat::OUT_TabSeparatedWithNames,
        OutFormat::OUT_TabSeparatedWithNamesAndTypes},
       {OutFormat::OUT_TSKV},
       {OutFormat::OUT_Values}};

const std::unordered_map<OutFormat, InFormat> outIn
    = {{OutFormat::OUT_Arrow, InFormat::IN_Arrow},
       {OutFormat::OUT_Avro, InFormat::IN_Avro},
       {OutFormat::OUT_BSONEachRow, InFormat::IN_BSONEachRow},
       {OutFormat::OUT_Buffers, InFormat::IN_Buffers},
       {OutFormat::OUT_CSV, InFormat::IN_CSV},
       {OutFormat::OUT_CSVWithNames, InFormat::IN_CSVWithNames},
       {OutFormat::OUT_CSVWithNamesAndTypes, InFormat::IN_CSVWithNamesAndTypes},
       {OutFormat::OUT_CustomSeparated, InFormat::IN_CustomSeparated},
       {OutFormat::OUT_CustomSeparatedWithNames, InFormat::IN_CustomSeparatedWithNames},
       {OutFormat::OUT_CustomSeparatedWithNamesAndTypes, InFormat::IN_CustomSeparatedWithNamesAndTypes},
       {OutFormat::OUT_JSON, InFormat::IN_JSON},
       {OutFormat::OUT_JSONColumns, InFormat::IN_JSONColumns},
       {OutFormat::OUT_JSONColumnsWithMetadata, InFormat::IN_JSONColumnsWithMetadata},
       {OutFormat::OUT_JSONCompact, InFormat::IN_JSONCompact},
       {OutFormat::OUT_JSONCompactColumns, InFormat::IN_JSONCompactColumns},
       {OutFormat::OUT_JSONCompactEachRow, InFormat::IN_JSONCompactEachRow},
       {OutFormat::OUT_JSONCompactEachRowWithNames, InFormat::IN_JSONCompactEachRowWithNames},
       {OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes, InFormat::IN_JSONCompactEachRowWithNamesAndTypes},
       {OutFormat::OUT_JSONCompactStringsEachRow, InFormat::IN_JSONCompactStringsEachRow},
       {OutFormat::OUT_JSONCompactStringsEachRowWithNames, InFormat::IN_JSONCompactStringsEachRowWithNames},
       {OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes, InFormat::IN_JSONCompactStringsEachRowWithNamesAndTypes},
       {OutFormat::OUT_JSONEachRow, InFormat::IN_JSONEachRow},
       {OutFormat::OUT_JSONLines, InFormat::IN_JSONLines},
       {OutFormat::OUT_JSONObjectEachRow, InFormat::IN_JSONObjectEachRow},
       {OutFormat::OUT_JSONStringsEachRow, InFormat::IN_JSONStringsEachRow},
       {OutFormat::OUT_LineAsString, InFormat::IN_LineAsString},
       {OutFormat::OUT_MsgPack, InFormat::IN_MsgPack},
       {OutFormat::OUT_Native, InFormat::IN_Native},
       {OutFormat::OUT_ORC, InFormat::IN_ORC},
       {OutFormat::OUT_Parquet, InFormat::IN_Parquet},
       {OutFormat::OUT_Protobuf, InFormat::IN_Protobuf},
       {OutFormat::OUT_ProtobufSingle, InFormat::IN_ProtobufSingle},
       {OutFormat::OUT_RawBLOB, InFormat::IN_RawBLOB},
       {OutFormat::OUT_RowBinary, InFormat::IN_RowBinary},
       {OutFormat::OUT_RowBinaryWithNames, InFormat::IN_RowBinaryWithNames},
       {OutFormat::OUT_RowBinaryWithNamesAndTypes, InFormat::IN_RowBinaryWithNamesAndTypes},
       {OutFormat::OUT_TabSeparated, InFormat::IN_TabSeparated},
       {OutFormat::OUT_TabSeparatedRaw, InFormat::IN_TabSeparatedRaw},
       {OutFormat::OUT_TabSeparatedRawWithNames, InFormat::IN_TabSeparatedRawWithNames},
       {OutFormat::OUT_TabSeparatedRawWithNamesAndTypes, InFormat::IN_TabSeparatedRawWithNamesAndTypes},
       {OutFormat::OUT_TabSeparatedWithNames, InFormat::IN_TabSeparatedWithNames},
       {OutFormat::OUT_TabSeparatedWithNamesAndTypes, InFormat::IN_TabSeparatedWithNamesAndTypes},
       {OutFormat::OUT_TSKV, InFormat::IN_TSKV},
       {OutFormat::OUT_Values, InFormat::IN_Values}};

const std::vector<std::vector<InOutFormat>> inOutFormats = {
    {InOutFormat::INOUT_Arrow, InOutFormat::INOUT_ArrowStream},
    {InOutFormat::INOUT_Avro},
    {InOutFormat::INOUT_BSONEachRow},
    {InOutFormat::INOUT_Buffers},
    {InOutFormat::INOUT_CSV, InOutFormat::INOUT_CSVWithNames, InOutFormat::INOUT_CSVWithNamesAndTypes},
    {InOutFormat::INOUT_CustomSeparated, InOutFormat::INOUT_CustomSeparatedWithNames, InOutFormat::INOUT_CustomSeparatedWithNamesAndTypes},
    {InOutFormat::INOUT_JSON,
     InOutFormat::INOUT_JSONColumns,
     InOutFormat::INOUT_JSONColumnsWithMetadata,
     InOutFormat::INOUT_JSONCompact,
     InOutFormat::INOUT_JSONCompactColumns,
     InOutFormat::INOUT_JSONCompactEachRow,
     InOutFormat::INOUT_JSONCompactEachRowWithNames,
     InOutFormat::INOUT_JSONCompactEachRowWithNamesAndTypes,
     InOutFormat::INOUT_JSONCompactStringsEachRow,
     InOutFormat::INOUT_JSONCompactStringsEachRowWithNames,
     InOutFormat::INOUT_JSONCompactStringsEachRowWithNamesAndTypes,
     InOutFormat::INOUT_JSONEachRow,
     InOutFormat::INOUT_JSONLines,
     InOutFormat::INOUT_JSONObjectEachRow,
     InOutFormat::INOUT_JSONStringsEachRow},
    {InOutFormat::INOUT_LineAsString},
    {InOutFormat::INOUT_MsgPack},
    {InOutFormat::INOUT_Native},
    {InOutFormat::INOUT_Npy},
    {InOutFormat::INOUT_ORC},
    {InOutFormat::INOUT_Parquet},
    {InOutFormat::INOUT_Protobuf, InOutFormat::INOUT_ProtobufList, InOutFormat::INOUT_ProtobufSingle},
    {InOutFormat::INOUT_RawBLOB},
    {InOutFormat::INOUT_RowBinary, InOutFormat::INOUT_RowBinaryWithNames, InOutFormat::INOUT_RowBinaryWithNamesAndTypes},
    {InOutFormat::INOUT_TabSeparated,
     InOutFormat::INOUT_TabSeparatedRaw,
     InOutFormat::INOUT_TabSeparatedRawWithNames,
     InOutFormat::INOUT_TabSeparatedRawWithNamesAndTypes,
     InOutFormat::INOUT_TabSeparatedWithNames,
     InOutFormat::INOUT_TabSeparatedWithNamesAndTypes},
    {InOutFormat::INOUT_Template},
    {InOutFormat::INOUT_TSKV},
    {InOutFormat::INOUT_Values}};

bool SQLColumn::canBeInserted() const
{
    return !dmod.has_value() || dmod.value() == DModifier::DEF_DEFAULT;
}

String SQLColumn::getColumnName() const
{
    return "c" + std::to_string(cname);
}

void SQLDatabase::setRandomDatabase(RandomGenerator & rg, SQLDatabase & d)
{
    d.random_engine = rg.nextMediumNumber() < 4;
}

void SQLDatabase::setName(Database * db, const uint32_t name)
{
    db->set_database("d" + std::to_string(name));
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

bool SQLDatabase::isLazyDatabase() const
{
    return deng == DatabaseEngineValues::DLazy;
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

const std::optional<String> & SQLDatabase::getCluster() const
{
    return cluster;
}

bool SQLDatabase::isAttached() const
{
    return attached == DetachStatus::ATTACHED;
}

bool SQLDatabase::isDettached() const
{
    return attached != DetachStatus::ATTACHED;
}

void SQLDatabase::setName(Database * db) const
{
    SQLDatabase::setName(db, dname);
}

String SQLDatabase::getName() const
{
    return "d" + std::to_string(dname);
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

void SQLBase::setDeterministic(const FuzzConfig & fc, RandomGenerator & rg, SQLBase & b)
{
    b.is_deterministic = rg.nextMediumNumber() <= fc.deterministic_prob;
    b.random_engine = !b.is_deterministic && rg.nextMediumNumber() < 6;
}

bool SQLBase::supportsFinal(const TableEngineValues teng)
{
    return teng >= TableEngineValues::ReplacingMergeTree && teng <= TableEngineValues::GraphiteMergeTree;
}

bool SQLBase::isMergeTreeFamily() const
{
    return teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::GraphiteMergeTree;
}

bool SQLBase::isLogFamily() const
{
    return teng >= TableEngineValues::StripeLog && teng <= TableEngineValues::TinyLog;
}

bool SQLBase::isSharedMergeTree() const
{
    return isMergeTreeFamily() && toption.has_value() && toption.value() == TableEngineOption::TShared;
}

bool SQLBase::isReplicatedMergeTree() const
{
    return isMergeTreeFamily() && toption.has_value() && toption.value() == TableEngineOption::TReplicated;
}

bool SQLBase::isReplicatedOrSharedMergeTree() const
{
    return isReplicatedMergeTree() || isSharedMergeTree();
}

bool SQLBase::isShared() const
{
    return toption.has_value() && toption.value() == TableEngineOption::TShared;
}

bool SQLBase::isFileEngine() const
{
    return teng == TableEngineValues::File;
}

bool SQLBase::isJoinEngine() const
{
    return teng == TableEngineValues::Join;
}

bool SQLBase::isNullEngine() const
{
    return teng == TableEngineValues::Null;
}

bool SQLBase::isSetEngine() const
{
    return teng == TableEngineValues::Set;
}

bool SQLBase::isBufferEngine() const
{
    return teng == TableEngineValues::Buffer;
}

bool SQLBase::isRocksEngine() const
{
    return teng == TableEngineValues::EmbeddedRocksDB;
}

bool SQLBase::isMemoryEngine() const
{
    return teng == TableEngineValues::Memory;
}

bool SQLBase::isMySQLEngine() const
{
    return teng == TableEngineValues::MySQL || (isExternalDistributedEngine() && sub == TableEngineValues::MySQL);
}

bool SQLBase::isPostgreSQLEngine() const
{
    return teng == TableEngineValues::PostgreSQL || teng == TableEngineValues::MaterializedPostgreSQL
        || (isExternalDistributedEngine() && (sub == TableEngineValues::PostgreSQL || sub == TableEngineValues::MaterializedPostgreSQL));
}

bool SQLBase::isSQLiteEngine() const
{
    return teng == TableEngineValues::SQLite;
}

bool SQLBase::isMongoDBEngine() const
{
    return teng == TableEngineValues::MongoDB;
}

bool SQLBase::isRedisEngine() const
{
    return teng == TableEngineValues::Redis;
}

bool SQLBase::isS3Engine() const
{
    return teng == TableEngineValues::S3;
}

bool SQLBase::isS3QueueEngine() const
{
    return teng == TableEngineValues::S3Queue;
}

bool SQLBase::isAnyS3Engine() const
{
    return isS3Engine() || isS3QueueEngine();
}

bool SQLBase::isAzureEngine() const
{
    return teng == TableEngineValues::AzureBlobStorage;
}

bool SQLBase::isAzureQueueEngine() const
{
    return teng == TableEngineValues::AzureQueue;
}

bool SQLBase::isAnyAzureEngine() const
{
    return isAzureEngine() || isAzureQueueEngine();
}

bool SQLBase::isAnyQueueEngine() const
{
    return isS3QueueEngine() || isAzureQueueEngine();
}

bool SQLBase::isHudiEngine() const
{
    return teng == TableEngineValues::Hudi;
}

bool SQLBase::isDeltaLakeS3Engine() const
{
    return teng == TableEngineValues::DeltaLakeS3;
}

bool SQLBase::isDeltaLakeAzureEngine() const
{
    return teng == TableEngineValues::DeltaLakeAzure;
}

bool SQLBase::isDeltaLakeLocalEngine() const
{
    return teng == TableEngineValues::DeltaLakeLocal;
}

bool SQLBase::isAnyDeltaLakeEngine() const
{
    return teng >= TableEngineValues::DeltaLakeS3 && teng <= TableEngineValues::DeltaLakeLocal;
}

bool SQLBase::isIcebergS3Engine() const
{
    return teng == TableEngineValues::IcebergS3;
}

bool SQLBase::isIcebergAzureEngine() const
{
    return teng == TableEngineValues::IcebergAzure;
}

bool SQLBase::isIcebergLocalEngine() const
{
    return teng == TableEngineValues::IcebergLocal;
}

bool SQLBase::isAnyIcebergEngine() const
{
    return teng >= TableEngineValues::IcebergS3 && teng <= TableEngineValues::IcebergLocal;
}

bool SQLBase::isOnS3() const
{
    return isIcebergS3Engine() || isDeltaLakeS3Engine() || isAnyS3Engine();
}

bool SQLBase::isOnAzure() const
{
    return isIcebergAzureEngine() || isDeltaLakeAzureEngine() || isAnyAzureEngine();
}

bool SQLBase::isOnLocal() const
{
    return isIcebergLocalEngine() || isDeltaLakeLocalEngine();
}

bool SQLBase::isMergeEngine() const
{
    return teng == TableEngineValues::Merge;
}

bool SQLBase::isDistributedEngine() const
{
    return teng == TableEngineValues::Distributed;
}

bool SQLBase::isDictionaryEngine() const
{
    return teng == TableEngineValues::Dictionary;
}

bool SQLBase::isGenerateRandomEngine() const
{
    return teng == TableEngineValues::GenerateRandom;
}

bool SQLBase::isURLEngine() const
{
    return teng == TableEngineValues::URL;
}

bool SQLBase::isKeeperMapEngine() const
{
    return teng == TableEngineValues::KeeperMap;
}

bool SQLBase::isExternalDistributedEngine() const
{
    return teng == TableEngineValues::ExternalDistributed;
}

bool SQLBase::isMaterializedPostgreSQLEngine() const
{
    return teng == TableEngineValues::MaterializedPostgreSQL;
}

bool SQLBase::isArrowFlightEngine() const
{
    return teng == TableEngineValues::ArrowFlight;
}

bool SQLBase::isAliasEngine() const
{
    return teng == TableEngineValues::Alias;
}

bool SQLBase::isKafkaEngine() const
{
    return teng == TableEngineValues::Kafka;
}

bool SQLBase::isNotTruncableEngine() const
{
    return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
        || isMongoDBEngine() || isHudiEngine() || isMergeEngine() || isDistributedEngine() || isDictionaryEngine()
        || isGenerateRandomEngine() || isMaterializedPostgreSQLEngine();
}

bool SQLBase::isEngineReplaceable() const
{
    return isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine()
        || isAnyAzureEngine() || isFileEngine() || isURLEngine() || isRedisEngine() || isMongoDBEngine() || isDictionaryEngine()
        || isNullEngine() || isGenerateRandomEngine() || isArrowFlightEngine();
}

bool SQLBase::isAnotherRelationalDatabaseEngine() const
{
    return isMySQLEngine() || isPostgreSQLEngine() || isMaterializedPostgreSQLEngine() || isSQLiteEngine();
}

bool SQLBase::hasDatabasePeer() const
{
    chassert(is_deterministic || peer_table == PeerTableDatabase::None);
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

const std::optional<String> & SQLBase::getCluster() const
{
    return cluster;
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
                    "{}{}{}{}t{}{}",
                    isOnLocal() ? fc.lakes_path.generic_string() : "",
                    isOnLocal() ? "/" : "",
                    (integration == IntegrationCall::Dolor) ? getSparkCatalogName() : "",
                    (integration == IntegrationCall::Dolor) ? "/test/" : "",
                    tname,
                    rg.nextBool() ? "/" : "");
            }
            else if (fc.dolor_server.has_value() && fc.minio_server.has_value())
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
                    "http://{}:{}/{}/t{}{}",
                    fc.minio_server.value().server_hostname,
                    fc.minio_server.value().port,
                    cat->warehouse,
                    tname,
                    rg.nextBool() ? "/" : "");
            }
        }
        else
        {
            /// S3 and Azure engines point to files
            bool used_partition = false;

            chassert(isAnyS3Engine() || isAnyAzureEngine());
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
            if (rg.nextBool())
            {
                const bool add_before = rg.nextBool();

                next_bucket_path += "file";
                next_bucket_path += add_before ? std::to_string(tname) : "";
                if (has_partition_by && !used_partition && rg.nextBool())
                {
                    next_bucket_path += PARTITION_STR;
                }
                next_bucket_path += !add_before ? std::to_string(tname) : "";
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
                next_bucket_path += ".data";
            }
        }
        bucket_path = std::move(next_bucket_path);
    }
    if (isAnyIcebergEngine() && rg.nextMediumNumber() < 91)
    {
        /// Iceberg supports 3 formats
        static const std::vector<InOutFormat> & formats = {InOutFormat::INOUT_ORC, InOutFormat::INOUT_Avro, InOutFormat::INOUT_Parquet};

        file_format = rg.nextMediumNumber() < 91 ? rg.pickRandomly(formats) : rg.pickRandomly(rg.pickRandomly(inOutFormats));
    }
    else if (isAnyDeltaLakeEngine() && rg.nextMediumNumber() < 91)
    {
        /// What Delta Lake supports
        file_format = rg.nextMediumNumber() < 91 ? INOUT_Parquet : rg.pickRandomly(rg.pickRandomly(inOutFormats));
    }
    else if (isAnyS3Engine() || isAnyAzureEngine() || isFileEngine() || isURLEngine() || isKafkaEngine())
    {
        /// Set other parameters
        if (isFileEngine() || rg.nextMediumNumber() < 91)
        {
            /// At the moment give more preference for Parquet
            file_format = rg.nextMediumNumber() < 26 ? INOUT_Parquet : rg.pickRandomly(rg.pickRandomly(inOutFormats));
        }
        if (!isKafkaEngine() && rg.nextMediumNumber() < 51)
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

String SQLBase::getTablePath(const FuzzConfig & fc) const
{
    if (isAnyIcebergEngine() || isAnyDeltaLakeEngine() || isAnyS3Engine() || isAnyAzureEngine())
    {
        return bucket_path.has_value() ? bucket_path.value() : "test";
    }
    if (isFileEngine())
    {
        return fmt::format("{}/file{}", fc.server_file_path.generic_string(), tname);
    }
    if (isURLEngine())
    {
        if (fc.http_server.has_value())
        {
            const ServerCredentials & sc = fc.http_server.value();

            return fmt::format("http://{}:{}/file{}", sc.server_hostname, sc.port, tname);
        }
        return "test";
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
    if ((isAnyIcebergEngine() || isAnyDeltaLakeEngine()) && allow_not_deterministic && rg.nextSmallNumber() < 4)
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
    return getTablePath(fc);
}

String SQLBase::getMetadataPath(const FuzzConfig & fc) const
{
    return has_metadata ? fmt::format("{}/metadatat{}", fc.server_file_path.generic_string(), tname) : "";
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

void SQLBase::setName(
    ExprSchemaTable * est, const String & prefix, const bool setdbname, std::shared_ptr<SQLDatabase> database, const uint32_t name)
{
    String res;

    if (database || setdbname)
    {
        est->mutable_database()->set_database("d" + (database ? std::to_string(database->dname) : "efault"));
    }
    if (database && database->catalog != LakeCatalog::None)
    {
        res += "test.";
    }
    res += prefix + std::to_string(name);
    est->mutable_table()->set_table(std::move(res));
}

void SQLBase::setName(ExprSchemaTable * est, const bool setdbname) const
{
    SQLBase::setName(est, this->prefix, setdbname, db, tname);
}

void SQLBase::setName(TableEngine * te) const
{
    te->add_params()->mutable_database()->set_database(getDatabaseName());
    te->add_params()->mutable_table()->set_table(getTableName());
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

bool SQLTable::supportsFinal() const
{
    return SQLBase::supportsFinal(teng) || isBufferEngine() || (isDistributedEngine() && SQLBase::supportsFinal(sub));
}

bool SQLTable::hasSignColumn() const
{
    return teng >= TableEngineValues::CollapsingMergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree;
}

bool SQLTable::hasVersionColumn() const
{
    return teng == TableEngineValues::VersionedCollapsingMergeTree;
}

bool SQLTable::areInsertsAppends() const
{
    return teng == TableEngineValues::MergeTree || isLogFamily() || isMemoryEngine() || isMySQLEngine() || isPostgreSQLEngine()
        || isSQLiteEngine() || isMongoDBEngine() || isRedisEngine() || isHudiEngine() || isAnyDeltaLakeEngine() || isAnyIcebergEngine()
        || isDictionaryEngine();
}

bool SQLView::supportsFinal() const
{
    return !this->is_materialized;
}

bool SQLDictionary::supportsFinal() const
{
    return false;
}

const std::optional<String> & SQLFunction::getCluster() const
{
    return cluster;
}

void SQLFunction::setName(Function * f) const
{
    f->set_function("f" + std::to_string(fname));
}

const String & ColumnPathChain::getBottomName() const
{
    return path[path.size() - 1].cname;
}

SQLType * ColumnPathChain::getBottomType() const
{
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
