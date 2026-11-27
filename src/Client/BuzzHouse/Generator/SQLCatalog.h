#pragma once

#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>

namespace BuzzHouse
{

/// Replaced by the partition id in the path of a partitioned write
static const constexpr String PARTITION_STR = "{_partition_id}";

enum class ColumnSpecial
{
    NONE = 0,
    SIGN = 1,
    IS_DELETED = 2,
    VERSION = 3,
    TTL_COL = 4,
    ID_COL = 5
};

enum class DetachStatus
{
    ATTACHED = 0,
    DETACHED = 1,
    PERM_DETACHED = 2
};

enum class IntegrationCall
{
    None = 0,
    MySQL = 1,
    PostgreSQL = 2,
    SQLite = 3,
    Redis = 4,
    MongoDB = 5,
    MinIO = 6,
    Azurite = 7,
    HTTP = 8,
    Dolor = 9
};

enum class PeerTableDatabase
{
    None = 0,
    MySQL = 1,
    PostgreSQL = 2,
    SQLite = 3,
    ClickHouse = 4
};

enum class PeerQuery
{
    None = 0,
    ClickHouseOnly = 1,
    AllPeers = 2
};

enum class LakeFormat
{
    All = 0,
    Iceberg = 1,
    DeltaLake = 2,
    Paimon = 3
};

enum class LakeStorage
{
    All = 0,
    S3 = 1,
    Azure = 2,
    Local = 3
};

enum class LakeCatalog
{
    None = 0,
    Glue = 1,
    Hive = 2,
    REST = 3,
    Unity = 4
};


struct SQLColumn
{
public:
    String cname;
    std::unique_ptr<SQLType> tp;
    ColumnSpecial special = ColumnSpecial::NONE;
    std::optional<bool> nullable;
    std::optional<DModifier> dmod;

    SQLColumn() = default;
    SQLColumn(const SQLColumn & c)
    {
        this->cname = c.cname;
        this->tp = c.tp ? c.tp->typeDeepCopy() : nullptr;
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
    }
    SQLColumn(SQLColumn && c) noexcept = default;
    SQLColumn & operator=(const SQLColumn & c)
    {
        if (this == &c)
        {
            return *this;
        }
        this->cname = c.cname;
        this->tp = c.tp ? c.tp->typeDeepCopy() : nullptr;
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
        return *this;
    }
    SQLColumn & operator=(SQLColumn && c) noexcept = default;
    ~SQLColumn() = default;

    [[nodiscard]] bool canBeInserted() const;

    [[nodiscard]] String getColumnName() const;
};

struct WithCluster
{
public:
    String name;
    std::optional<String> cluster;

    [[nodiscard]] const std::optional<String> & getCluster() const { return cluster; }

    void setName(SQLIdentifier * f) const;
};

struct SQLDatabase : WithCluster
{
public:
    bool random_engine = false;
    String keeper_path;
    String shard_name;
    String replica_name;
    uint32_t replica_counter = 0;
    uint32_t shard_counter = 0;
    uint32_t backup_number = 0;
    DatabaseEngineValues deng{};
    DetachStatus attached = DetachStatus::ATTACHED;
    IntegrationCall integration = IntegrationCall::None;
    /// For DataLakeCatalog
    LakeCatalog catalog = LakeCatalog::None;
    LakeStorage storage = LakeStorage::All;
    LakeFormat format = LakeFormat::All;
    /// For Cluster and Remote, the database they proxy (for Overlay, its first source)
    String proxy_target;

    static void setRandomDatabase(RandomGenerator & rg, SQLDatabase & d);

    static void setName(SQLIdentifier * db, const String & name);

    [[nodiscard]] bool isReplicatedDatabase() const;

    [[nodiscard]] bool isSharedDatabase() const;

    [[nodiscard]] bool isBackupDatabase() const;

    [[nodiscard]] bool isOrdinaryDatabase() const;

    [[nodiscard]] bool isDataLakeCatalogDatabase() const;

    [[nodiscard]] bool isClusterDatabase() const;

    [[nodiscard]] bool isRemoteDatabase() const;

    [[nodiscard]] bool isOverlayDatabase() const;

    /// Cluster, Remote, RemoteSecure or Overlay: a read-through view of databases elsewhere
    [[nodiscard]] bool isProxyDatabase() const;

    [[nodiscard]] bool isReplicatedOrSharedDatabase() const;

    [[nodiscard]] bool isAttached() const;

    [[nodiscard]] bool isDettached() const;

    void setName(SQLIdentifier * db) const;

    [[nodiscard]] String getName() const;

    [[nodiscard]] String getSparkCatalogName() const;

    void setDatabasePath(RandomGenerator & rg, const FuzzConfig & fc);

    void finishDatabaseSpecification(DatabaseEngine * de);
};

/// Bundles a table engine value with its optional engine option (Replicated/Shared), so the main
/// engine and an alias/distributed sub-engine can be classified through the same helpers.
struct TableEngineDescriptor
{
    TableEngineValues value = TableEngineValues::Null;
    std::optional<TableEngineOption> option;
    bool is_deterministic = false;
    /// An insert into a UNIQUE KEY table overwrites the rows with the same key
    bool has_unique_key = false;

    TableEngineDescriptor() = default;
    explicit TableEngineDescriptor(const TableEngineValues value_)
        : value(value_)
    {
    }

    [[nodiscard]] bool isMergeTreeFamily() const;

    [[nodiscard]] bool isLogFamily() const;

    [[nodiscard]] bool isShared() const;

    [[nodiscard]] bool isReplicated() const;

    [[nodiscard]] bool supportsFinal() const;

    [[nodiscard]] bool hasSignColumn() const;

    [[nodiscard]] bool hasVersionColumn() const;

    [[nodiscard]] bool areInsertsAppends() const;

    [[nodiscard]] bool isDeterministic() const { return is_deterministic; }
};

struct SQLBase : WithCluster
{
public:
    uint32_t counter = 0;
    bool is_temp = false;
    bool has_partition_by = false;
    bool has_order_by = false;
    bool random_engine = false;
    bool can_run_merges = true;
    uint32_t replica_counter = 0;
    uint32_t shard_counter = 0;
    std::shared_ptr<SQLDatabase> db = nullptr;
    std::optional<String> file_comp;
    std::optional<String> partition_strategy;
    std::optional<String> partition_columns_in_data_file;
    /// The PARTITION BY of an Iceberg table, which `system.tables` does not show
    std::optional<String> iceberg_partition_key;
    std::optional<String> storage_class_name;
    std::optional<String> host_params;
    std::optional<String> bucket_path;
    std::optional<String> topic;
    std::optional<String> group;
    String keeper_path;
    String shard_name;
    String replica_db;
    String replica_table;
    String replica_name;
    DetachStatus attached = DetachStatus::ATTACHED;
    TableEngineDescriptor engine;
    std::optional<TableEngineDescriptor> subengine;
    PeerTableDatabase peer_table = PeerTableDatabase::None;
    std::optional<String> file_format;
    IntegrationCall integration = IntegrationCall::None;

    SQLBase() = default;
    explicit SQLBase(const String && n) { name = n; }
    virtual ~SQLBase() = default;
    SQLBase(const SQLBase &) = default;
    SQLBase & operator=(const SQLBase &) = default;
    SQLBase(SQLBase &&) = default;
    SQLBase & operator=(SQLBase &&) = default;

    static void setDeterministic(const FuzzConfig & fc, RandomGenerator & rg, SQLBase & b);

    /// Assign a synthetic sub-engine that does not correspond to a concrete target table
    /// (Distributed placeholder, ExternalDistributed remote kind, random engine). It inherits this
    /// table's determinism so isDeterministic() is not dragged to false by the descriptor default.
    void setSyntheticSubengine(const TableEngineValues sub_value)
    {
        subengine = TableEngineDescriptor{sub_value};
        subengine->is_deterministic = engine.is_deterministic;
    }

    [[nodiscard]] bool isDeterministic() const;

    [[nodiscard]] bool isMergeTreeFamily(bool as_alias = false) const;

    [[nodiscard]] bool isLogFamily(bool as_alias = false) const;

    [[nodiscard]] bool isSharedMergeTree(bool as_alias = false) const;

    [[nodiscard]] bool isReplicatedMergeTree(bool as_alias = false) const;

    [[nodiscard]] bool isReplicatedOrSharedMergeTree(bool as_alias = false) const;

    [[nodiscard]] bool isShared(bool as_alias = false) const;

    /// The engine is `v`, or with `as_alias` an `Alias` of a `v` table
    [[nodiscard]] bool hasEngine(const TableEngineValues v, const bool as_alias) const
    {
        return engine.value == v
            || (as_alias && engine.value == TableEngineValues::Alias && subengine.has_value() && subengine->value == v);
    }

    [[nodiscard]] bool isFileEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::File, as_alias); }
    [[nodiscard]] bool isSummingMergeTree(const bool as_alias = false) const { return hasEngine(TableEngineValues::SummingMergeTree, as_alias); }
    [[nodiscard]] bool isJoinEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Join, as_alias); }
    [[nodiscard]] bool isNullEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Null, as_alias); }
    [[nodiscard]] bool isSetEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Set, as_alias); }
    [[nodiscard]] bool isBufferEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Buffer, as_alias); }
    [[nodiscard]] bool isRocksEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::EmbeddedRocksDB, as_alias); }
    [[nodiscard]] bool isMySQLEngine(bool as_alias = false) const;
    [[nodiscard]] bool isPostgreSQLEngine(bool as_alias = false) const;
    [[nodiscard]] bool isSQLiteEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::SQLite, as_alias); }
    [[nodiscard]] bool isMongoDBEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::MongoDB, as_alias); }
    [[nodiscard]] bool isRedisEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Redis, as_alias); }
    [[nodiscard]] bool isS3Engine(const bool as_alias = false) const { return hasEngine(TableEngineValues::S3, as_alias); }
    [[nodiscard]] bool isS3QueueEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::S3Queue, as_alias); }
    [[nodiscard]] bool isAnyS3Engine(bool as_alias = false) const;
    [[nodiscard]] bool isAzureEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::AzureBlobStorage, as_alias); }
    [[nodiscard]] bool isAzureQueueEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::AzureQueue, as_alias); }
    [[nodiscard]] bool isAnyAzureEngine(bool as_alias = false) const;
    [[nodiscard]] bool isAnyQueueEngine(bool as_alias = false) const;
    [[nodiscard]] bool isHudiEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Hudi, as_alias); }
    [[nodiscard]] bool isDeltaLakeS3Engine(const bool as_alias = false) const { return hasEngine(TableEngineValues::DeltaLakeS3, as_alias); }
    [[nodiscard]] bool isDeltaLakeAzureEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::DeltaLakeAzure, as_alias); }
    [[nodiscard]] bool isDeltaLakeLocalEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::DeltaLakeLocal, as_alias); }
    [[nodiscard]] bool isAnyDeltaLakeEngine(bool as_alias = false) const;
    [[nodiscard]] bool isIcebergS3Engine(const bool as_alias = false) const { return hasEngine(TableEngineValues::IcebergS3, as_alias); }
    [[nodiscard]] bool isIcebergAzureEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::IcebergAzure, as_alias); }
    [[nodiscard]] bool isIcebergLocalEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::IcebergLocal, as_alias); }
    [[nodiscard]] bool isAnyIcebergEngine(bool as_alias = false) const;
    [[nodiscard]] bool isPaimonS3Engine(const bool as_alias = false) const { return hasEngine(TableEngineValues::PaimonS3, as_alias); }
    [[nodiscard]] bool isPaimonAzureEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::PaimonAzure, as_alias); }
    [[nodiscard]] bool isPaimonLocalEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::PaimonLocal, as_alias); }
    [[nodiscard]] bool isAnyPaimonEngine(bool as_alias = false) const;
    [[nodiscard]] bool isAnyLakeEngine(bool as_alias = false) const;
    [[nodiscard]] bool isOnS3() const;
    [[nodiscard]] bool isOnAzure() const;
    [[nodiscard]] bool isOnLocal() const;
    [[nodiscard]] bool isMergeEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Merge, as_alias); }
    [[nodiscard]] bool isDistributedEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Distributed, as_alias); }
    [[nodiscard]] bool isRemoteSecureEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::RemoteSecure, as_alias); }
    [[nodiscard]] bool isAnyRemoteEngine(bool as_alias = false) const;
    [[nodiscard]] bool isDictionaryEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Dictionary, as_alias); }
    [[nodiscard]] bool isGenerateRandomEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::GenerateRandom, as_alias); }
    [[nodiscard]] bool isURLEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::URL, as_alias); }
    [[nodiscard]] bool isKeeperMapEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::KeeperMap, as_alias); }
    [[nodiscard]] bool isExternalDistributedEngine(const bool as_alias = false) const
    {
        return hasEngine(TableEngineValues::ExternalDistributed, as_alias);
    }
    [[nodiscard]] bool isMaterializedPostgreSQLEngine(const bool as_alias = false) const
    {
        return hasEngine(TableEngineValues::MaterializedPostgreSQL, as_alias);
    }
    [[nodiscard]] bool isArrowFlightEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::ArrowFlight, as_alias); }
    [[nodiscard]] bool isAliasEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Alias, as_alias); }
    [[nodiscard]] bool isKafkaEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::Kafka, as_alias); }
    [[nodiscard]] bool isNATSEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::NATS, as_alias); }
    [[nodiscard]] bool isRabbitMQEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::RabbitMQ, as_alias); }

    /// Kafka, NATS or RabbitMQ: a broker that la Casa del Dolor feeds
    [[nodiscard]] bool isAnyStreamingEngine(bool as_alias = false) const;

    /// File, S3, AzureBlobStorage or URL: one data file that la Casa del Dolor may write
    [[nodiscard]] bool isDataFileEngine() const;

    /// The name la Casa del Dolor gives the engine of a table it handles
    [[nodiscard]] String getDolorEngineName() const;

    [[nodiscard]] bool isTimeSeriesEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::TimeSeries, as_alias); }

    [[nodiscard]] bool isFileLogEngine(const bool as_alias = false) const { return hasEngine(TableEngineValues::FileLog, as_alias); }

    [[nodiscard]] bool isNotTruncableEngine() const;

    [[nodiscard]] bool isEngineReplaceable() const;

    [[nodiscard]] bool isAnotherRelationalDatabaseEngine() const;

    [[nodiscard]] bool hasDatabasePeer() const;

    [[nodiscard]] bool hasMySQLPeer() const;

    [[nodiscard]] bool hasPostgreSQLPeer() const;

    [[nodiscard]] bool hasSQLitePeer() const;

    [[nodiscard]] bool hasClickHousePeer() const;

    [[nodiscard]] bool isAttached() const;

    [[nodiscard]] bool isDettached() const;

    [[nodiscard]] String getDatabaseName() const;

    [[nodiscard]] String getBaseName(bool full = true) const;

    [[nodiscard]] String getFullName(bool setdbname) const;

    [[nodiscard]] String getSparkCatalogName() const;

    void setTablePath(RandomGenerator & rg, const FuzzConfig & fc, bool has_dolor);

    [[nodiscard]] String getTablePath() const;

    String getTablePath(RandomGenerator & rg, bool allow_not_deterministic) const;

    [[nodiscard]] LakeCatalog getLakeCatalog() const;

    [[nodiscard]] LakeStorage getPossibleLakeStorage() const;

    [[nodiscard]] LakeFormat getPossibleLakeFormat() const;

    static void setName(ExprSchemaTable * est, const String & name, bool setdbname, std::shared_ptr<SQLDatabase> database);

    void setName(ExprSchemaTable * est, bool setdbname) const;

    void setName(TableEngine * te) const;
};

struct SQLTable : SQLBase
{
public:
    uint32_t col_counter = 0;
    uint32_t idx_counter = 0;
    uint32_t hidx_counter = 0;
    uint32_t proj_counter = 0;
    uint32_t hproj_counter = 0;
    uint32_t constr_counter = 0;
    std::unordered_map<String, SQLColumn> cols;
    std::unordered_map<String, SQLColumn> staged_cols;
    std::unordered_map<String, String> frozen_partitions;
    /// Names of hypothetical (WHAT-IF) indexes and projections created on this table. They are
    /// session scoped on the server, so this is best effort only.
    std::unordered_set<String> hypothetical_indexes;
    std::unordered_set<String> hypothetical_projections;

    SQLTable()
        : SQLBase("t")
    {
    }

    [[nodiscard]] size_t numberOfInsertableColumns(bool all) const;

    [[nodiscard]] bool supportsFinal(bool as_alias = false) const;

    [[nodiscard]] bool hasSignColumn(bool as_alias = false) const;

    [[nodiscard]] bool hasVersionColumn(bool as_alias = false) const;

    [[nodiscard]] bool areInsertsAppends(bool as_alias = false) const;
};

struct SQLView : SQLBase
{
public:
    bool is_materialized = false;
    bool is_refreshable = false;
    bool has_with_cols = false;
    uint32_t staged_ncols = 0;
    std::unordered_set<String> cols;

    SQLView()
        : SQLBase("v")
    {
    }

    [[nodiscard]] bool supportsFinal() const;
};

struct SQLDictionary : SQLBase
{
public:
    std::unordered_map<String, SQLColumn> cols;

    SQLDictionary()
        : SQLBase("d")
    {
    }

    [[nodiscard]] bool supportsFinal() const;
};

struct SQLFunction : WithCluster
{
public:
    bool is_deterministic = false;
    uint32_t nargs = 0;
};

struct SQLPolicy : WithCluster
{
public:
    bool is_row = true;
    String table_key;
    /// USING predicate stored at creation time; absent means the policy allows all rows.
    std::optional<WhereStatement> where_expr;
    /// True when the policy was created with `TO buzzhouse_oracle_role` — eligible for the row policy oracle.
    bool targets_oracle_role = false;

    SQLPolicy() = default;
    SQLPolicy(const SQLPolicy & other) = default;
    SQLPolicy & operator=(const SQLPolicy & other) = default;
};

struct ColumnPathChainEntry
{
public:
    const String cname;
    SQLType * tp = nullptr;

    ColumnPathChainEntry(const String cn, SQLType * t)
        : cname(cn)
        , tp(t)
    {
    }
};

struct ColumnPathChain
{
public:
    std::optional<bool> nullable;
    ColumnSpecial special = ColumnSpecial::NONE;
    std::optional<DModifier> dmod;
    std::vector<ColumnPathChainEntry> path;

    ColumnPathChain(
        const std::optional<bool> nu, const ColumnSpecial cs, const std::optional<DModifier> dm, const std::vector<ColumnPathChainEntry> p)
        : nullable(nu)
        , special(cs)
        , dmod(dm)
        , path(p)
    {
    }

    [[nodiscard]] const String & getBottomName() const;

    /// Returns the bottom name as a backtick-quoted SQL identifier: `escaped_name`.
    [[nodiscard]] String getBottomNameSQL() const;

    [[nodiscard]] SQLType * getBottomType() const;

    /// The dotted path inside `quote`, each name escaped for it, or raw with an empty `quote`
    [[nodiscard]] String columnPathRef(const String & quote = "`") const;
};

}
