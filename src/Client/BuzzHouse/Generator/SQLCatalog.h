#pragma once

#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>

namespace BuzzHouse
{

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
    DeltaLake = 2
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

extern const std::vector<std::vector<OutFormat>> outFormats;
extern const std::unordered_map<OutFormat, InFormat> outIn;
extern const std::vector<std::vector<InOutFormat>> inOutFormats;

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
    SQLColumn(SQLColumn && c) noexcept
    {
        this->cname = std::move(c.cname);
        this->tp = std::move(c.tp);
        this->special = c.special;
        this->nullable = c.nullable;
        this->dmod = c.dmod;
    }
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
    SQLColumn & operator=(SQLColumn && c) noexcept
    {
        if (this == &c)
        {
            return *this;
        }
        this->cname = std::move(c.cname);
        this->tp = std::move(c.tp);
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
        return *this;
    }
    ~SQLColumn() = default;

    bool canBeInserted() const;

    String getColumnName() const;
};

struct WithCluster
{
public:
    String name;
    std::optional<String> cluster;

    const std::optional<String> & getCluster() const { return cluster; }

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
    DatabaseEngineValues deng;
    DetachStatus attached = DetachStatus::ATTACHED;
    IntegrationCall integration = IntegrationCall::None;
    /// For DataLakeCatalog
    LakeCatalog catalog = LakeCatalog::None;
    LakeStorage storage = LakeStorage::All;
    LakeFormat format = LakeFormat::All;

    static void setRandomDatabase(RandomGenerator & rg, SQLDatabase & d);

    static void setName(SQLIdentifier * db, const String & name);

    bool isAtomicDatabase() const;

    bool isMemoryDatabase() const;

    bool isReplicatedDatabase() const;

    bool isSharedDatabase() const;

    bool isBackupDatabase() const;

    bool isOrdinaryDatabase() const;

    bool isDataLakeCatalogDatabase() const;

    bool isReplicatedOrSharedDatabase() const;

    bool isAttached() const;

    bool isDettached() const;

    void setName(SQLIdentifier * db) const;

    String getName() const;

    String getSparkCatalogName() const;

    void setDatabasePath(RandomGenerator & rg, const FuzzConfig & fc);

    void finishDatabaseSpecification(DatabaseEngine * de);
};

struct SQLBase : WithCluster
{
public:
    uint32_t counter = 0;
    bool is_temp = false;
    bool is_deterministic = false;
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
    std::optional<TableEngineOption> toption;
    TableEngineValues teng = TableEngineValues::Null;
    TableEngineValues sub = TableEngineValues::Null;
    PeerTableDatabase peer_table = PeerTableDatabase::None;
    std::optional<InOutFormat> file_format;
    IntegrationCall integration = IntegrationCall::None;

    SQLBase() = default;
    explicit SQLBase(const String && n) { name = n; }
    virtual ~SQLBase() = default;
    SQLBase(const SQLBase &) = default;
    SQLBase & operator=(const SQLBase &) = default;
    SQLBase(SQLBase &&) = default;
    SQLBase & operator=(SQLBase &&) = default;

    static void setDeterministic(const FuzzConfig & fc, RandomGenerator & rg, SQLBase & b);

    static bool supportsFinal(TableEngineValues teng);

    bool isMergeTreeFamily() const;

    bool isLogFamily() const;

    bool isSharedMergeTree() const;

    bool isReplicatedMergeTree() const;

    bool isReplicatedOrSharedMergeTree() const;

    bool isShared() const;

    bool isFileEngine() const;

    bool isJoinEngine() const;

    bool isNullEngine() const;

    bool isSetEngine() const;

    bool isBufferEngine() const;

    bool isRocksEngine() const;

    bool isMemoryEngine() const;

    bool isMySQLEngine() const;

    bool isPostgreSQLEngine() const;

    bool isSQLiteEngine() const;

    bool isMongoDBEngine() const;

    bool isRedisEngine() const;

    bool isS3Engine() const;

    bool isS3QueueEngine() const;

    bool isAnyS3Engine() const;

    bool isAzureEngine() const;

    bool isAzureQueueEngine() const;

    bool isAnyAzureEngine() const;

    bool isAnyQueueEngine() const;

    bool isHudiEngine() const;

    bool isDeltaLakeS3Engine() const;

    bool isDeltaLakeAzureEngine() const;

    bool isDeltaLakeLocalEngine() const;

    bool isAnyDeltaLakeEngine() const;

    bool isIcebergS3Engine() const;

    bool isIcebergAzureEngine() const;

    bool isIcebergLocalEngine() const;

    bool isAnyIcebergEngine() const;

    bool isOnS3() const;

    bool isOnAzure() const;

    bool isOnLocal() const;

    bool isMergeEngine() const;

    bool isDistributedEngine() const;

    bool isDictionaryEngine() const;

    bool isGenerateRandomEngine() const;

    bool isURLEngine() const;

    bool isKeeperMapEngine() const;

    bool isExternalDistributedEngine() const;

    bool isMaterializedPostgreSQLEngine() const;

    bool isArrowFlightEngine() const;

    bool isAliasEngine() const;

    bool isKafkaEngine() const;

    bool isNotTruncableEngine() const;

    bool isEngineReplaceable() const;

    bool isAnotherRelationalDatabaseEngine() const;

    bool hasDatabasePeer() const;

    bool hasMySQLPeer() const;

    bool hasPostgreSQLPeer() const;

    bool hasSQLitePeer() const;

    bool hasClickHousePeer() const;

    bool isAttached() const;

    bool isDettached() const;

    String getDatabaseName() const;

    String getBaseName(bool full = true) const;

    String getFullName(bool setdbname) const;

    String getSparkCatalogName() const;

    void setTablePath(RandomGenerator & rg, const FuzzConfig & fc, bool has_dolor);

    String getTablePath() const;

    String getTablePath(RandomGenerator & rg, bool allow_not_deterministic) const;

    LakeCatalog getLakeCatalog() const;

    LakeStorage getPossibleLakeStorage() const;

    LakeFormat getPossibleLakeFormat() const;

    static void setName(ExprSchemaTable * est, const String & name, bool setdbname, std::shared_ptr<SQLDatabase> database);

    void setName(ExprSchemaTable * est, bool setdbname) const;

    void setName(TableEngine * te) const;
};

struct SQLTable : SQLBase
{
public:
    uint32_t col_counter = 0;
    uint32_t idx_counter = 0;
    uint32_t proj_counter = 0;
    uint32_t constr_counter = 0;
    std::unordered_map<String, SQLColumn> cols;
    std::unordered_map<String, SQLColumn> staged_cols;
    std::unordered_set<String> constrs;
    std::unordered_set<String> staged_constrs;
    std::unordered_map<String, String> frozen_partitions;

    SQLTable()
        : SQLBase("t")
    {
    }

    size_t numberOfInsertableColumns(bool all) const;

    bool supportsFinal() const;

    bool hasSignColumn() const;

    bool hasVersionColumn() const;

    bool areInsertsAppends() const;
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

    bool supportsFinal() const;
};

struct SQLDictionary : SQLBase
{
public:
    std::unordered_map<String, SQLColumn> cols;

    SQLDictionary()
        : SQLBase("d")
    {
    }

    bool supportsFinal() const;
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
    SQLPolicy(const SQLPolicy & other)
        : WithCluster(other)
    {
        this->is_row = other.is_row;
        this->table_key = other.table_key;
        this->name = other.name;
        this->where_expr = other.where_expr;
        this->targets_oracle_role = other.targets_oracle_role;
    }
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

    const String & getBottomName() const;

    /// Returns the bottom name as a backtick-quoted SQL identifier: `escaped_name`.
    String getBottomNameSQL() const;

    SQLType * getBottomType() const;

    String columnPathRef(const String & quote = "`") const;
};

}
