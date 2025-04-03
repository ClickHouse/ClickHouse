#pragma once

#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>

namespace BuzzHouse
{

enum class ColumnSpecial
{
    NONE = 0,
    SIGN = 1,
    IS_DELETED = 2,
    VERSION = 3
};

enum class DetachStatus
{
    ATTACHED = 0,
    DETACHED = 1,
    PERM_DETACHED = 2
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

struct SQLColumn
{
public:
    uint32_t cname = 0;
    SQLType * tp = nullptr;
    ColumnSpecial special = ColumnSpecial::NONE;
    std::optional<bool> nullable;
    std::optional<DModifier> dmod;

    SQLColumn() = default;
    SQLColumn(const SQLColumn & c)
    {
        this->cname = c.cname;
        this->tp = c.tp->typeDeepCopy();
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
    }
    SQLColumn(SQLColumn && c) noexcept
    {
        this->cname = c.cname;
        this->tp = c.tp->typeDeepCopy();
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
    }
    SQLColumn & operator=(const SQLColumn & c)
    {
        if (this == &c)
        {
            return *this;
        }
        this->cname = c.cname;
        delete this->tp;
        this->tp = c.tp->typeDeepCopy();
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
        this->cname = c.cname;
        delete this->tp;
        this->tp = c.tp;
        c.tp = nullptr;
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
        return *this;
    }
    ~SQLColumn() { delete tp; }

    bool canBeInserted() const { return !dmod.has_value() || dmod.value() == DModifier::DEF_DEFAULT; }
};

struct SQLIndex
{
public:
    uint32_t iname = 0;
};

struct SQLDatabase
{
public:
    std::optional<String> cluster;
    DetachStatus attached = DetachStatus::ATTACHED;
    uint32_t dname = 0;
    DatabaseEngineValues deng;
    uint32_t zoo_path_counter;
    String backed_db;
    String backed_disk;

    bool isReplicatedDatabase() const { return deng == DatabaseEngineValues::DReplicated; }

    bool isReplicatedOrSharedDatabase() const { return deng == DatabaseEngineValues::DReplicated || deng == DatabaseEngineValues::DShared; }

    bool isBackupDatabase() const { return deng == DatabaseEngineValues::DBackup; }

    const std::optional<String> & getCluster() const { return cluster; }

    bool isAttached() const { return attached == DetachStatus::ATTACHED; }

    bool isDettached() const { return attached != DetachStatus::ATTACHED; }

    void setName(Database * db) const { db->set_database("d" + std::to_string(dname)); }

    void finishDatabaseSpecification(DatabaseEngine * dspec)
    {
        if (isReplicatedDatabase())
        {
            dspec->add_params()->set_svalue("/test/db" + std::to_string(zoo_path_counter));
            dspec->add_params()->set_svalue("s1");
            dspec->add_params()->set_svalue("r1");
        }
        else if (isBackupDatabase())
        {
            dspec->add_params()->mutable_database()->set_database(backed_db);
            BackupDisk * bd = dspec->add_params()->mutable_disk();
            bd->set_disk(backed_disk);
            this->setName(bd->mutable_database());
        }
    }
};

struct SQLBase
{
public:
    bool is_temp = false, is_deterministic = false;
    uint32_t tname = 0;
    std::shared_ptr<SQLDatabase> db = nullptr;
    std::optional<String> cluster;
    DetachStatus attached = DetachStatus::ATTACHED;
    std::optional<TableEngineOption> toption;
    TableEngineValues teng = TableEngineValues::Null;
    PeerTableDatabase peer_table = PeerTableDatabase::None;
    String file_comp;
    InOutFormat file_format;

    bool isMergeTreeFamily() const
    {
        return teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree;
    }

    bool isSharedMergeTree() const { return isMergeTreeFamily() && toption.has_value() && toption.value() == TableEngineOption::TShared; }

    bool isReplicatedMergeTree() const
    {
        return isMergeTreeFamily() && toption.has_value() && toption.value() == TableEngineOption::TReplicated;
    }

    bool isFileEngine() const { return teng == TableEngineValues::File; }

    bool isJoinEngine() const { return teng == TableEngineValues::Join; }

    bool isNullEngine() const { return teng == TableEngineValues::Null; }

    bool isSetEngine() const { return teng == TableEngineValues::Set; }

    bool isBufferEngine() const { return teng == TableEngineValues::Buffer; }

    bool isRocksEngine() const { return teng == TableEngineValues::EmbeddedRocksDB; }

    bool isMySQLEngine() const { return teng == TableEngineValues::MySQL; }

    bool isPostgreSQLEngine() const { return teng == TableEngineValues::PostgreSQL; }

    bool isSQLiteEngine() const { return teng == TableEngineValues::SQLite; }

    bool isMongoDBEngine() const { return teng == TableEngineValues::MongoDB; }

    bool isRedisEngine() const { return teng == TableEngineValues::Redis; }

    bool isS3Engine() const { return teng == TableEngineValues::S3; }

    bool isS3QueueEngine() const { return teng == TableEngineValues::S3Queue; }

    bool isAnyS3Engine() const { return isS3Engine() || isS3QueueEngine(); }

    bool isHudiEngine() const { return teng == TableEngineValues::Hudi; }

    bool isDeltaLakeEngine() const { return teng == TableEngineValues::DeltaLake; }

    bool isIcebergEngine() const { return teng == TableEngineValues::IcebergS3; }

    bool isMergeEngine() const { return teng == TableEngineValues::Merge; }

    bool isDistributedEngine() const { return teng == TableEngineValues::Distributed; }

    bool isDictionaryEngine() const { return teng == TableEngineValues::Dictionary; }

    bool isGenerateRandomEngine() const { return teng == TableEngineValues::GenerateRandom; }

    bool isNotTruncableEngine() const
    {
        return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
            || isMongoDBEngine() || isAnyS3Engine() || isHudiEngine() || isDeltaLakeEngine() || isIcebergEngine() || isMergeEngine()
            || isDistributedEngine() || isDictionaryEngine() || isGenerateRandomEngine();
    }

    bool isAnotherRelationalDatabaseEngine() const { return isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine(); }

    bool hasDatabasePeer() const { return peer_table != PeerTableDatabase::None; }

    bool hasMySQLPeer() const { return peer_table == PeerTableDatabase::MySQL; }

    bool hasPostgreSQLPeer() const { return peer_table == PeerTableDatabase::PostgreSQL; }

    bool hasSQLitePeer() const { return peer_table == PeerTableDatabase::SQLite; }

    bool hasClickHousePeer() const { return peer_table == PeerTableDatabase::ClickHouse; }

    const std::optional<String> & getCluster() const { return cluster; }

    bool isAttached() const { return (!db || db->isAttached()) && attached == DetachStatus::ATTACHED; }

    bool isDettached() const { return (db && db->attached != DetachStatus::ATTACHED) || attached != DetachStatus::ATTACHED; }
};

struct SQLTable : SQLBase
{
public:
    uint32_t col_counter = 0, idx_counter = 0, proj_counter = 0, constr_counter = 0, freeze_counter = 0;
    std::unordered_map<uint32_t, SQLColumn> cols, staged_cols;
    std::unordered_map<uint32_t, SQLIndex> idxs, staged_idxs;
    std::unordered_set<uint32_t> projs, staged_projs, constrs, staged_constrs;
    std::unordered_map<uint32_t, String> frozen_partitions;

    size_t numberOfInsertableColumns() const
    {
        size_t res = 0;

        for (const auto & entry : cols)
        {
            res += entry.second.canBeInserted() ? 1 : 0;
        }
        return res;
    }

    bool supportsFinal() const
    {
        return (teng >= TableEngineValues::ReplacingMergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree)
            || isBufferEngine() || isDistributedEngine();
    }

    bool hasSignColumn() const
    {
        return teng >= TableEngineValues::CollapsingMergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree;
    }

    bool hasVersionColumn() const { return teng == TableEngineValues::VersionedCollapsingMergeTree; }

    void setName(ExprSchemaTable * est, const bool setdbname) const
    {
        if (db || setdbname)
        {
            est->mutable_database()->set_database("d" + (db ? std::to_string(db->dname) : "efault"));
        }
        est->mutable_table()->set_table("t" + std::to_string(tname));
    }

    void setName(TableEngine * te) const
    {
        te->add_params()->mutable_database()->set_database("d" + (db ? std::to_string(db->dname) : "efault"));
        te->add_params()->mutable_table()->set_table("t" + std::to_string(tname));
    }
};

struct SQLView : SQLBase
{
public:
    bool is_materialized = false, is_refreshable = false, has_with_cols = false;
    uint32_t staged_ncols = 0;
    std::unordered_set<uint32_t> cols;

    void setName(ExprSchemaTable * est, const bool setdbname) const
    {
        if (db || setdbname)
        {
            est->mutable_database()->set_database("d" + (db ? std::to_string(db->dname) : "efault"));
        }
        est->mutable_table()->set_table("v" + std::to_string(tname));
    }

    void setName(TableEngine * te) const
    {
        te->add_params()->mutable_database()->set_database("d" + (db ? std::to_string(db->dname) : "efault"));
        te->add_params()->mutable_table()->set_table("v" + std::to_string(tname));
    }

    bool supportsFinal() const { return !this->is_materialized; }
};

struct SQLDictionary : SQLBase
{
public:
    std::unordered_map<uint32_t, SQLColumn> cols;

    void setName(ExprSchemaTable * est, const bool setdbname) const
    {
        if (db || setdbname)
        {
            est->mutable_database()->set_database("d" + (db ? std::to_string(db->dname) : "efault"));
        }
        est->mutable_table()->set_table("d" + std::to_string(tname));
    }

    void setName(TableEngine * te) const
    {
        te->add_params()->mutable_database()->set_database("d" + (db ? std::to_string(db->dname) : "efault"));
        te->add_params()->mutable_table()->set_table("d" + std::to_string(tname));
    }

    bool supportsFinal() const { return false; }
};

struct SQLFunction
{
public:
    bool is_deterministic = false;
    uint32_t fname = 0, nargs = 0;
    std::optional<String> cluster;

    const std::optional<String> & getCluster() const { return cluster; }

    void setName(Function * f) const { f->set_function("f" + std::to_string(fname)); }
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

    const String & getBottomName() const { return path[path.size() - 1].cname; }

    SQLType * getBottomType() const { return path[path.size() - 1].tp; }
};

}
