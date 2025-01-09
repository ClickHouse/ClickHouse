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

struct SQLColumn
{
public:
    uint32_t cname = 0;
    SQLType * tp = nullptr;
    ColumnSpecial special = ColumnSpecial::NONE;
    std::optional<bool> nullable = std::nullopt;
    std::optional<DModifier> dmod = std::nullopt;

    SQLColumn() = default;
    SQLColumn(const SQLColumn & c)
    {
        this->cname = c.cname;
        this->tp = TypeDeepCopy(c.tp);
        this->special = c.special;
        this->nullable = std::optional<bool>(c.nullable);
        this->dmod = std::optional<DModifier>(c.dmod);
    }
    SQLColumn(SQLColumn && c) noexcept
    {
        this->cname = c.cname;
        this->tp = TypeDeepCopy(c.tp);
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
        this->tp = TypeDeepCopy(c.tp);
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
    DetachStatus attached = DetachStatus::ATTACHED;
    uint32_t dname = 0;
    DatabaseEngineValues deng;

    bool isReplicatedDatabase() const { return deng == DatabaseEngineValues::DReplicated; }
};

struct SQLBase
{
public:
    bool is_temp = false;
    uint32_t tname = 0;
    std::shared_ptr<SQLDatabase> db = nullptr;
    DetachStatus attached = DetachStatus::ATTACHED;
    std::optional<TableEngineOption> toption = std::nullopt;
    TableEngineValues teng = TableEngineValues::Null;
    PeerTableDatabase peer_table = PeerTableDatabase::None;
    std::string file_comp;
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

    bool isNotTruncableEngine() const
    {
        return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
            || isMongoDBEngine() || isAnyS3Engine() || isHudiEngine() || isDeltaLakeEngine() || isIcebergEngine() || isMergeEngine();
    }

    bool hasDatabasePeer() const { return peer_table != PeerTableDatabase::None; }

    bool hasMySQLPeer() const { return peer_table == PeerTableDatabase::MySQL; }

    bool hasPostgreSQLPeer() const { return peer_table == PeerTableDatabase::PostgreSQL; }

    bool hasSQLitePeer() const { return peer_table == PeerTableDatabase::SQLite; }

    bool hasClickHousePeer() const { return peer_table == PeerTableDatabase::ClickHouse; }
};

struct SQLTable : SQLBase
{
public:
    uint32_t col_counter = 0, idx_counter = 0, proj_counter = 0, constr_counter = 0, freeze_counter = 0;
    std::map<uint32_t, SQLColumn> cols, staged_cols;
    std::map<uint32_t, SQLIndex> idxs, staged_idxs;
    std::set<uint32_t> projs, staged_projs, constrs, staged_constrs;
    std::map<uint32_t, std::string> frozen_partitions;

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
            || this->isBufferEngine();
    }

    bool hasSignColumn() const
    {
        return teng >= TableEngineValues::CollapsingMergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree;
    }

    bool hasVersionColumn() const { return teng == TableEngineValues::VersionedCollapsingMergeTree; }
};

struct SQLView : SQLBase
{
public:
    bool is_materialized = false, is_refreshable = false, is_deterministic = false;
    uint32_t ncols = 1, staged_ncols = 1;
};

struct SQLFunction
{
public:
    bool is_deterministic = false;
    uint32_t fname = 0, nargs = 0;
};

struct ColumnPathChainEntry
{
public:
    const std::string cname;
    SQLType * tp = nullptr;

    ColumnPathChainEntry(const std::string cn, SQLType * t) : cname(cn), tp(t) { }
};

struct ColumnPathChain
{
public:
    std::optional<bool> nullable = std::nullopt;
    ColumnSpecial special = ColumnSpecial::NONE;
    std::optional<DModifier> dmod = std::nullopt;
    std::vector<ColumnPathChainEntry> path;

    ColumnPathChain(
        const std::optional<bool> nu, const ColumnSpecial cs, const std::optional<DModifier> dm, const std::vector<ColumnPathChainEntry> p)
        : nullable(nu), special(cs), dmod(dm), path(p)
    {
    }

    const std::string & getBottomName() const { return path[path.size() - 1].cname; }

    SQLType * getBottomType() const { return path[path.size() - 1].tp; }
};

}
