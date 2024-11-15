#pragma once

#include "SQLTypes.h"

namespace BuzzHouse
{

using ColumnSpecial = enum ColumnSpecial { NONE = 0, SIGN = 1, IS_DELETED = 2, VERSION = 3 };

using DetachStatus = enum DetachStatus { ATTACHED = 0, DETACHED = 1, PERM_DETACHED = 2 };

struct SQLColumn
{
public:
    uint32_t cname = 0;
    const SQLType * tp = nullptr;
    ColumnSpecial special = ColumnSpecial::NONE;
    std::optional<bool> nullable = std::nullopt;
    std::optional<DModifier> dmod = std::nullopt;

    SQLColumn() = default;
    SQLColumn(const SQLColumn & c)
    {
        this->cname = c.cname;
        this->special = c.special;
        this->nullable = c.nullable;
        this->dmod = c.dmod;
        this->tp = TypeDeepCopy(c.tp);
    }
    SQLColumn(SQLColumn && c)
    {
        this->cname = c.cname;
        this->special = c.special;
        this->nullable = c.nullable;
        this->dmod = c.dmod;
        this->tp = c.tp;
        c.tp = nullptr;
    }
    SQLColumn & operator=(const SQLColumn & c)
    {
        this->cname = c.cname;
        this->special = c.special;
        this->nullable = c.nullable;
        this->dmod = c.dmod;
        this->tp = TypeDeepCopy(c.tp);
        return *this;
    }
    SQLColumn & operator=(SQLColumn && c)
    {
        this->cname = c.cname;
        this->special = c.special;
        this->nullable = c.nullable;
        this->dmod = std::optional<DModifier>(c.dmod);
        this->tp = c.tp;
        c.tp = nullptr;
        return *this;
    }
    ~SQLColumn() { delete tp; }

    bool CanBeInserted() const { return !dmod.has_value() || dmod.value() == DModifier::DEF_DEFAULT; }
};

struct SQLIndex
{
public:
    uint32_t iname = 0;
};

struct SQLDatabase
{
public:
    DetachStatus attached = ATTACHED;
    uint32_t dname = 0;
    DatabaseEngineValues deng;
};

struct SQLBase
{
public:
    uint32_t tname = 0;
    std::shared_ptr<SQLDatabase> db = nullptr;
    DetachStatus attached = ATTACHED;
    std::optional<TableEngineOption> toption;
    TableEngineValues teng;

    bool isMergeTreeFamily() const
    {
        return teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree;
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

    bool isNotTruncableEngine() const
    {
        return isNullEngine() || isSetEngine() || isMySQLEngine() || isPostgreSQLEngine() || isSQLiteEngine() || isRedisEngine()
            || isMongoDBEngine() || isAnyS3Engine() || isHudiEngine() || isDeltaLakeEngine() || isIcebergEngine();
    }
};

struct SQLTable : SQLBase
{
public:
    bool is_temp = false;
    uint32_t col_counter = 0, idx_counter = 0, proj_counter = 0, constr_counter = 0, freeze_counter = 0;
    std::map<uint32_t, SQLColumn> cols, staged_cols;
    std::map<uint32_t, SQLIndex> idxs, staged_idxs;
    std::set<uint32_t> projs, staged_projs, constrs, staged_constrs;
    std::map<uint32_t, std::string> frozen_partitions;

    size_t realNumberOfColumns() const
    {
        size_t res = 0;
        const NestedType * ntp = nullptr;

        for (const auto & entry : cols)
        {
            if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
            {
                res += ntp->subtypes.size();
            }
            else
            {
                res++;
            }
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

typedef struct InsertEntry
{
    std::optional<bool> nullable = std::nullopt;
    ColumnSpecial special = ColumnSpecial::NONE;
    uint32_t cname1 = 0;
    std::optional<uint32_t> cname2 = std::nullopt;
    const SQLType * tp = nullptr;
    std::optional<DModifier> dmod = std::nullopt;

    InsertEntry(
        const std::optional<bool> nu,
        const ColumnSpecial cs,
        const uint32_t c1,
        const std::optional<uint32_t> c2,
        const SQLType * t,
        const std::optional<DModifier> dm)
        : nullable(nu), special(cs), cname1(c1), cname2(c2), tp(t), dmod(dm)
    {
    }
} InsertEntry;

}
