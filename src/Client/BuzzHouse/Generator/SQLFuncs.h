#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <Client/BuzzHouse/AST/SQLProtoStr.h>

namespace BuzzHouse
{

class CHTableFunction
{
public:
    const uint32_t fnum;
    const uint32_t min_args;
    const uint32_t max_args;

    CHTableFunction(const uint32_t f, const uint32_t min_a, const uint32_t m_args)
        : fnum(f)
        , min_args(min_a)
        , max_args(m_args)
    {
    }
};

class CHAggregate
{
public:
    const bool support_nulls_clause;
    const std::string fname;
    const uint32_t min_params;
    const uint32_t max_params;
    const uint32_t min_args;
    const uint32_t max_args;

    CHAggregate(std::string f, const uint32_t min_p, const uint32_t max_p, const uint32_t min_a, const uint32_t m_args, const bool snc)
        : support_nulls_clause(snc)
        , fname(std::move(f))
        , min_params(min_p)
        , max_params(max_p)
        , min_args(min_a)
        , max_args(m_args)
    {
    }
};


class CHFunction
{
public:
    const std::string fname;
    const bool lambda_param;
    const uint32_t min_args;
    const uint32_t max_args;

    CHFunction(std::string f, const bool lamb_p, const uint32_t min_a, const uint32_t m_args)
        : fname(std::move(f))
        , lambda_param(lamb_p)
        , min_args(min_a)
        , max_args(m_args)
    {
    }
};


const constexpr uint32_t ulimited_params = 10000;


const std::vector<CHTableFunction> CHTableFuncs = {
    /// Table Functions
    CHTableFunction(SQLTableFunc::TFarrowFlight, 2, 4),
    CHTableFunction(SQLTableFunc::TFazureBlobStorage, 3, 8),
    CHTableFunction(SQLTableFunc::TFazureBlobStorageCluster, 4, 9),
    CHTableFunction(SQLTableFunc::TFcluster, 2, 4),
    CHTableFunction(SQLTableFunc::TFclusterAllReplicas, 2, 4),
    CHTableFunction(SQLTableFunc::TFdeltaLake, 1, 6),
    CHTableFunction(SQLTableFunc::TFdeltaLakeAzure, 1, 6),
    CHTableFunction(SQLTableFunc::TFdeltaLakeAzureCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFdeltaLakeCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFdeltaLakeLocal, 1, 6),
    CHTableFunction(SQLTableFunc::TFdeltaLakeLocalCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFdeltaLakeS3, 1, 6),
    CHTableFunction(SQLTableFunc::TFdeltaLakeS3Cluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFdictionary, 1, 1),
    CHTableFunction(SQLTableFunc::TFexecutable, 3, 4),
    CHTableFunction(SQLTableFunc::TFfile, 1, 5),
    CHTableFunction(SQLTableFunc::TFfileCluster, 2, 5),
    /// `filesystem([path])` — 0 or 1 string argument, lists files under user_files or the given path.
    CHTableFunction(SQLTableFunc::TFfilesystem, 0, 1),
    CHTableFunction(SQLTableFunc::TFformat, 2, 3),
    CHTableFunction(SQLTableFunc::TFfuzzJSON, 1, 14),
    CHTableFunction(SQLTableFunc::TFfuzzQuery, 1, 3),
    CHTableFunction(SQLTableFunc::TFgcs, 1, 8),
    CHTableFunction(SQLTableFunc::TFgenerateSeries, 2, 3),
    CHTableFunction(SQLTableFunc::TFhdfs, 1, 3),
    CHTableFunction(SQLTableFunc::TFhdfsCluster, 2, 4),
    CHTableFunction(SQLTableFunc::TFhudi, 1, 6),
    CHTableFunction(SQLTableFunc::TFhudiCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFiceberg, 1, 6),
    CHTableFunction(SQLTableFunc::TFicebergAzure, 1, 6),
    CHTableFunction(SQLTableFunc::TFicebergAzureCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFicebergCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFicebergLocal, 1, 6),
    CHTableFunction(SQLTableFunc::TFicebergLocalCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFicebergS3, 1, 6),
    CHTableFunction(SQLTableFunc::TFicebergS3Cluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFinput, 1, 1),
    CHTableFunction(SQLTableFunc::TFjdbc, 1, 3),
    CHTableFunction(SQLTableFunc::TFmerge, 1, 2),
    CHTableFunction(SQLTableFunc::TFmergeTreeAnalyzeIndexes, 2, 3),
    CHTableFunction(SQLTableFunc::TFmergeTreeAnalyzeIndexesUUID, 1, 2),
    CHTableFunction(SQLTableFunc::TFmergeTreeIndex, 2, 4),
    CHTableFunction(SQLTableFunc::TFmergeTreeProjection, 3, 3),
    CHTableFunction(SQLTableFunc::TFmergeTreeTextIndex, 3, 3),
    CHTableFunction(SQLTableFunc::TFmongodb, 6, 8),
    CHTableFunction(SQLTableFunc::TFmysql, 5, 7),
    CHTableFunction(SQLTableFunc::TFnull, 1, 1),
    CHTableFunction(SQLTableFunc::TFnumbers, 1, 3),
    CHTableFunction(SQLTableFunc::TFodbc, 1, 3),
    CHTableFunction(SQLTableFunc::TFpaimon, 1, 6),
    CHTableFunction(SQLTableFunc::TFpaimonAzure, 1, 6),
    CHTableFunction(SQLTableFunc::TFpaimonAzureCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFpaimonCluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFpaimonLocal, 1, 6),
    CHTableFunction(SQLTableFunc::TFpaimonS3, 2, 7),
    CHTableFunction(SQLTableFunc::TFpaimonS3Cluster, 2, 7),
    CHTableFunction(SQLTableFunc::TFpostgresql, 5, 7),
    CHTableFunction(SQLTableFunc::TFprimes, 1, 3),
    CHTableFunction(SQLTableFunc::TFprometheusQuery, 3, 4),
    CHTableFunction(SQLTableFunc::TFprometheusQueryRange, 5, 6),
    CHTableFunction(SQLTableFunc::TFredis, 3, 6),
    CHTableFunction(SQLTableFunc::TFremote, 1, 6),
    CHTableFunction(SQLTableFunc::TFremoteSecure, 1, 6),
    CHTableFunction(SQLTableFunc::TFs3, 1, 8),
    CHTableFunction(SQLTableFunc::TFs3Cluster, 2, 9),
    CHTableFunction(SQLTableFunc::TFsqlite, 2, 2),
    CHTableFunction(SQLTableFunc::TFtimeSeriesData, 1, 2),
    CHTableFunction(SQLTableFunc::TFtimeSeriesMetrics, 1, 2),
    CHTableFunction(SQLTableFunc::TFtimeSeriesSelector, 4, 5),
    CHTableFunction(SQLTableFunc::TFtimeSeriesTags, 1, 2),
    CHTableFunction(SQLTableFunc::TFurl, 1, 4),
    CHTableFunction(SQLTableFunc::TFurlCluster, 4, 4),
    CHTableFunction(SQLTableFunc::TFvalues, 1, ulimited_params),
    CHTableFunction(SQLTableFunc::TFview, 1, 1),
    CHTableFunction(SQLTableFunc::TFytsaurus, 4, 4)};
/// These are infinite loops
/// CHTableFunction(SQLTableFunc::TFgenerateRandom, 1, 4),
/// CHTableFunction(SQLTableFunc::TFloop, 1, 2),

}
