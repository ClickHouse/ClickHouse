#pragma once

#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Client/BuzzHouse/Utils/MD5Impl.h>

namespace BuzzHouse
{

enum class DumpOracleStrategy
{
    DUMP_TABLE = 1,
    OPTIMIZE = 2,
    REATTACH = 3,
    BACKUP_RESTORE = 4
};

class QueryOracle
{
private:
    static const std::vector<std::vector<OutFormat>> oracleFormats;
    const FuzzConfig & fc;
    const std::filesystem::path qcfile, qsfile, qfile_peer;

    MD5Impl md5_hash1, md5_hash2;
    Poco::DigestEngine::Digest first_digest, second_digest;
    PerformanceResult res1, res2;

    PeerQuery peer_query = PeerQuery::AllPeers;
    int first_errcode = 0;
    bool other_steps_sucess = true, can_test_oracle_result, measure_performance, compare_explain;

    std::unordered_set<uint32_t> found_tables;
    DB::Strings nsettings;

    void swapQuery(RandomGenerator & rg, google::protobuf::Message & mes);
    bool findTablesWithPeersAndReplace(RandomGenerator & rg, google::protobuf::Message & mes, StatementGenerator & gen, bool replace);
    void addLimitOrOffset(RandomGenerator & rg, StatementGenerator & gen, SelectStatementCore * ssc) const;
    void insertOnTableOrCluster(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, bool peer, TableOrFunction * tof) const;
    void generateExportQuery(RandomGenerator & rg, StatementGenerator & gen, bool test_content, const SQLTable & t, SQLQuery & sq2);
    void
    generateImportQuery(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4) const;

public:
    explicit QueryOracle(const FuzzConfig & ffc)
        : fc(ffc)
        , qcfile(ffc.client_file_path / "query.data")
        , qsfile(ffc.server_file_path / "query.data")
        , qfile_peer(
              ffc.clickhouse_server.has_value() ? (ffc.clickhouse_server.value().user_files_dir / "peer.data")
                                                : std::filesystem::temp_directory_path())
        , can_test_oracle_result(fc.compare_success_results)
        , measure_performance(fc.measure_performance)
    {
    }

    void resetOracleValues();
    void setIntermediateStepSuccess(bool success);
    void processFirstOracleQueryResult(int errcode, ExternalIntegrations & ei);
    void processSecondOracleQueryResult(int errcode, ExternalIntegrations & ei, const String & oracle_name);

    /// Correctness query oracle
    void generateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq);
    void generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2);

    /// Dump and read table oracle
    void dumpTableContent(RandomGenerator & rg, StatementGenerator & gen, bool test_content, const SQLTable & t, SQLQuery & sq1);
    void dumpOracleIntermediateSteps(
        RandomGenerator & rg,
        StatementGenerator & gen,
        const SQLTable & t,
        DumpOracleStrategy strategy,
        bool test_content,
        std::vector<SQLQuery> & intermediate_queries);

    /// Run query with different settings oracle
    bool generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1);
    void generateOracleSelectQuery(RandomGenerator & rg, PeerQuery pq, StatementGenerator & gen, SQLQuery & sq2);
    void generateSecondSetting(RandomGenerator & rg, StatementGenerator & gen, bool use_settings, const SQLQuery & sq1, SQLQuery & sq3);
    void maybeUpdateOracleSelectQuery(RandomGenerator & rg, const SQLQuery & sq1, SQLQuery & sq2);

    /// Replace query with peer tables
    void truncatePeerTables(const StatementGenerator & gen);
    void optimizePeerTables(const StatementGenerator & gen);
    void replaceQueryWithTablePeers(
        RandomGenerator & rg, const SQLQuery & sq1, StatementGenerator & gen, std::vector<SQLQuery> & peer_queries, SQLQuery & sq2);
};

}
