#pragma once

#include <Client/BuzzHouse/Generator/RandomGenerator.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Client/BuzzHouse/Utils/MD5Impl.h>

namespace BuzzHouse
{

class QueryOracle
{
private:
    const FuzzConfig & fc;
    const std::filesystem::path qfile;
    MD5Impl md5_hash;
    PeerQuery peer_query = PeerQuery::AllPeers;
    bool first_success = true, second_sucess = true, other_steps_sucess = true, can_test_query_success = true;
    uint8_t first_digest[16], second_digest[16];
    std::string buf;
    std::set<uint32_t> found_tables;
    std::vector<std::string> nsettings;

    void findTablesWithPeersAndReplace(RandomGenerator & rg, google::protobuf::Message & mes, StatementGenerator & gen);

public:
    explicit QueryOracle(const FuzzConfig & ffc) : fc(ffc), qfile(ffc.db_file_path / "query.data") { buf.reserve(4096); }

    void resetOracleValues();
    void setIntermediateStepSuccess(bool success);
    void processFirstOracleQueryResult(bool success);
    void processSecondOracleQueryResult(bool success, const std::string & oracle_name);

    /* Correctness query oracle */
    void generateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq);
    void generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2);

    /* Dump and read table oracle */
    void dumpTableContent(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq1);
    void generateExportQuery(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq2);
    void generateClearQuery(const SQLTable & t, SQLQuery & sq3);
    void generateImportQuery(StatementGenerator & gen, const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4);

    /* Run query with different settings oracle */
    void generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1);
    void generateOracleSelectQuery(RandomGenerator & rg, PeerQuery pq, StatementGenerator & gen, SQLQuery & sq2);
    void generateSecondSetting(const SQLQuery & sq1, SQLQuery & sq3);

    /* Replace query with peer tables */
    void truncatePeerTables(const StatementGenerator & gen) const;
    void optimizePeerTables(const StatementGenerator & gen) const;
    void replaceQueryWithTablePeers(
        RandomGenerator & rg, const SQLQuery & sq1, StatementGenerator & gen, std::vector<SQLQuery> & peer_queries, SQLQuery & sq2);
};

void loadFuzzerOracleSettings(const FuzzConfig & fc);

}
