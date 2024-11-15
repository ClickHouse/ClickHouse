#pragma once

#include "MD5Impl.h"
#include "RandomGenerator.h"
#include "StatementGenerator.h"

namespace BuzzHouse
{

class QueryOracle
{
private:
    const FuzzConfig & fc;
    MD5Impl md5_hash;
    bool first_success = false, second_sucess = false;
    uint8_t first_digest[16], second_digest[16];
    std::string buf;
    std::vector<std::string> nsettings;

public:
    QueryOracle(const FuzzConfig & ffc) : fc(ffc) { buf.reserve(4096); }

    int processOracleQueryResult(const bool first, const bool success, const std::string & oracle_name);

    /* Correctness query oracle */
    int generateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq);
    int generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2);

    /* Dump and read table oracle */
    int dumpTableContent(RandomGenerator & rg, const SQLTable & t, SQLQuery & sq1);
    int generateExportQuery(RandomGenerator & rg, const SQLTable & t, SQLQuery & sq2);
    int generateClearQuery(const SQLTable & t, SQLQuery & sq3);
    int generateImportQuery(const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4);

    /* Run query with different settings oracle */
    int generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1);
    int generateSettingQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq2);
    int generateSecondSetting(const SQLQuery & sq1, SQLQuery & sq3);
};

}
