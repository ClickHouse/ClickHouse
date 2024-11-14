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

    int ProcessOracleQueryResult(const bool first, const bool success, const std::string & oracle_name);

    /* Correctness query oracle */
    int GenerateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq);
    int GenerateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2);

    /* Dump and read table oracle */
    int DumpTableContent(RandomGenerator & rg, const SQLTable & t, SQLQuery & sq1);
    int GenerateExportQuery(RandomGenerator & rg, const SQLTable & t, SQLQuery & sq2);
    int GenerateClearQuery(const SQLTable & t, SQLQuery & sq3);
    int GenerateImportQuery(const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4);

    /* Run query with different settings oracle */
    int GenerateFirstSetting(RandomGenerator & rg, SQLQuery & sq1);
    int GenerateSettingQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq2);
    int GenerateSecondSetting(const SQLQuery & sq1, SQLQuery & sq3);
};

}
