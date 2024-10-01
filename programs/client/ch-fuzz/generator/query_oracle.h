#pragma once

#include "../third_party/md5.h"
#include "fuzz_config.h"
#include "random_generator.h"
#include "statement_generator.h"

namespace chfuzz {

class QueryOracle {
private:
	const FuzzConfig &fc;
	MD5 md5_hash;
	bool first_success = false, second_sucess = false;
	uint8_t first_digest[16], second_digest[16];
	std::string buf;
	std::vector<std::string> nsettings;
public:
	QueryOracle(const FuzzConfig &ffc) : fc(ffc) {
		buf.reserve(4096);
	}

	int ProcessOracleQueryResult(const bool first, const bool success, const std::string &oracle_name);

	/* Correctness query oracle */
	int GenerateCorrectnessTestFirstQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq);
	int GenerateCorrectnessTestSecondQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2);

	/* Dump and read table oracle */
	int DumpTableContent(const SQLTable &t, sql_query_grammar::SQLQuery &sq1);
	int GenerateExportQuery(RandomGenerator &rg, const SQLTable &t, sql_query_grammar::SQLQuery &sq2);
	int GenerateClearQuery(const SQLTable &t, sql_query_grammar::SQLQuery &sq3);
	int GenerateImportQuery(const SQLTable &t, const sql_query_grammar::SQLQuery &sq2, sql_query_grammar::SQLQuery &sq4);

	/* Run query with different settings oracle */
	int GenerateFirstSetting(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq1);
	int GenerateSettingQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq2);
	int GenerateSecondSetting(const sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq3);
};

}
