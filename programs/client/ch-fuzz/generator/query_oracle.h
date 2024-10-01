#pragma once

#include <string>

#include "fuzz_config.h"
#include "random_generator.h"
#include "statement_generator.h"

namespace chfuzz {

class QueryOracle {
private:
	std::string buf, nsetting;
	const FuzzConfig &fc;
public:
	QueryOracle(const FuzzConfig &ffc) : fc(ffc) {
		buf.reserve(4096);
		nsetting.reserve(16);
	}

	int GenerateCorrectnessTestFirstQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq);
	int GenerateCorrectnessTestSecondQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2);

	int GenerateExportQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq1);
	int GenerateClearQuery(sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2);
	int GenerateImportQuery(StatementGenerator &gen, sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq2, sql_query_grammar::SQLQuery &sq3);

	int GenerateFirstSetting(RandomGenerator &rg, sql_query_grammar::SQLQuery &sq1);
	int GenerateFirtSettingQuery(RandomGenerator &rg, StatementGenerator &gen, sql_query_grammar::SQLQuery &sq2);
	int GenerateSecondSetting(const sql_query_grammar::SQLQuery &sq1, sql_query_grammar::SQLQuery &sq3);
};

}
