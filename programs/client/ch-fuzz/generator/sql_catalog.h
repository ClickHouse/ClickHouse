#pragma once

#include "sql_types.h"

namespace chfuzz {

using ColumnSpecial = enum ColumnSpecial {
	NONE = 0,
	SIGN = 1,
	VERSION = 2
};

struct SQLColumn {
public:
	bool nullable = true;
	ColumnSpecial special;
	uint32_t cname;
	SQLType *tp;
};

struct SQLIndex {
public:
	uint32_t iname;
};

struct SQLBase {
public:
	sql_query_grammar::TableEngineValues teng;

	bool IsMergeTreeFamily() const {
		return teng >= sql_query_grammar::TableEngineValues::MergeTree &&
			   teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree;
	}
};

struct SQLTable : SQLBase {
public:
	bool is_temp = false;
	uint32_t tname, col_counter = 0, idx_counter = 0, proj_counter = 0, constr_counter = 0;
	std::map<uint32_t, SQLColumn> cols, staged_cols;
	std::map<uint32_t, SQLIndex> idxs, staged_idxs;
	std::set<uint32_t> projs, staged_projs, constrs, staged_constrs;

	size_t RealNumberOfColumns() const {
		size_t res = 0;
		NestedType *ntp = nullptr;

		for (const auto &entry : cols) {
			if ((ntp = dynamic_cast<NestedType*>(entry.second.tp))) {
				res += ntp->subtypes.size();
			} else {
				res++;
			}
		}
		return res;
	}

	bool SupportsFinal() const {
		return teng != sql_query_grammar::TableEngineValues::Memory &&
			   teng != sql_query_grammar::TableEngineValues::MergeTree;
	}

	bool HasSignColumn() const {
		return teng >= sql_query_grammar::TableEngineValues::CollapsingMergeTree &&
			   teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree;
	}

	bool HasVersionColumn() const {
		return teng == sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree;
	}
};

struct SQLView : SQLBase {
public:
	bool is_materialized = false;
	uint32_t vname, ncols = 1, staged_ncols;
};

}
