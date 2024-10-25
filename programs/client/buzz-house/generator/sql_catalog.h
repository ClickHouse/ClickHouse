#pragma once

#include "sql_types.h"

namespace buzzhouse {

using ColumnSpecial = enum ColumnSpecial {
	NONE = 0,
	SIGN = 1,
	IS_DELETED = 2,
	VERSION = 3
};

struct SQLColumn {
public:
	uint32_t cname = 0;
	const SQLType *tp = nullptr;
	ColumnSpecial special = ColumnSpecial::NONE;
	std::optional<bool> nullable = std::nullopt;
	std::optional<sql_query_grammar::DModifier> dmod = std::nullopt;

	SQLColumn() = default;
	SQLColumn(const SQLColumn& c) {
		this->cname = c.cname;
		this->special = c.special;
		this->nullable = c.nullable;
		this->dmod = c.dmod;
		this->tp = TypeDeepCopy(c.tp);
	}
	SQLColumn(SQLColumn&& c) {
		this->cname = c.cname;
		this->special = c.special;
		this->nullable = c.nullable;
		this->dmod = c.dmod;
		this->tp = c.tp;
		c.tp = nullptr;
	}
	SQLColumn& operator=(const SQLColumn& c) {
		this->cname = c.cname;
		this->special = c.special;
		this->nullable = c.nullable;
		this->dmod = c.dmod;
		this->tp = TypeDeepCopy(c.tp);
		return *this;
	}
	SQLColumn& operator=(SQLColumn&& c) {
		this->cname = c.cname;
		this->special = c.special;
		this->nullable = c.nullable;
		this->dmod = std::optional<sql_query_grammar::DModifier>(c.dmod);
		this->tp = c.tp;
		c.tp = nullptr;
		return *this;
	}
	~SQLColumn() {
		delete tp;
	}

	bool CanBeInserted() const {
		return !dmod.has_value() || dmod.value() == sql_query_grammar::DModifier::DEF_DEFAULT;
	}
};

struct SQLIndex {
public:
	uint32_t iname = 0;
};

struct SQLDatabase {
public:
	bool attached = true;
	uint32_t dname = 0;
	sql_query_grammar::DatabaseEngineValues deng;
};

struct SQLBase {
public:
	std::shared_ptr<SQLDatabase> db = nullptr;
	bool attached = true;
	std::optional<sql_query_grammar::TableEngineOption> toption;
	sql_query_grammar::TableEngineValues teng;

	bool IsMergeTreeFamily() const {
		return teng >= sql_query_grammar::TableEngineValues::MergeTree &&
			   teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree;
	}

	bool IsFileEngine() const {
		return teng == sql_query_grammar::TableEngineValues::File;
	}

	bool IsJoinEngine() const {
		return teng == sql_query_grammar::TableEngineValues::Join;
	}

	bool IsBufferEngine() const {
		return teng == sql_query_grammar::TableEngineValues::Buffer;
	}
};

struct SQLTable : SQLBase {
public:
	bool is_temp = false;
	uint32_t tname = 0, col_counter = 0, idx_counter = 0, proj_counter = 0, constr_counter = 0;
	std::map<uint32_t, SQLColumn> cols, staged_cols;
	std::map<uint32_t, SQLIndex> idxs, staged_idxs;
	std::set<uint32_t> projs, staged_projs, constrs, staged_constrs;

	size_t RealNumberOfColumns() const {
		size_t res = 0;
		const NestedType *ntp = nullptr;

		for (const auto &entry : cols) {
			if ((ntp = dynamic_cast<const NestedType*>(entry.second.tp))) {
				res += ntp->subtypes.size();
			} else {
				res++;
			}
		}
		return res;
	}

	bool SupportsFinal() const {
		return (teng >= sql_query_grammar::TableEngineValues::ReplacingMergeTree &&
			    teng <= sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree) || this->IsBufferEngine();
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
	bool is_materialized = false, is_refreshable = false;
	uint32_t vname = 0, ncols = 1, staged_ncols = 1;
};

struct SQLFunction {
public:
	bool not_deterministic = false;
	uint32_t fname = 0, nargs = 0;
};

}
