#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{

/** CREATE TABLE or ATTACH TABLE query.
  */
class ASTCreateQuery : public IAST
{
public:
	bool attach{false};	/// If true then it means what query is ATTACH TABLE.
	bool if_not_exists{false};
	bool is_view{false};
	bool is_materialized_view{false};
	bool is_populate{false};
	bool is_temporary{false};
	String database;
	String table;
	String cluster;
	ASTPtr columns;
	ASTPtr storage;
	ASTPtr inner_storage;	/// Inner engine for CREATE MATERIALIZED VIEW query.
	String as_database;
	String as_table;
	ASTPtr select;

	ASTCreateQuery() = default;
	ASTCreateQuery(const StringRange range_) : IAST(range_) {}

	/// Returns text which identifies this element.
	String getID() const override;

	ASTPtr clone() const override;

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
