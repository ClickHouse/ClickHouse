#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Core/Types.h>
#include <mysqlxx/Manip.h>

namespace DB
{

class ASTWeightedZooKeeperPath : public IAST
{
public:
	ASTWeightedZooKeeperPath() = default;
	ASTWeightedZooKeeperPath(StringRange range_) : IAST(range_) {}
	String getID() const override { return "Weighted_ZooKeeper_Path"; }
	ASTPtr clone() const override { return new ASTWeightedZooKeeperPath(*this); }

public:
	String path;
	UInt64 weight;

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
		settings.ostr << settings.nl_or_ws << indent_str << mysqlxx::quote << path << " WEIGHT " << weight;
	}
};

}
