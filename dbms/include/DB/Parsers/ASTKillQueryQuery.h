#include <DB/Parsers/IAST.h>

namespace DB
{

class ASTKillQueryQuery : public IAST
{
public:
	ASTPtr where_expression;
	bool sync;

	ASTKillQueryQuery() = default;

	ASTKillQueryQuery(const StringRange range_) : IAST(range_) {}

	ASTPtr clone() const override { return std::make_shared<ASTKillQueryQuery>(*this); }

	String getID() const override
	{
		return "KillQueryQuery_" + (where_expression ? where_expression->getID() : "") + "_" + String(sync ? "SYNC" : "ASYNC");
	}

	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		settings.ostr << "KILL QUERY WHERE ";

		if (where_expression)
			where_expression->formatImpl(settings, state, frame);

		settings.ostr << (sync ? " SYNC" : " ASYNC");
	}
};

}
