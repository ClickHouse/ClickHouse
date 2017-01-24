#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>

namespace DB
{

class ASTKillQueryQuery : public ASTQueryWithOutput
{
public:
	ASTPtr where_expression;
	bool sync;

	ASTKillQueryQuery() = default;

	ASTKillQueryQuery(const StringRange range_) : ASTQueryWithOutput(range_) {}

	ASTPtr clone() const override { return std::make_shared<ASTKillQueryQuery>(*this); }

	String getID() const override;

	void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
