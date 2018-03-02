#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

class ASTKillQueryQuery : public ASTQueryWithOutput
{
public:
    ASTPtr where_expression;    // expression to filter processes from system.processes table
    bool sync = false;          // SYNC or ASYNC mode
    bool test = false;          // does it TEST mode? (doesn't cancel queries just checks and shows them)

    ASTPtr clone() const override { return std::make_shared<ASTKillQueryQuery>(*this); }

    String getID() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
