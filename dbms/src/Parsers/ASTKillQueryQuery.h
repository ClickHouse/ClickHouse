#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTKillQueryQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    ASTPtr where_expression;    // expression to filter processes from system.processes table
    bool sync = false;          // SYNC or ASYNC mode
    bool test = false;          // does it TEST mode? (doesn't cancel queries just checks and shows them)

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTKillQueryQuery>(*this);
        clone->where_expression = where_expression->clone();
        clone->children = {clone->where_expression};
        return std::move(clone);
    }

    String getID() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &new_database) const override;
};

}
