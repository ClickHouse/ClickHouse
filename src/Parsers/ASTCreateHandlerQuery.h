#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTCreateHandlerQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string handler_name;
    std::string url;
    std::string url_type; /// "exact", "prefix", or "regexp"
    std::vector<std::string> methods; /// e.g. {"GET", "POST"}
    std::string query; /// The SQL query to execute
    bool if_not_exists = false;

    String getID(char) const override { return "CreateHandlerQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateHandlerQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
