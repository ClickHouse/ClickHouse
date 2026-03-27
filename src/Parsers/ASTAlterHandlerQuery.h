#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/HandlerURLType.h>


namespace DB
{

class ASTAlterHandlerQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    std::string handler_name;
    std::optional<std::string> url;
    std::optional<HandlerURLType> url_type;
    std::optional<std::vector<std::string>> methods;
    std::optional<std::string> query;
    bool if_exists = false;

    String getID(char) const override { return "AlterHandlerQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTAlterHandlerQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
