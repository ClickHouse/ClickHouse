#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// UPDATE [db.]name SET column1 = value1, column2 = value2, ... WHERE ...
class ASTUpdateQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    String getID(char delim) const final;
    ASTPtr clone() const final;
    QueryKind getQueryKind() const override { return QueryKind::Update; }
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTUpdateQuery>(clone(), params.default_database);
    }

    ASTPtr predicate;
    ASTPtr assignments;
    ASTPtr partition;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
