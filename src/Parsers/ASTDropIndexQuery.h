#pragma once

#include <optional>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>


namespace DB
{

/** DROP INDEX [IF EXISTS] name on [db].name
 */

class ASTDropIndexQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:

    ASTPtr index_name;

    bool if_exists{false};

    String getID(char delim) const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTDropIndexQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

    /// Convert ASTDropIndexQuery to ASTAlterCommand
    ASTPtr convertToASTAlterCommand() const;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
