#pragma once

#include <Parsers/IAST.h>
#include <Access/RowPolicy.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/** DROP USER [IF EXISTS] name [,...]
  * DROP ROLE [IF EXISTS] name [,...]
  * DROP QUOTA [IF EXISTS] name [,...]
  * DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...]
  * DROP [SETTINGS] PROFILE [IF EXISTS] name [,...]
  */
class ASTDropAccessEntityQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;
    bool if_exists = false;
    Strings names;
    std::vector<RowPolicy::NameParts> row_policies_name_parts;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropAccessEntityQuery>(clone()); }
};
}
