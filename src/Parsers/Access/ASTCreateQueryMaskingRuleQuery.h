#pragma once

#include <optional>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/** CREATE QUERY MASKING RULE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster] [IN access_storage]
  *      REGEXP 'regexp' [REPLACE 'replacement']
  *
  * ALTER QUERY MASKING RULE [IF EXISTS] name [ON CLUSTER cluster]
  *      [RENAME TO new_name] [REGEXP 'regexp'] [REPLACE 'replacement']
  */
class ASTCreateQueryMaskingRuleQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;
    String storage_name;

    std::optional<String> regexp;
    std::optional<String> replace;

    String getID(char) const override;
    ASTPtr clone() const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateQueryMaskingRuleQuery>(clone()); }

    QueryKind getQueryKind() const override { return alter ? QueryKind::Alter : QueryKind::Create; }

    bool hasSecretParts() const override { return regexp.has_value(); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
