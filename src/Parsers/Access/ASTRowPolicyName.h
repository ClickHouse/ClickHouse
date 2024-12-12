#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/Common/RowPolicyDefs.h>


namespace DB
{
/** Represents a row policy's name in one of the following forms:
  * short_name ON [db.]table_name [ON CLUSTER 'cluster_name']
  * short_name [ON CLUSTER 'cluster_name'] ON [db.]table_name
  */
class ASTRowPolicyName : public IAST, public ASTQueryWithOnCluster
{
public:
    RowPolicyName full_name;
    String toString() const { return full_name.toString(); }

    String getID(char) const override { return "RowPolicyName"; }
    ASTPtr clone() const override { return std::make_shared<ASTRowPolicyName>(*this); }
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTRowPolicyName>(clone()); }

    void replaceEmptyDatabase(const String & current_database);
};


/** Represents multiple names of row policies, comma-separated, in one of the following forms:
  * short_name1 ON [db1.]table_name1 [, short_name2 ON [db2.]table_name2 ...] [ON CLUSTER 'cluster_name']
  * short_name1 [, short_name2 ...] ON [db.]table_name [ON CLUSTER 'cluster_name']
  * short_name1 [, short_name2 ...] [ON CLUSTER 'cluster_name'] ON [db.]table_name
  * short_name ON [db1.]table_name1 [, [db2.]table_name2 ...] [ON CLUSTER 'cluster_name']
  * short_name [ON CLUSTER 'cluster_name'] ON [db1.]table_name1 [, [db2.]table_name2 ...]
  */
class ASTRowPolicyNames : public IAST, public ASTQueryWithOnCluster
{
public:
    std::vector<RowPolicyName> full_names;
    Strings toStrings() const;

    String getID(char) const override { return "RowPolicyNames"; }
    ASTPtr clone() const override { return std::make_shared<ASTRowPolicyNames>(*this); }
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTRowPolicyNames>(clone()); }

    void replaceEmptyDatabase(const String & current_database);

private:
    String tableOrAsterisk(const String & table_name) const;
};
}
