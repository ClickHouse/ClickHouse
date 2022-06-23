#pragma once

#include <Parsers/IParserBase.h>
#include <Access/RowPolicy.h>


namespace DB
{
/** Parses a string in one of the following form:
  * short_name ON [db.]table_name [ON CLUSTER 'cluster_name']
  * short_name [ON CLUSTER 'cluster_name'] ON [db.]table_name
  */
class ParserRowPolicyName : public IParserBase
{
public:
    void allowOnCluster(bool allow = true) { allow_on_cluster = allow; }

protected:
    const char * getName() const override { return "RowPolicyName"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_on_cluster = false;
};


/** Parses a string in one of the following form:
  * short_name1 ON [db1.]table_name1 [, short_name2 ON [db2.]table_name2 ...] [ON CLUSTER 'cluster_name']
  * short_name1 [, short_name2 ...] ON [db.]table_name [ON CLUSTER 'cluster_name']
  * short_name1 [, short_name2 ...] [ON CLUSTER 'cluster_name'] ON [db.]table_name
  * short_name ON [db1.]table_name1 [, [db2.]table_name2 ...] [ON CLUSTER 'cluster_name']
  * short_name [ON CLUSTER 'cluster_name'] ON [db1.]table_name1 [, [db2.]table_name2 ...]
  */
class ParserRowPolicyNames : public IParserBase
{
public:
    void allowOnCluster(bool allow = true) { allow_on_cluster = allow; }

protected:
    const char * getName() const override { return "SettingsProfileElements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_on_cluster = false;
};

}
