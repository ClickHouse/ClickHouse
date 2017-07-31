#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParser.h>

namespace DB
{

/// TODO: Quite messy.
class ASTQueryWithOnCluster
{
public:
    using Pos = IParser::Pos;

    /// Should be parsed from ON CLUSTER <cluster> clause
    String cluster;

    /// new_database should be used by queries that refer to default db
    ///  and default_database is specified for remote server
    virtual ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database = {}) const = 0;

    /// Returns a query prepared for execution on remote server
    std::string getRewrittenQueryWithoutOnCluster(const std::string & new_database = {}) const;

    void formatOnCluster(const IAST::FormatSettings & settings) const;

    /// Parses " CLUSTER [cluster|'cluster'] " clause
    static bool parse(Pos & pos, std::string & cluster_str, Expected & expected);

    virtual ~ASTQueryWithOnCluster() = default;
};

}
