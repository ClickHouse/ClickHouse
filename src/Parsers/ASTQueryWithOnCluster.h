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
    ASTQueryWithOnCluster() = default;
    ASTQueryWithOnCluster(const ASTQueryWithOnCluster &) = default;
    ASTQueryWithOnCluster & operator=(const ASTQueryWithOnCluster &) = default;

protected:
    template <typename T>
    static ASTPtr removeOnCluster(ASTPtr query_ptr, const std::string & new_database)
    {
        T & query = static_cast<T &>(*query_ptr);

        query.cluster.clear();
        if (query.database.empty())
            query.database = new_database;

        return query_ptr;
    }

    template <typename T>
    static ASTPtr removeOnCluster(ASTPtr query_ptr)
    {
        T & query = static_cast<T &>(*query_ptr);
        query.cluster.clear();
        return query_ptr;
    }
};

}
