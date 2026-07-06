#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParser.h>

namespace DB
{

/// Parameters for rewriting queries without ON CLUSTER, see getRewrittenASTWithoutOnCluster().
struct WithoutOnClusterASTRewriteParams
{
    /// Default database from the cluster's configuration.
    String default_database;

    /// The ID of the current host in the format.
    String host_id;
};


/// TODO: Quite messy.
class ASTQueryWithOnCluster
{
public:
    using Pos = IParser::Pos;

    /// Should be parsed from ON CLUSTER <cluster> clause
    String cluster;

    /// True when the query was written as `ON CLUSTER` without an explicit cluster name.
    /// In this case `cluster` is filled from the `default_cluster` setting during query execution.
    bool use_default_cluster = false;

    /// new_database should be used by queries that refer to default db
    ///  and default_database is specified for remote server
    virtual ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params = {}) const = 0; /// NOLINT

    /// Returns a query prepared for execution on remote server
    std::string getRewrittenQueryWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params = {}) const;

    void formatOnCluster(WriteBuffer & ostr, const IAST::FormatSettings & settings) const;

    /// Parses " CLUSTER [cluster|'cluster'] " clause.
    /// If `out_use_default_cluster` is not null, the cluster name is allowed to be omitted;
    /// in that case `*out_use_default_cluster` is set to true and the name is expected to be
    /// resolved later from the `default_cluster` setting. Otherwise a missing name is a parse error.
    static bool parse(Pos & pos, std::string & cluster_str, Expected & expected, bool * out_use_default_cluster = nullptr);

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
        query.use_default_cluster = false;
        if (!query.database)
            query.setDatabase(new_database);

        return query_ptr;
    }

    template <typename T>
    static ASTPtr removeOnCluster(ASTPtr query_ptr)
    {
        T & query = static_cast<T &>(*query_ptr);
        query.cluster.clear();
        query.use_default_cluster = false;
        return query_ptr;
    }
};

}
