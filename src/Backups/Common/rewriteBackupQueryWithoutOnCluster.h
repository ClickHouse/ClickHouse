#pragma once

#include <memory>


namespace DB
{
class ASTBackupQuery;
struct WithoutOnClusterASTRewriteParams;

/// Rewrites elements of BACKUP-ON-CLUSTER query after receiving it on shards or replica.
std::shared_ptr<ASTBackupQuery>
rewriteBackupQueryWithoutOnCluster(const ASTBackupQuery & backup_query, const WithoutOnClusterASTRewriteParams & params);

/// Rewrites elements of RESTORE-ON-CLUSTER query after receiving it on shards or replica.
std::shared_ptr<ASTBackupQuery>
rewriteRestoreQueryWithoutOnCluster(const ASTBackupQuery & restore_query, const WithoutOnClusterASTRewriteParams & params);

}
