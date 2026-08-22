#!/usr/bin/env bash

# A persisted `remote()` target that names this server by its own host name - the spelling `hostName`
# returns, which is also how a cluster definition usually spells a server - runs locally, so the persisted
# table depends on the local table the function reads. `DDLDependencyVisitor` recognizes that spelling
# without resolving anything: the name this server reports for itself is known locally, unlike the address
# of an arbitrary remote host, which the dependency analysis must not resolve (it is re-run from metadata at
# server startup). Any other host keeps the long-standing non-local assumption and records nothing, which
# `04824_distributed_over_local_remote_dependencies` covers along with the address-literal spellings.
# This test is a shell one because the host name of the server has to be substituted into the queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

THIS_HOST=$(${CLICKHOUSE_CLIENT} --query "SELECT hostName()")
# The leading label of the name, e.g. `clickhouse01` of `clickhouse01.example.com`; a server named by a
# fully qualified domain name is commonly addressed by the short form as well.
SHORT_HOST=${THIS_HOST%%.*}

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dep_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO dep_src VALUES (1), (2), (3);

CREATE TABLE dist_host_name ENGINE = Distributed(test_shard_localhost, remote('${THIS_HOST}', currentDatabase(), 'dep_src'));
CREATE TABLE dist_short_host_name ENGINE = Distributed(test_shard_localhost, remote('${SHORT_HOST}', currentDatabase(), 'dep_src'));
CREATE TABLE direct_host_name AS remote('${THIS_HOST}', currentDatabase(), 'dep_src');

SELECT sum(n) FROM dist_host_name;
"

# Dropping the dependent tables one by one shows that each of the local spellings holds its own referential
# dependency on the source table, and so does renaming it; once all of them are gone, it can be dropped.
function expect_dependent_objects()
{
    if ${CLICKHOUSE_CLIENT} --check_referential_table_dependencies 1 --query "$1" 2>&1 | grep -q -F 'HAVE_DEPENDENT_OBJECTS'
    then
        echo 'HAVE_DEPENDENT_OBJECTS'
    else
        echo 'the source table is not protected'
    fi
}

expect_dependent_objects "RENAME TABLE dep_src TO dep_src_renamed"
expect_dependent_objects "DROP TABLE dep_src"
${CLICKHOUSE_CLIENT} --query "DROP TABLE dist_host_name"
expect_dependent_objects "DROP TABLE dep_src"
${CLICKHOUSE_CLIENT} --query "DROP TABLE dist_short_host_name"
expect_dependent_objects "DROP TABLE dep_src"
${CLICKHOUSE_CLIENT} --check_referential_table_dependencies 1 --query "
DROP TABLE direct_host_name;
DROP TABLE dep_src;
"
