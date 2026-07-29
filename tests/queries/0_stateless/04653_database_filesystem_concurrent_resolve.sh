#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
user_files_tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${user_files_tmp_dir}"/nested/

# Several files, all with the same schema, some nested: `**/*.csv` then has real globbing and schema
# inference to do, which is what widens the race window in `getTableImpl`. Keeping one schema across
# every file is deliberate, so that reading the glob is deterministic under format randomization.
printf '"id","str"\n1,"a"\n2,"b"\n' > "${user_files_tmp_dir}"/one.csv
printf '"id","str"\n3,"c"\n4,"d"\n5,"e"\n' > "${user_files_tmp_dir}"/two.csv
printf '"id","str"\n6,"f"\n' > "${user_files_tmp_dir}"/nested/three.csv
printf '"id","str"\n7,"g"\n8,"h"\n9,"i"\n10,"j"\n' > "${user_files_tmp_dir}"/nested/four.csv

DB="${CLICKHOUSE_DATABASE}_fs"
${CLICKHOUSE_CLIENT} --multiline -q "
DROP DATABASE IF EXISTS ${DB};
CREATE DATABASE ${DB} ENGINE = Filesystem;
"

# Resolving one table name concurrently races the cache probe against the cache insert in
# `DatabaseFilesystem::getTableImpl`. The loser of that race used to reach a throw whose message
# re-locked the already-held non-recursive `IDatabase::mutex`, wedging the database forever.
#
# The oracle is the deadlock itself: that all 16 clients return at all, and that the database is
# still usable afterwards. Which of the racing storages a loser receives is deliberately not
# asserted, because a `Filesystem` database exposes no table cache to SQL - `loaded_tables` has no
# reference outside the engine and `getTablesIterator` returns an empty snapshot, so winner and
# loser are indistinguishable from a query. The glob name is chosen for the width of the race
# window, not for cache residency: `tryGetTableFromCache` invalidates an entry whenever
# `fs::exists` fails on the table path, and that path is the literal glob pattern, so a glob
# entry is dropped again on every lookup.
TABLE="${unique_name}/**/*.csv"
QUERY="SELECT count(DISTINCT _path) FROM ${DB}.\`${TABLE}\` SETTINGS optimize_count_from_files = 1"

out="${CLICKHOUSE_TMP}/${unique_name}_04653_out.txt"
: > "${out}"
for _ in {1..16}; do
    ${CLICKHOUSE_CLIENT} --query "${QUERY}" >> "${out}" 2>&1 &
done
wait

# Every query must have returned the same count. A wedged database shows up as missing lines:
# the clients never return and `clickhouse-test` kills the test on its own timeout.
echo "distinct results:"
sort -u "${out}"
echo "result count: $(grep -c . "${out}")"

# The database must still be usable. With `mutex` held forever, this query never returns.
echo "still usable: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB}.\`${TABLE}\`")"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
rm -f "${out}"
rm -rd "${user_files_tmp_dir}"
