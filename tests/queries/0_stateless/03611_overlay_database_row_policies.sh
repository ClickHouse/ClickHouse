#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading a table through a read-only Overlay facade applies the row policies of *both* the facade
# and the underlying source table (a row must pass both). This must hold in both analyzers.

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_A="db_a_${SUF}"
DB_OVL="dboverlay_${SUF}"
USER="u_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP USER IF EXISTS ${USER};

    CREATE DATABASE ${DB_A} ENGINE = Atomic;
    CREATE TABLE ${DB_A}.t (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_A}.t VALUES (1), (2), (3), (4);

    CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_A}');

    CREATE USER ${USER} NOT IDENTIFIED;
    GRANT SELECT ON ${DB_OVL}.* TO ${USER};
    GRANT SELECT ON ${DB_A}.* TO ${USER};

    -- A row policy defined on the Overlay facade.
    CREATE ROW POLICY p_ovl_${SUF} ON ${DB_OVL}.t AS permissive FOR SELECT USING id <= 2 TO ${USER};
"

read_overlay() { ${CLICKHOUSE_CLIENT} --user="${USER}" --query "SELECT id FROM ${DB_OVL}.t ORDER BY id SETTINGS enable_analyzer=$1" | xargs; }
read_source()  { ${CLICKHOUSE_CLIENT} --user="${USER}" --query "SELECT id FROM ${DB_A}.t ORDER BY id SETTINGS enable_analyzer=$1" | xargs; }

for a in 1 0; do
    echo "analyzer=$a: facade policy applies through the facade (expect 1 2)"
    read_overlay "$a"
    echo "analyzer=$a: facade policy does not affect direct reads of the source (expect 1 2 3 4)"
    read_source "$a"
done

# Now add a policy on the underlying source too: reading through the facade must satisfy both.
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE ROW POLICY p_src_${SUF} ON ${DB_A}.t AS permissive FOR SELECT USING id >= 2 TO ${USER};
"

for a in 1 0; do
    echo "analyzer=$a: through the facade both policies apply, id<=2 AND id>=2 (expect 2)"
    read_overlay "$a"
    echo "analyzer=$a: direct reads of the source apply only the source policy, id>=2 (expect 2 3 4)"
    read_source "$a"
done

${CLICKHOUSE_CLIENT} -nm --query "
    DROP ROW POLICY IF EXISTS p_ovl_${SUF} ON ${DB_OVL}.t;
    DROP ROW POLICY IF EXISTS p_src_${SUF} ON ${DB_A}.t;
    DROP DATABASE IF EXISTS ${DB_OVL};
    DROP DATABASE IF EXISTS ${DB_A};
    DROP USER IF EXISTS ${USER};
"
