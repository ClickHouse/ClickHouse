#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires mysql client

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Some BI tools which connect to ClickHouse's MySQL port, run queries which succeed only with (the analyzer enabled)
# or (without analyzer and setting prefer_column_name_to_alias = 1). Since the setting is too impactful to enable it
# globally, it is enabled only by the MySQL handler internally as a workaround. Run a query from Bug 56173 to verify.
#
# When the analyzer is the new default, the test and the workaround can be deleted.
${MYSQL_CLIENT} --execute "select a + b as b, count() from (select 1 as a, 1 as b) group by a + b";
