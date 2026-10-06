#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `write_buffer_valid_utf8_finalize_throw` fail point, which affects
# the whole server (it fires on the next `WriteBufferValidUTF8` flush anywhere), so a concurrent
# query from another test could consume the injected fault.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The helpers that render column names through a scoped `WriteBufferValidUTF8`
# (`JSONUtils::makeNamesValidJSONStrings` and the BSON name sanitizer) used to rely on the buffer's
# destructor for the final flush. That destructor catches and suppresses a failure, so the
# destination string stayed empty and the `substr` that strips the surrounding quotes threw
# `std::out_of_range` - reported as `Logical error: 'std::exception. Code: 1001, type:
# std::out_of_range` (an exception, and an abort under a sanitizer build) instead of the real
# error. BuzzHouse hit exactly this by enabling the fail point at random. With the explicit flush,
# the failure propagates as itself. The formats are exercised over HTTP because the fail point is
# armed in the server, and only there does the server render the output format.

echo '--- an injected flush failure propagates as itself while the JSON names are rendered'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT 1 AS x FORMAT JSONCompact")
echo "$RESPONSE" | grep -q -F 'FAULT_INJECTED' && echo 'the real error is reported: OK' || echo "MISMATCH: $RESPONSE"
echo "$RESPONSE" | grep -q -i 'logical error\|out_of_range' && echo "MISMATCH: the failure degraded into a logical error: $RESPONSE" || echo 'no out-of-range error: OK'

echo '--- the same for the BSON name sanitizer'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT 1 AS x FORMAT BSONEachRow")
echo "$RESPONSE" | grep -q -F 'FAULT_INJECTED' && echo 'the real error is reported: OK' || echo "MISMATCH: $RESPONSE"
echo "$RESPONSE" | grep -q -i 'logical error\|out_of_range' && echo "MISMATCH: the failure degraded into a logical error: $RESPONSE" || echo 'no out-of-range error: OK'

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"

echo '--- without the fault the format works'
${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" -d "SELECT 1 AS x FORMAT JSONCompact" | grep -o '"data":'
