#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_pump_logs_throw` fail point, which affects the whole server.
# It fires on the next framing-format log pump anywhere on the server, so a concurrent framing query
# from another test could consume the injected fault - making this test miss its own exception packet
# and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failure in the framing format's non-emitting work between packet writes (here: draining the log
# queue, injected right after the previous packet was fully written) leaves no partially written
# packet on the wire. The exception recovery re-enters the framing `finalize`, which must not fail
# closed in this case: the client must still receive the terminal framed `exception` packet, keeping
# the stream well-formed and the error visible. Any `data` packet written before the injected fault
# must be complete. The fault may fire on the first packet boundary of any kind (a `data` payload or
# a `progress` update, whichever the query hits first), so the exact preceding packets are not pinned.
echo '--- an error before any bytes of the next packet still ends with a framed exception'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_pump_logs_throw"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 AS x FORMAT JSONEachRow" | python3 -c "
import sys, json
lines = [line for line in sys.stdin.read().splitlines() if line]
try:
    packets = [json.loads(line) for line in lines]
except json.JSONDecodeError:
    print('MISMATCH: response is not valid NDJSON')
else:
    bad_data = [p for p in packets if p.get('packet') == 'data' and p.get('data') != '{\"x\":1}\n']
    if not bad_data:
        print('data packets complete: OK')
    else:
        print('MISMATCH: incomplete data packets =', bad_data)
    if packets and packets[-1].get('packet') == 'exception' and 'FAULT_INJECTED' in packets[-1].get('exception', ''):
        print('terminal framed exception: OK')
    else:
        print('MISMATCH: last packet =', packets[-1] if packets else None)
"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_pump_logs_throw"
