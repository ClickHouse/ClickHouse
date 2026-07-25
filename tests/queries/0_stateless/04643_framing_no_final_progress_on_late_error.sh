#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_throw_after_final_progress` fail point, which affects the
# whole server. It fires on the next framed query finish anywhere on the server, so a concurrent
# framing query from another test could consume the injected fault - making this test miss its own
# exception packet and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failure after the final counters were already stashed in the framing format (here: injected
# right after `flushQueryProgress`, the same window where `BlockIO::onFinish` - a query-log write,
# for example - can throw) must not emit the success-style final `progress` packet. The final
# `progress` packet with the final counters (`result_rows` / `result_bytes`) is the success
# terminator of the stream, so a failed stream must end with the `exception` packet alone -
# otherwise a client that treats the final-counters `progress` packet as the success terminator
# would take the failed query for a successful one. Intermediate `progress` packets never carry
# `result_rows` (it is known only after the query finished and zero values are omitted), so its
# presence identifies the final progress packet.
echo '--- a failure after the final progress was stashed ends with the exception packet only'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_throw_after_final_progress"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 AS x FORMAT JSONEachRow" | python3 -c "
import sys, json
lines = [line for line in sys.stdin.read().splitlines() if line]
try:
    packets = [json.loads(line) for line in lines]
except json.JSONDecodeError:
    print('MISMATCH: response is not valid NDJSON')
else:
    final_progress = [p for p in packets if p.get('packet') == 'progress' and 'result_rows' in p.get('progress', {})]
    if not final_progress:
        print('no final progress packet on a failed stream: OK')
    else:
        print('MISMATCH: final progress packets =', final_progress)
    if packets and packets[-1].get('packet') == 'exception' and 'FAULT_INJECTED' in packets[-1].get('exception', ''):
        print('terminal framed exception: OK')
    else:
        print('MISMATCH: last packet =', packets[-1] if packets else None)
"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_throw_after_final_progress"
