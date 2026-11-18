#!/usr/bin/env bash
# Tags: no-msan, no-parallel
# The sampling query profiler is disabled under MSan.
# Millisecond sampling can exhaust the shared profiler pipe and delay trace delivery.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CURL" "$CLICKHOUSE_URL" "$CLICKHOUSE_TMP" <<'PY'
import json
import shlex
import subprocess
import sys
import uuid
from pathlib import Path
from urllib.parse import urlencode

curl_command, base_url, temporary_directory = sys.argv[1:]
curl = shlex.split(curl_command)
settings = {
    "framing_output_format": "JSONEachPacketString",
    "send_profile_traces": 1,
    "send_profile_events": 0,
    "send_logs_level": "none",
    "query_profiler_cpu_time_period_ns": 1000000,
    "query_profiler_real_time_period_ns": 1000000,
    "memory_profiler_step": 0,
    "memory_profiler_sample_probability": 0,
    "http_wait_end_of_query": 0,
    "http_response_buffer_size": 0,
    "output_format_parallel_formatting": 0,
    "max_threads": 1,
    "max_execution_time": 2,
    "timeout_overflow_mode": "break",
    "max_rows_to_read": 0,
    "interactive_delay": 1000,
    "enable_http_compression": 1,
    "http_zlib_compression_level": 9,
}
transport_functions = (
    "WriteBufferFromHTTPServerResponse::nextImpl",
    "HTTPWriteBuffer::nextImpl",
    "ZlibDeflatingWriteBuffer::nextImpl",
    "LibdeflateDeflatingWriteBuffer::nextImpl",
    "CompressedWriteBuffer::nextImpl",
)


def sample_diagnostics(samples):
    payload = [sample for sample in samples if any("IFramingFormat::onPayload" in symbol for symbol in sample["symbols"])]
    transport = [sample for sample in samples if any(function in symbol for symbol in sample["symbols"] for function in transport_functions)]
    compression = [sample for sample in samples if any("deflate" in symbol for symbol in sample["symbols"])]

    def stack_example(rows):
        return [{"trace_type": row["trace_type"], "symbols": [symbol[:200] for symbol in row["symbols"][:24]]} for row in rows[:1]]

    return {
        "timer_samples": {kind: sum(sample["trace_type"] == kind for sample in samples) for kind in ("CPU", "Real")},
        "resolved_samples": sum(bool(any(sample["symbols"])) for sample in samples),
        "on_payload_samples": len(payload),
        "transport_samples": len(transport),
        "compression_samples": len(compression),
        "on_payload_example": stack_example(payload),
        "transport_example": stack_example(transport),
        "compression_example": stack_example(compression),
        "timer_example": stack_example(samples),
    }


def execute(query, block_size):
    query_id = "profile_http_flush_" + uuid.uuid4().hex
    options = dict(settings, query_id=query_id, max_block_size=block_size)
    headers_file = Path(temporary_directory) / (query_id + ".headers")
    result = subprocess.run(
        curl
        + [
            "-sS",
            "--compressed",
            "--dump-header",
            str(headers_file),
            "-H",
            "Accept-Encoding: gzip",
            base_url + "&" + urlencode(options),
            "--data-binary",
            query,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "content-encoding: gzip" in headers_file.read_text().lower().splitlines()
    packets = [json.loads(line) for line in result.stdout.splitlines()]
    assert packets and packets[-1]["packet"] == "progress", packets[-1:]
    batches = [packet["profile_traces"] for packet in packets if packet["packet"] == "profile_traces"]
    assert len(batches) > 1, "trace delivery was not exercised during execution"
    samples = [sample for batch in batches for sample in batch if sample["trace_type"] in ("CPU", "Real")]
    assert {sample["trace_type"] for sample in samples} == {"CPU", "Real"}, "missing query timer samples"
    output_samples = [
        sample
        for sample in samples
        if any("IFramingFormat::onPayload" in symbol for symbol in sample["symbols"]) and any(function in symbol for symbol in sample["symbols"] for function in transport_functions)
    ]
    return packets, output_samples, samples


# `Null` still takes a payload boundary for every chunk but writes no data bytes.
# With logs and profile events disabled, actual compression/socket work below
# `onPayload` can only deliver trace packets. Merely entering an empty `flushOut`
# is not sufficient evidence of unsuppressed trace delivery.
packets, output_samples, samples = execute(
    "SELECT length(range(number + 100000)) FROM numbers(1000000) FORMAT Null",
    16,
)
assert not any(packet["packet"] == "data" for packet in packets)
assert not output_samples, output_samples[:1]
print("trace-only HTTP payload flushes are excluded from streamed samples")

# Ordinary result compression must remain visible when profile traces are enabled.
# This catches a guard widened to cover every output flush instead of trace delivery.
packets, output_samples, samples = execute(
    "SELECT hex(cityHash64(number)) FROM numbers(500000) FORMAT TSV",
    8192,
)
assert any(packet["packet"] == "data" for packet in packets)
assert output_samples, "ordinary HTTP result delivery was excluded from the profile: " + json.dumps(sample_diagnostics(samples))
print("ordinary HTTP result compression remains visible in streamed samples")
PY
