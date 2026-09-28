# Local AI function validation

Use a ClickHouse binary built from this checkout. An installed release or a
prebuilt image cannot validate changes to the C++ implementation.

## Deterministic regression tests

Build with the [Linux build instructions](../../../docs/resources/develop-contribute/build/build.mdx)
or [macOS build instructions](../../../docs/resources/develop-contribute/build/build-osx.mdx).
The Docker integration runner needs a Linux binary, including when Docker runs
on macOS. Redirect build output to `build/build.log`.

From the repository root, with Docker running and the Linux binary at
`build/programs/clickhouse`:

```sh
mkdir -p build
uv run --no-project python -m ci.praktika run integration \
    --test test_ai_functions --workers 1 \
    > build/test_ai_functions.log 2>&1
```

The `test_ai_metrics_*` cases check both provider usage formats, missing cache
fields, billed malformed and truncated responses, cache-aware quotas, NULL and
empty inputs, multiple blocks, embedding batches, and throwing/non-throwing
retries. The local HTTP mock supplies exact token counts and a fixed minimum
response delay. No provider credentials or billable inference are needed.

The assertions use `system.query_log`, including `ExceptionWhileProcessing`,
to check that metrics reach the query rather than just a process-wide counter.

## Optional real inference through a local gateway

Start the inference gateway from its own checkout, loading `.env.smoke` there.
Keep provider keys in that gateway process. Use a dummy local master key for
the ClickHouse connection. For example, in a separate terminal:

```sh
cd "$GATEWAY_DIR"
set -a
source .env.smoke
set +a
LISTEN_ADDR=127.0.0.1:18080 PROXY_MASTER_KEY=sk-local-smoke \
    DATABASE_URL= METASTORE_HOST= go run .
```

Run a native ClickHouse server built from this checkout on the same host.
Choose a chat model advertised by the local gateway's `/v1/models` endpoint
that accepts `temperature` and `max_tokens`. Set up a named collection with
that model:

```sql
CREATE NAMED COLLECTION ai_local_metrics AS
    provider = 'openai',
    endpoint = 'http://127.0.0.1:18080/v1/chat/completions',
    model = '<chat-model>',
    api_key = 'sk-local-smoke';
```

Run one bounded request with no retries:

```sh
build/programs/clickhouse client --query_id ai-metrics-smoke --query "
    SELECT aiGenerate('Reply with OK', map('credentials', 'ai_local_metrics', 'max_tokens', '32'))
    SETTINGS ai_function_max_retries = 0,
             ai_function_max_api_calls_per_query = 1,
             ai_function_request_timeout_sec = 60,
             log_queries = 1, log_profile_events = 1"
```

Inspect the result with the query in the
[AI observability documentation](../../../docs/reference/functions/regular-functions/ai-functions.mdx#observability),
using `ai-metrics-smoke` as the query ID. Expect one API call, one input row,
one processed row, and positive request time. Cache tokens may be zero for a short
or cold prompt; the deterministic tests above verify nonzero cache accounting.
To exercise the native Anthropic usage format, use a separate collection with
`provider = 'anthropic'` and the local gateway's `/v1/messages` endpoint.

For a ClickHouse server in Docker, bind the gateway to an address reachable
from that container and use that address in the collection instead of the
container's `127.0.0.1`.
