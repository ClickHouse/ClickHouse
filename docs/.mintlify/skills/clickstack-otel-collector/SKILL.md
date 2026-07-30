---
name: clickstack-otel-collector
description: Use when a user wants to set up the OpenTelemetry collector for Managed ClickStack on a ClickHouse Cloud service, send logs/traces/metrics, and verify the data lands in ClickStack.
license: Apache-2.0
metadata:
  author: ClickHouse Inc
  version: "0.1.0"
---

# Set up the ClickStack OpenTelemetry collector

This skill walks an agent through wiring an OpenTelemetry collector into a Managed ClickStack service running on ClickHouse Cloud. It uses [`clickhousectl`](https://clickhouse.com/docs/interfaces/cli) for all cloud and SQL operations.

The end state is:

- A dedicated `hyperdx_ingest` SQL user on the target service.
- A ClickStack-distribution OpenTelemetry collector running locally, accepting OTLP on `4317`/`4318` and writing into the `otel` database on the target service.
- Synthetic telemetry exercising the pipeline.
- A URL the user can open to view the data in ClickStack.

Follow these steps in order. Do not skip ahead — each step depends on state established by the previous one.

---

## Before you begin: heads-up on permissions

Coding agents typically ask the user to approve each shell command the first time they see it. To keep this run smooth, tell the user up front what categories of commands you'll need to run, and ask them to grant the permissions in one batch — "allow always" / "approve for session" in their agent — for each category below. That way you won't have to interrupt them every few steps.

You will run, in order:

1. **`openssl rand …`** — to generate a random OTLP auth token and a SQL password.
2. **`clickhousectl …`** — to authenticate, look up the service, and run SQL via the Query API. Specifically: `cloud auth status`, `cloud auth login`, `cloud service get`, `cloud service query`.
3. **`docker info`, `docker run`, `docker ps`, `docker logs`** — to deploy and observe the OpenTelemetry collector container.
4. **`otelgen …`** — to send synthetic logs, traces, and metrics through the collector.
5. **`jq …`** and **`curl …`** — small text/JSON utilities and one local HTTP healthcheck.

Tell the user: *"If your agent supports it, choose 'always allow' for each command class above the first time it asks — there are no destructive operations and no commands targeting anything outside this project / your ClickHouse Cloud service."*

Then continue.

---

## Step 1: Confirm the target service

The user's prompt contains a service identifier — either a service ID (UUID) or a service name. Treat that value as `SERVICE_REF`.

**Ask the user to confirm**, and capture three values in your working memory:

1. `SERVICE_REF` — what they gave you.
2. `OTLP_AUTH_TOKEN` — a shared secret the collector will require on inbound OTLP requests. Generate a random one (e.g. `openssl rand -hex 16`) unless the user provides one.
3. `HYPERDX_PASSWORD` — the password for the `hyperdx_ingest` SQL user. Generate a strong random password (e.g. `openssl rand -base64 32`) unless the user provides one.

Echo the values back to the user with a one-line note that the OTLP token and the SQL password are sensitive. Do **not** print the password again after this confirmation.

---

## Step 2: Install and authenticate `clickhousectl`

Check `clickhousectl` is on `PATH`:

```bash
which clickhousectl
```

If it is missing, install it:

```bash
curl -fsSL https://clickhouse.com/cli | sh
```

Then verify authentication:

```bash
clickhousectl cloud auth status
```

The skill needs **API key authentication** (OAuth is read-only and cannot create users or run write queries). If the `API key` row is not `Active`, ask the user for credentials:

> I need a ClickHouse Cloud API key to create the ingestion user and verify the data.
>
> **To get one**, open the [ClickHouse Cloud console](https://console.clickhouse.cloud), open the **Organization** menu on the left nav, choose **API keys**, then **New API key**. Select the **Admin** role — Developer-scoped keys can't auto-provision the per-service query endpoint that `cloud service query` uses.
>
> **Then, either:**
>
> - **Paste them in your next message** and I'll authenticate from this session, **or**
> - **Authenticate yourself in a separate terminal** with:
>
>   ```bash
>   clickhousectl cloud auth login --api-key <key-id> --api-secret <key-secret>
>   ```
>
>   …and tell me when you're done — I'll poll `clickhousectl cloud auth status` until the API key row shows `Active`.

If the user pastes credentials, run the login command yourself (do not echo the secret back) and then verify:

```bash
clickhousectl cloud auth status
```

Do not continue until the API key row is `Active`.

---

## Step 3: Resolve the service and capture the HTTPS endpoint

Find the target service. If `SERVICE_REF` looks like a UUID, use it directly; otherwise, look it up by name:

```bash
# UUID form
clickhousectl cloud service get "$SERVICE_REF" --json

# Name form (search across orgs)
clickhousectl cloud service list --json | jq '.[] | select(.name=="<name>")'
```

From the JSON, extract:

- `id` → `SERVICE_ID`
- `name` → `SERVICE_NAME`
- `state` — must be `running`. If it is `stopped` or `starting`, ask the user to start the service or wait; do not proceed.
- The `endpoints` array entry with `"protocol": "https"` → `HTTPS_ENDPOINT_HOST` and `HTTPS_ENDPOINT_PORT` (typically `8443`).

Note: `port` in the JSON is a number that may serialize as a float (`8443.0`). Coerce it to an integer when building the URL — for example with `jq`:

```bash
HTTPS_ENDPOINT=$(jq -r '.endpoints[] | select(.protocol=="https") | "https://\(.host):\(.port | tonumber | floor)"' /tmp/svc.json)
```

That produces:

```
CLICKHOUSE_ENDPOINT=https://<host>:8443
```

Do not let `:8443.0` leak through — the OTel collector's ClickHouse exporter will fail to dial it.

Sanity-check the service is reachable via the query API:

```bash
clickhousectl cloud service query --id "$SERVICE_ID" --query "SELECT version()"
```

A successful response confirms both the service and the per-service query endpoint key were provisioned. If this is the first time, `clickhousectl` will print `Provisioning Query API endpoint + key for service '<name>'...` — that is expected.

---

## Step 4: Create the `hyperdx_ingest` SQL user

Create the user and grant it the minimum privileges the ClickStack OTel collector needs to create the `otel` database and write into it:

```bash
clickhousectl cloud service query --id "$SERVICE_ID" --query \
  "CREATE USER hyperdx_ingest IDENTIFIED WITH sha256_password BY '$HYPERDX_PASSWORD'"

clickhousectl cloud service query --id "$SERVICE_ID" --query \
  "GRANT SELECT, INSERT, CREATE DATABASE, CREATE TABLE, CREATE VIEW ON otel.* TO hyperdx_ingest"
```

If `CREATE USER` fails with `already exists`, rotate the password instead so this run uses a known value:

```bash
clickhousectl cloud service query --id "$SERVICE_ID" --query \
  "ALTER USER hyperdx_ingest IDENTIFIED WITH sha256_password BY '$HYPERDX_PASSWORD'"
```

Verify the grants:

```bash
clickhousectl cloud service query --id "$SERVICE_ID" --query "SHOW GRANTS FOR hyperdx_ingest"
```

You should see `GRANT SELECT, INSERT, CREATE DATABASE, CREATE TABLE, CREATE VIEW ON otel.* TO hyperdx_ingest`.

---

## Step 5: Deploy the ClickStack OpenTelemetry collector

Run the ClickStack-distribution collector locally. It is preconfigured for Managed ClickStack — it creates the `otel.*` schema on first use and routes Session Replay events to `otel.hyperdx_sessions`.

Make sure Docker is running:

```bash
docker info > /dev/null
```

Start the collector:

```bash
docker run -d \
  --name clickstack-otel-collector \
  -e OTLP_AUTH_TOKEN="$OTLP_AUTH_TOKEN" \
  -e CLICKHOUSE_ENDPOINT="$CLICKHOUSE_ENDPOINT" \
  -e CLICKHOUSE_USER=hyperdx_ingest \
  -e CLICKHOUSE_PASSWORD="$HYPERDX_PASSWORD" \
  -e HYPERDX_OTEL_EXPORTER_CLICKHOUSE_DATABASE=otel \
  -p 4317:4317 \
  -p 4318:4318 \
  clickhouse/clickstack-otel-collector:latest
```

Confirm it is healthy:

```bash
docker ps --filter name=clickstack-otel-collector --format '{{.Status}}'
docker logs --tail 30 clickstack-otel-collector 2>&1 | tail -30
```

The logs should contain `Everything is ready. Begin running and processing data.` (or equivalent), and no repeated `clickhouseexporter` connection errors. If you see TLS or auth errors, the most common cause is a malformed `CLICKHOUSE_ENDPOINT` (must include `https://` and the `:8443` port).

---

## Step 6: Send synthetic telemetry and verify ingestion

Install [`otelgen`](https://github.com/krzko/otelgen). On macOS:

```bash
brew install krzko/tap/otelgen
```

Otherwise, with Go:

```bash
go install github.com/krzko/otelgen@latest
```

### Time-box every otelgen call

`otelgen`'s flag handling is inconsistent — `--duration` is honored on some subcommands and silently ignored on others (notably `metrics`, but in practice not reliable on `logs multi` / `traces multi` either, depending on the build). So **do not trust `--duration`** to terminate the process. Instead, wrap every call in a small bash helper that backgrounds it, waits a bounded number of seconds, and then sends `SIGINT` (followed by `SIGKILL` if it ignores the interrupt):

```bash
run_otelgen() {
  # usage: run_otelgen <wall-clock-seconds> <otelgen-subcommand-args...>
  local secs="$1"; shift
  otelgen --otel-exporter-otlp-endpoint localhost:4317 --insecure --protocol grpc \
          --header "authorization=$OTLP_AUTH_TOKEN" --rate 5 "$@" &
  local pid=$!
  ( sleep "$secs" && kill -INT "$pid" 2>/dev/null \
    && sleep 3 && kill -KILL "$pid" 2>/dev/null ) &
  local watchdog=$!
  wait "$pid" 2>/dev/null || true
  kill "$watchdog" 2>/dev/null || true
}
```

Define this once, then send a short burst of each signal:

```bash
run_otelgen 20 logs multi
run_otelgen 20 traces multi
run_otelgen 15 metrics sum
```

Each call returns when the wall-clock budget expires — no hung processes, no stalled scripts.

Notes on the synthetic data:

- `logs multi` and `traces multi` emit several events per tick; 20 seconds is plenty to populate `otel_logs` and `otel_traces`.
- `metrics sum` only emits one data point every few seconds — 15 seconds yields 2–3 points, which is enough to verify the metrics path.
- See the [otelgen synthetic-data guide](/use-cases/observability/clickstack/getting-started/otelgen) for the other `metrics` subcommands (`gauge`, `histogram`, `exponential-histogram`).

Wait ~15 seconds for the collector batch flush. The ClickStack collector creates its tables on first write — confirm they exist:

```bash
clickhousectl cloud service query --id "$SERVICE_ID" --query \
  "SELECT name FROM system.tables WHERE database='otel' ORDER BY name"
```

Expect at minimum `otel_logs`, `otel_traces`, and one or more `otel_metrics_*` tables. Then confirm rows are landing in each signal. The schema uses standard upstream ClickHouse-exporter column names (`Timestamp` on logs/traces, `TimeUnix` on metrics) — but rather than hard-coding column names, count by `parts.rows` on the underlying parts which is signal-agnostic:

```bash
clickhousectl cloud service query --id "$SERVICE_ID" --query \
  "SELECT table, sum(rows) AS rows
   FROM system.parts
   WHERE database='otel' AND active
     AND table IN ('otel_logs','otel_traces',
                   'otel_metrics_sum','otel_metrics_gauge',
                   'otel_metrics_histogram','otel_metrics_exponential_histogram',
                   'otel_metrics_summary')
   GROUP BY table
   ORDER BY table"
```

You should see non-zero `rows` for `otel_logs`, `otel_traces`, and at least one `otel_metrics_*` table (`otel_metrics_sum` if you used the `metrics sum` command above). If any signal is missing:

1. Tail the collector logs (`docker logs --tail 50 clickstack-otel-collector`) for export errors.
2. Confirm the `authorization` header sent by `otelgen` matches `$OTLP_AUTH_TOKEN`.
3. Re-check `CLICKHOUSE_ENDPOINT` includes the `:8443` port and `https://` scheme.
4. Some metric kinds take longer to flush — re-run the query after another 30 seconds before declaring failure.

Do not proceed until every expected signal has non-zero rows.

---

## Step 7: Summarize the result and hand off

The collector is running as a local Docker container on this machine. The ClickStack UI is reached most directly at:

```
https://hyperdx.clickhouse.cloud/search?chcServiceId=<SERVICE_ID>
```

(There is also a Cloud-console route — `https://console.clickhouse.cloud/services/<SERVICE_ID>/clickstack` — which redirects to the same UI after auth. Either works; prefer the direct HyperDX URL.)

Print a summary to the user, formatted exactly like this:

```
✅ ClickStack is set up and ingesting telemetry for service <SERVICE_NAME> (<SERVICE_ID>).

Local OpenTelemetry collector
  ▸ Container: clickstack-otel-collector (Docker)
  ▸ Send OTLP gRPC to: localhost:4317
  ▸ Send OTLP HTTP to: localhost:4318
  ▸ Required header:   authorization: <OTLP_AUTH_TOKEN>

ClickHouse target
  ▸ Endpoint: <CLICKHOUSE_ENDPOINT>
  ▸ SQL user: hyperdx_ingest
  ▸ Database: otel

Open ClickStack:
  ▸ https://hyperdx.clickhouse.cloud/search?chcServiceId=<SERVICE_ID>
```

Then tell the user, in your own words, that:

1. The collector keeps running until they stop the container — `docker stop clickstack-otel-collector` shuts it down; `docker start clickstack-otel-collector` brings it back.
2. Any application, SDK, or agent collector on this host can now send OTLP to `localhost:4317` (gRPC) or `localhost:4318` (HTTP), with the `authorization` header above.
3. Synthetic data is already flowing — they can open the HyperDX URL above to see logs / traces / metrics under the `otelgen` service.

---

## Cleanup (only if the user explicitly asks)

```bash
docker rm -f clickstack-otel-collector

clickhousectl cloud service query --id "$SERVICE_ID" --query "DROP USER IF EXISTS hyperdx_ingest"
```

Do **not** drop the `otel` database — it contains telemetry the user may want to retain.
