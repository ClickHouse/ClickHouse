# Keeper Bench

`keeper-bench` benchmarks ClickHouse Keeper (or any ZooKeeper-compatible service) in two modes:

- Generate requests from a workload config (`generator` section).
- Replay requests from a recorded request log (`--input-request-log`).

## Quick start

Replace placeholders in the commands below with paths in your environment.

```bash
# Generated workload from config
clickhouse keeper-bench \
    --config <config_file>

# Replay workload from a request log
clickhouse keeper-bench \
    -h localhost:9181 \
    --input-request-log <request_log_file>
```

An example config is available at `programs/keeper-bench/example.yaml`.

## Command-line options

| Flag | Short | Description |
|------|-------|-------------|
| `--help` | | Print help and exit |
| `--config` | | YAML/XML config file |
| `--input-request-log` | | Replay requests from a request log file |
| `--setup-nodes-snapshot-path` | | Directory containing Keeper snapshots used to build initial node state for replay |
| `--concurrency` | `-c` | Number of parallel worker threads |
| `--report-delay` | `-d` | Delay between periodic reports in seconds (`0` disables periodic reports) |
| `--iterations` | `-i` | Total number of requests to execute (`0` means unlimited) |
| `--time-limit` | `-t` | Stop producing new requests after this many seconds |
| `--hosts` | `-h` | Host list, e.g. `-h host1:9181 host2:9181` |
| `--continue_on_errors` | | Continue running after request exceptions |

Rules:

- `--config` or `--input-request-log` must be provided.
- Command-line values override config values for overlapping fields.
- Hosts can come from `--hosts` or from `connections` in config.
- If config does not define `generator`, execution uses replay mode, so `--input-request-log` must be provided.

## Modes

### Generated mode (`generator`)

Use when you want synthetic workload generation.

Required:

- `generator.requests` in config.
- At least one host from `--hosts` or `connections`.

Optional:

- `setup` for creating initial Keeper tree.
- `output` for JSON output.

### Replay mode (`--input-request-log`)

Use when you want to replay previously recorded Keeper traffic.

Required:

- `--input-request-log`.
- At least one host from `--hosts` or `connections`.

Optional:

- `--setup-nodes-snapshot-path` to build/update initial snapshot state inferred from expected replay outcomes.

## Configuration file

Config can be YAML or XML.

### Table of contents

- [Top-level keys](#top-level-keys)
- [Special types](#special-types)
- [General settings](#general-settings)
- [Connections](#connections)
- [Setup](#setup)
- [Generator](#generator)
- [Replay request log](#replay-request-log)
- [Output](#output)
- [Troubleshooting](#troubleshooting)

---

## Top-level keys

| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `concurrency` | integer | `1` | Worker threads |
| `iterations` | integer | `0` | `0` means unlimited |
| `report_delay` | float | `1.0` | Seconds between periodic reports; `0` disables |
| `timelimit` | float | `0` | Seconds to stop producing new requests; `0` disables |
| `continue_on_error` | bool | `false` | Continue after request exceptions |
| `pipeline_depth` | integer | `1` | In-flight async requests per worker (`>= 1`) |
| `warmup_seconds` | float | `0` | Measurement warmup window |
| `enable_tracing` | bool | `false` | Attach OpenTelemetry trace context |
| `use_remove_recursive` | bool | `true` | Use the native `RemoveRecursive` Keeper request for cleanup; falls back to a manual recursive traversal if the server does not advertise the `REMOVE_RECURSIVE` feature flag or if this is set to `false` (needed for ZooKeeper or older Keeper versions) |
| `connections` | object | required if `--hosts` absent | Keeper endpoints and connection settings |
| `setup` | object | optional | Data tree created before run |
| `generator` | object | optional | Request generator (enables generated mode) |
| `output` | object | optional | JSON output controls |

---

## Special types

These reusable types are used in `setup` and `generator` sections.

### `IntegerGetter`

A constant integer, or a random value drawn uniformly from a range on each use.

```yaml
# constant
key: 42

# random from [10, 20]
key:
    min_value: 10
    max_value: 20
```

### `StringGetter`

A constant string, or a random string whose length is an `IntegerGetter`.

```yaml
# constant
key: "hello"

# random string with length drawn from [10, 20]
key:
    random_string:
        size:
            min_value: 10
            max_value: 20
```

### `PathGetter`

One or more ZooKeeper paths. Paths can be explicit, expanded from children of a parent, or drawn from a tagged set of paths created during setup.

```yaml
# explicit paths
path:
    - "/path1"
    - "/path2"

# children of a parent node
path:
    children_of: "/path3"

# paths collected by tag during setup (see Setup section)
path:
    tagged: "my_tag"
```

All forms can be used together and merged into one candidate set.

Notes:

- Paths must start with `/`.
- `children_of` is resolved at startup; if it has no children and no explicit paths are provided, an exception is raised.
- `tagged` references a tag name assigned to setup nodes via the `tag` field. All paths created with that tag are included. If the tag is not found, an exception is raised.
- Duplicate `path` keys in one section are supported when parsed by ClickHouse config loader (Poco-style key indexing).

---

## General settings

```yaml
# number of worker threads (default: 1)
concurrency: 20

# total requests to execute; 0 = unlimited (default: 0)
iterations: 10000

# periodic report interval in seconds; 0 = disable (default: 1.0)
report_delay: 4

# stop producing new requests after this many seconds; 0 = no limit (default: 0)
timelimit: 300

# continue on request exceptions (default: false)
continue_on_error: true

# max in-flight requests per worker; must be >= 1 (default: 1)
pipeline_depth: 8

# ignore stats during first N seconds, then reset counters (default: 0)
warmup_seconds: 5

# attach OpenTelemetry tracing context to requests (default: false)
enable_tracing: false

# use the native `RemoveRecursive` Keeper request to wipe setup roots; set to
# false for ZooKeeper or older Keeper servers that do not support it (default: true)
use_remove_recursive: true
```

---

## Connections

Connections are defined under top-level `connections`.

- Values directly under `connections` are defaults.
- `connections.host` and `connections.connection` entries define concrete endpoints.
- Each endpoint can open multiple sessions via `sessions`.

### Per-connection settings

```yaml
secure: boolean                  # use TLS (default: false)
operation_timeout_ms: integer    # operation timeout (default: Keeper client default)
session_timeout_ms: integer      # session timeout (default: Keeper client default)
connection_timeout_ms: integer   # connect timeout (default: Keeper client default)
use_compression: boolean         # protocol compression (default: false)
use_xid_64: boolean              # use 64-bit xid (default: false)
sessions: integer                # sessions per endpoint (default: 1)
```

### Example

```yaml
connections:
    # defaults for all endpoints
    secure: true
    operation_timeout_ms: 3000

    # one session
    host: "localhost:9181"

    # two sessions with per-endpoint overrides
    connection:
        host: "localhost:9182"
        sessions: 2
        operation_timeout_ms: 2000
        session_timeout_ms: 2000
```

---

## Setup

`setup` defines nodes created before benchmarking.

Important behavior:

- Before setup creation, the tool recursively removes each configured root node path.
- On normal shutdown, it attempts to clean these root paths again.
- `repeat` requires random `name`; otherwise an exception is raised.

Structure:

```yaml
node:
    name: StringGetter
    data: StringGetter           # optional
    repeat: integer              # optional, requires random name
    tag: string                  # optional, collects created paths under this tag
    node: ...                    # nested children
```

The `tag` field labels a node so that all paths created from it (including repeated copies) are collected into a named set. Generators can reference this set with `tagged` in their `PathGetter` config, allowing requests to target specific nodes from the setup tree.

Example:

```yaml
setup:
    node:
        name: "tree"
        node:
            repeat: 10
            name:
                random_string:
                    size: 20
            tag: "inner"
            node:
                repeat: 3
                name:
                    random_string:
                        size: 10
                tag: "leaves"

generator:
    requests:
        set:
            path:
                tagged: "leaves"
            data: "updated"
```

In this example, `set` requests target the 30 leaf nodes (3 per each of 10 inner nodes), selected randomly from all paths tagged `"leaves"` during setup.

---

## Generator

`generator` controls synthetic workload generation.

```yaml
generator:
    # fixed seed for reproducibility; if omitted, random seed is generated and printed
    seed: 12345
```

Requests are defined in `generator.requests`.

- Supported request types: `create`, `set`, `get`, `list`, `multi`.
- Each request entry can define `weight` (default `1`, must be `>= 1`).
- Nested `multi` is not allowed.

### `create`

```yaml
create:
    path: "/bench/creates"           # PathGetter
    name_length: 10                    # IntegerGetter, default: 5
    data: "payload"                   # StringGetter, default: empty
    remove_factor: 0.5                 # in [0.0, 1.0], default: 0
```

When `remove_factor` is enabled, some operations become random removes of previously created nodes.
If unique name generation keeps colliding, the generator raises an exception after bounded retries.

### `set`

```yaml
set:
    path: PathGetter
    data: StringGetter
```

### `get`

```yaml
get:
    path: PathGetter
    watch_probability: 0.5           # in [0.0, 1.0], default: 0 (no watches)
```

When `watch_probability` is set, each request has that probability of setting a watch on the node.
Watch fire events are counted and reported in stats.

### `list`

```yaml
list:
    path: PathGetter
    watch_probability: 0.5           # in [0.0, 1.0], default: 0 (no watches)
```

When `watch_probability` is set, each request has that probability of setting a watch on the node.
Watch fire events are counted and reported in stats.

### `multi`

```yaml
multi:
    size: IntegerGetter              # optional
    # nested request generators (`create`/`set`/`get`/`list`) with optional `weight`
```

Behavior:

- If `size` is set, that many subrequests are sampled.
- If `size` is omitted, one subrequest from each nested generator is included.

### Example

```yaml
generator:
    seed: 42
    requests:
        create:
            path: "/test_create"
            name_length:
                min_value: 10
                max_value: 20
            remove_factor: 0.5

        multi:
            weight: 20
            size: 10
            get:
                path:
                    children_of: "/test_get1"
            get:
                weight: 2
                path:
                    children_of: "/test_get2"
```

In this example, `multi` is selected about 20 times more often than `create`.
Inside `multi`, gets from `/test_get2` are selected about 2 times more often than gets from `/test_get1`.

---

## Replay request log

Replay mode reads requests from `--input-request-log`.

Behavior details:

- Input format and schema are auto-detected.
- Compressed files are supported through ClickHouse format/compression detection.
- Replay preserves per-session request ordering via executor queues.

Supported operation kinds in logs include `Create`, `Set`, `Remove`, `Check`, `CheckNotExists`, `Sync`, `Get`, `List`, `Exists`, `Multi`, and `MultiRead`.

Expected log columns:

- `hostname`
- `request_event_time`
- `thread_id`
- `session_id`
- `xid`
- `has_watch`
- `op_num`
- `path`
- `data`
- `is_ephemeral`
- `is_sequential`
- `response_event_time`
- `error`
- `requests_size`
- `version`

If `--setup-nodes-snapshot-path` is provided during replay, the tool can infer required initial nodes from expected outcomes and write an updated snapshot.

---

## Output

### Stderr progress reports

Periodic stderr reports (controlled by `report_delay`) include:

- Total read/write request counts (cumulative).
- Read/write RPS and throughput (for the last reporting period).
- Read/write latency percentiles (`0, 10, ..., 90, 95, 99, 99.9, 99.99`).
- Watches fired (when `watch_probability` is configured).

### JSON output

Configure JSON output with:

```yaml
output:
    file: "output.json"
    # or:
    file:
        path: "output.json"
        with_timestamp: true
    stdout: true
```

JSON fields:

- `timestamp` (epoch milliseconds).
- `read_results` (present only if read requests exist).
- `write_results` (present only if write requests exist).
- `watches_fired` (present only when watches are used).

Each result object contains:

- `total_requests`
- `requests_per_second`
- `bytes_per_second`
- `percentiles` (array of `{ "<percent>": <latency_ms> }` objects)

---

## Troubleshooting

Common configuration exceptions:

- `No config file or hosts defined`: provide `--hosts` or `connections`.
- `Both --config and --input_request_log cannot be empty`: provide at least one mode input.
- `Invalid path for request generator`: all paths must start with `/`.
- `PathGetter has no paths after initialization`: `children_of` parent has no children and no explicit `path` entries were supplied.
- `Generator weight must be >= 1`: use positive weights only.
- `remove_factor must be in [0.0, 1.0]`: keep probability in range.
- `watch_probability must be in [0.0, 1.0]`: keep probability in range.
- `Nested multi requests are not allowed`: only one `multi` level is supported.
- `Tag '...' not found in setup`: a `tagged` path reference names a tag that no setup node defines. Check spelling and ensure the setup section includes nodes with matching `tag` values.
- `Repeating node creation ..., but name is not randomly generated`: use random `name` when `repeat` is set.
