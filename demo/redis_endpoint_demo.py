#!/usr/bin/env python3

import http.client
import socket
import time
from dataclasses import dataclass
from typing import Any

import streamlit as st


class RedisErrorResponse(Exception):
    pass


class NullBulkString:
    pass


NULL_BULK_STRING = NullBulkString()


@dataclass
class QueryResult:
    value: Any = None
    latency_ms: float = 0.0
    error: str | None = None


def quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def quote_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def format_key_for_sql(key: str, key_type: str) -> str:
    if key_type == "UInt64":
        if not key or not key.isdigit():
            raise ValueError(f"Invalid UInt64 key: {key!r}")
        return key
    return quote_string(key)


def execute_clickhouse_http(host: str, port: int, query: str) -> QueryResult:
    start = time.perf_counter()
    connection = None
    try:
        connection = http.client.HTTPConnection(host, port, timeout=10)
        connection.request("POST", "/", body=query.encode("utf-8"))
        response = connection.getresponse()
        body = response.read().decode("utf-8", errors="replace")
        latency_ms = (time.perf_counter() - start) * 1000.0
        if response.status >= 400:
            return QueryResult(error=f"HTTP {response.status}: {body}", latency_ms=latency_ms)
        return QueryResult(value=body, latency_ms=latency_ms)
    except Exception as exc:
        return QueryResult(error=str(exc), latency_ms=(time.perf_counter() - start) * 1000.0)
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def encode_command(*parts: Any) -> bytes:
    encoded = [f"*{len(parts)}\r\n".encode("ascii")]
    for part in parts:
        if isinstance(part, bytes):
            value = part
        else:
            value = str(part).encode("utf-8")
        encoded.append(f"${len(value)}\r\n".encode("ascii"))
        encoded.append(value)
        encoded.append(b"\r\n")
    return b"".join(encoded)


def read_line(sock_file) -> bytes:
    line = sock_file.readline()
    if not line:
        raise EOFError("connection closed while reading RESP line")
    if not line.endswith(b"\r\n"):
        raise RuntimeError(f"invalid RESP line: {line!r}")
    return line[:-2]


def read_exact(sock_file, size: int) -> bytes:
    data = sock_file.read(size)
    if data is None or len(data) != size:
        raise EOFError("connection closed while reading RESP payload")
    return data


def read_response(sock_file):
    prefix = read_exact(sock_file, 1)
    if prefix == b"+":
        return read_line(sock_file).decode("utf-8", errors="replace")
    if prefix == b"-":
        raise RedisErrorResponse(read_line(sock_file).decode("utf-8", errors="replace"))
    if prefix == b"$":
        size = int(read_line(sock_file))
        if size == -1:
            return NULL_BULK_STRING
        data = read_exact(sock_file, size)
        trailer = read_exact(sock_file, 2)
        if trailer != b"\r\n":
            raise RuntimeError("invalid RESP bulk string trailer")
        return data.decode("utf-8", errors="replace")
    if prefix == b"*":
        size = int(read_line(sock_file))
        if size == -1:
            return None
        return [read_response(sock_file) for _ in range(size)]
    raise RuntimeError(f"unsupported RESP response prefix: {prefix!r}")


def execute_redis_commands(host: str, port: int, commands: list[tuple[Any, ...]]) -> QueryResult:
    start = time.perf_counter()
    responses = []
    try:
        with socket.create_connection((host, port), timeout=10) as client:
            with client.makefile("rb") as sock_file:
                for command in commands:
                    client.sendall(encode_command(*command))
                    responses.append(read_response(sock_file))
                return QueryResult(value=responses[-1] if responses else None, latency_ms=(time.perf_counter() - start) * 1000.0)
    except RedisErrorResponse as exc:
        return QueryResult(error=f"Redis error: {exc}", value=responses, latency_ms=(time.perf_counter() - start) * 1000.0)
    except Exception as exc:
        return QueryResult(error=str(exc), value=responses, latency_ms=(time.perf_counter() - start) * 1000.0)


def execute_redis_commands_with_responses(host: str, port: int, commands: list[tuple[Any, ...]]) -> QueryResult:
    start = time.perf_counter()
    responses = []
    try:
        with socket.create_connection((host, port), timeout=10) as client:
            with client.makefile("rb") as sock_file:
                for command in commands:
                    client.sendall(encode_command(*command))
                    responses.append(read_response(sock_file))
                return QueryResult(value=responses, latency_ms=(time.perf_counter() - start) * 1000.0)
    except RedisErrorResponse as exc:
        responses.append(f"ERR {exc}")
        return QueryResult(error=f"Redis error: {exc}", value=responses, latency_ms=(time.perf_counter() - start) * 1000.0)
    except Exception as exc:
        return QueryResult(error=str(exc), value=responses, latency_ms=(time.perf_counter() - start) * 1000.0)


def display_value(value: Any) -> str:
    if value is NULL_BULK_STRING:
        return "(nil)"
    if isinstance(value, list):
        return "\n".join(display_value(item) for item in value)
    if value is None:
        return "(nil)"
    return str(value).rstrip("\n")


def pretty_response(value: Any) -> str:
    if isinstance(value, list):
        return "\n".join(f"{index + 1}) {pretty_response(item)}" for index, item in enumerate(value))
    return display_value(value)


def normalize_result_value(value: Any) -> str | None:
    if value is NULL_BULK_STRING or value is None:
        return None
    return display_value(value)


def parse_sql_single_value(result: QueryResult) -> str | None:
    if result.error or result.value is None:
        return None
    lines = [line for line in str(result.value).splitlines() if line]
    return lines[0] if lines else None


def parse_sql_key_values(result: QueryResult) -> dict[str, str]:
    if result.error or result.value is None:
        return {}
    rows = {}
    for line in str(result.value).splitlines():
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) == 2:
            rows[parts[0]] = parts[1]
    return rows


def parse_keys(text: str) -> list[str]:
    return [key.strip() for key in text.split(",") if key.strip()]


def has_duplicates(values: list[str]) -> bool:
    return len(set(values)) != len(values)


def build_lookup_requests(settings: dict[str, Any], keys: list[str]) -> tuple[str, list[tuple[Any, ...]], bool]:
    if not keys:
        raise ValueError("Key input is empty")

    table_name = f"{quote_identifier(settings['database'])}.{quote_identifier(settings['table'])}"
    key_column = quote_identifier(settings["key_column"])
    value_column = quote_identifier(settings["value_column"])
    sql_keys = [format_key_for_sql(key, settings["key_type"]) for key in keys]
    redis_db = str(settings["redis_db"])

    if len(keys) == 1:
        sql = f"SELECT {value_column} FROM {table_name} WHERE {key_column} = {sql_keys[0]} LIMIT 1 FORMAT TabSeparated"
        redis_commands = [("SELECT", redis_db), ("GET", keys[0])]
        return sql, redis_commands, False

    sql = (
        f"SELECT {key_column}, {value_column}\n"
        f"FROM {table_name}\n"
        f"WHERE {key_column} IN ({', '.join(sql_keys)})\n"
        "FORMAT TSV"
    )
    redis_commands = [("SELECT", redis_db), tuple(["MGET", *keys])]
    return sql, redis_commands, True


def redis_commands_text(commands: list[tuple[Any, ...]]) -> str:
    return "\n".join(" ".join(str(part) for part in command) for command in commands)


def parse_manual_redis_commands(text: str) -> list[tuple[str, ...]]:
    commands = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        commands.append(tuple(stripped.split()))
    return commands


def compare_results(keys: list[str], is_mget: bool, sql_result: QueryResult | None, redis_result: QueryResult | None):
    if sql_result is None or redis_result is None:
        return None, []
    if sql_result.error or redis_result.error:
        return None, []

    if not is_mget:
        sql_value = parse_sql_single_value(sql_result)
        redis_value = normalize_result_value(redis_result.value)
        return sql_value == redis_value, [{
            "key": keys[0],
            "sql_value": sql_value,
            "redis_value": redis_value,
            "match": sql_value == redis_value,
        }]

    sql_rows = parse_sql_key_values(sql_result)
    redis_values = redis_result.value if isinstance(redis_result.value, list) else []
    comparison = []
    for index, key in enumerate(keys):
        sql_value = sql_rows.get(key)
        redis_value = normalize_result_value(redis_values[index]) if index < len(redis_values) else None
        comparison.append({
            "key": key,
            "sql_value": sql_value,
            "redis_value": redis_value,
            "match": sql_value == redis_value,
        })
    return all(row["match"] for row in comparison), comparison


def render_query_result(result: QueryResult | None, raw_label: str) -> None:
    if result is None:
        st.info("Not run")
        return
    st.metric("Latency", f"{result.latency_ms:.3f} ms")
    if result.error:
        st.error(result.error)
    st.text_area(raw_label, display_value(result.value), height=180)


def render_overview_tab(settings: dict[str, Any]) -> None:
    st.subheader("Current configuration")
    rows = [
        {"Setting": "ClickHouse HTTP endpoint", "Value": f"{settings['http_host']}:{settings['http_port']}"},
        {"Setting": "Redis-compatible endpoint", "Value": f"{settings['redis_host']}:{settings['redis_port']}"},
        {"Setting": "Database", "Value": settings["database"]},
        {"Setting": "Table", "Value": settings["table"]},
        {"Setting": "Key column", "Value": settings["key_column"]},
        {"Setting": "Value column", "Value": settings["value_column"]},
        {"Setting": "Key type", "Value": settings["key_type"]},
        {"Setting": "Redis DB index", "Value": str(settings["redis_db"])},
        {"Setting": "Scope", "Value": "Read-only point lookups over prepared key-value tables"},
        {"Setting": "Supported commands", "Value": "PING, QUIT, SELECT, GET, MGET"},
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.write(
        "The demo sends equivalent lookup requests through two paths: regular SQL over HTTP "
        "and the Redis-compatible TCP endpoint."
    )

    st.subheader("Endpoint status")
    if st.button("Check endpoints", type="primary"):
        http_status = execute_clickhouse_http(settings["http_host"], settings["http_port"], "SELECT 1 FORMAT TabSeparated")
        redis_status = execute_redis_commands(settings["redis_host"], settings["redis_port"], [("PING",)])

        left, right = st.columns(2)
        with left:
            st.markdown("**ClickHouse HTTP endpoint**")
            if http_status.error:
                st.error(http_status.error)
            else:
                st.success(f"OK: {display_value(http_status.value)}")
            st.caption(f"{http_status.latency_ms:.3f} ms")

        with right:
            st.markdown("**Redis-compatible endpoint**")
            if redis_status.error:
                st.error(redis_status.error)
            else:
                st.success(f"OK: {display_value(redis_status.value)}")
            st.caption(f"{redis_status.latency_ms:.3f} ms")
    else:
        st.info("Click Check endpoints to run lightweight `SELECT 1` and `PING` checks.")


def render_compare_tab(settings: dict[str, Any]) -> None:
    default_keys = "0, 65536, 131072" if settings["key_type"] == "UInt64" else "key_1, key_2, key_3"
    keys_text = st.text_input("Comma-separated keys", default_keys)
    keys = parse_keys(keys_text)

    if has_duplicates(keys):
        st.warning("Duplicate keys are supported by Redis MGET; SQL comparison uses key lookup normalization for demo purposes.")

    try:
        sql, redis_commands, is_mget = build_lookup_requests(settings, keys)
    except ValueError as exc:
        st.error(str(exc))
        return

    button_left, button_middle, button_right = st.columns(3)
    with button_left:
        run_sql = st.button("Run SQL path")
    with button_middle:
        run_redis = st.button("Run Redis endpoint path")
    with button_right:
        run_both = st.button("Run both paths", type="primary")

    sql_result = None
    redis_result = None
    if run_sql or run_both:
        sql_result = execute_clickhouse_http(settings["http_host"], settings["http_port"], sql)
    if run_redis or run_both:
        redis_result = execute_redis_commands(settings["redis_host"], settings["redis_port"], redis_commands)

    left, right = st.columns(2)
    with left:
        st.subheader("SQL / HTTP path")
        st.caption("HTTP -> SQL parser -> analyzer/planner -> query pipeline -> storage")
        st.markdown("**SQL query**")
        st.code(sql, language="sql")
        st.markdown("**Low-level request**")
        st.code("POST /\n\n" + sql, language="http")
        render_query_result(sql_result, "SQL raw response")

    with right:
        st.subheader("Redis-compatible endpoint path")
        st.caption("RESP -> RedisProtocol -> RedisHandler -> IKeyValueEntity::getByKeys")
        st.markdown("**Redis command sequence**")
        st.code(redis_commands_text(redis_commands), language="text")
        st.markdown("**Low-level request**")
        st.code("RESP array commands over one TCP connection; binary RESP payload is not shown.", language="text")
        render_query_result(redis_result, "Redis parsed response")

    match, comparison = compare_results(keys, is_mget, sql_result, redis_result)
    st.subheader("Result comparison")
    if match is None:
        st.info("Comparison unavailable because one path was not run or one path failed.")
    elif match:
        st.success("Results match")
    else:
        st.warning("Results differ")

    if comparison:
        st.dataframe(comparison, use_container_width=True, hide_index=True)


def render_request_path_tab() -> None:
    st.subheader("Request paths")
    left, right = st.columns(2)
    with left:
        st.markdown("**SQL path**")
        st.markdown(
            """
            <div style="border:1px solid #d0d7de; border-radius:8px; padding:14px; line-height:2.0">
            Client -> HTTP -> SQL parser -> analyzer/planner -> query pipeline -> storage -> result
            </div>
            """,
            unsafe_allow_html=True,
        )
    with right:
        st.markdown("**Redis-compatible endpoint path**")
        st.markdown(
            """
            <div style="border:1px solid #d0d7de; border-radius:8px; padding:14px; line-height:2.0">
            Client -> TCP :9006 / RESP -> RedisProtocol -> RedisHandler -> DatabaseCatalog -> IKeyValueEntity::getByKeys -> RESP response
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.info("The Redis-compatible path avoids SQL parsing, planning, and query pipeline construction for supported GET/MGET requests.")
    st.write("The Redis-compatible path still reads ClickHouse-managed data through the existing `IKeyValueEntity` abstraction.")


def render_manual_console_tab(settings: dict[str, Any]) -> None:
    st.warning("The SQL console is intended for local defense demonstration. Use read-only SELECT queries.")
    left, right = st.columns(2)
    with left:
        st.subheader("SQL console")
        default_sql = f"SELECT {settings['value_column']} FROM {settings['database']}.{settings['table']} WHERE {settings['key_column']} = 0 FORMAT TabSeparated"
        sql_text = st.text_area("SQL", default_sql, height=150)
        if st.button("Run SQL", type="primary"):
            result = execute_clickhouse_http(settings["http_host"], settings["http_port"], sql_text)
            st.metric("Latency", f"{result.latency_ms:.3f} ms")
            if result.error:
                st.error(result.error)
            st.text_area("Raw response", display_value(result.value), height=220)

    with right:
        st.subheader("Redis console")
        default_redis = f"SELECT {settings['redis_db']}\nGET 0"
        redis_text = st.text_area("Redis commands, one command per line", default_redis, height=150)
        st.caption(f"Example MGET:\nSELECT {settings['redis_db']}\nMGET 0 65536 131072")
        if st.button("Run Redis commands", type="primary"):
            commands = parse_manual_redis_commands(redis_text)
            result = execute_redis_commands_with_responses(settings["redis_host"], settings["redis_port"], commands)
            st.metric("Latency", f"{result.latency_ms:.3f} ms")
            if result.error:
                st.error(result.error)
            st.text_area("Parsed response list", pretty_response(result.value), height=220)


def render_proof_tab(settings: dict[str, Any]) -> None:
    st.subheader("Copyable terminal checks")
    st.code("ss -ltnp | grep 9006", language="bash")
    st.code("pgrep -a clickhouse", language="bash")
    st.code('watch -n 1 "ss -ltnp | grep 9006 || true"', language="bash")

    query_log_cli = f'''clickhouse-client --query "
SYSTEM FLUSH LOGS;
SELECT event_time, query_duration_ms, left(query, 160) AS query
FROM system.query_log
WHERE event_time > now() - INTERVAL 5 MINUTE
  AND type = 'QueryFinish'
  AND query ILIKE '%{settings["database"]}%'
  AND query ILIKE '%{settings["table"]}%'
ORDER BY event_time DESC
LIMIT 10
FORMAT PrettyCompact
"'''
    st.code(query_log_cli, language="bash")
    st.code(f"tail -f {settings['log_path']} | grep -i redis", language="bash")

    st.write("SQL path requests should appear in `system.query_log`.")
    st.write("Redis `GET`/`MGET` requests are handled by `RedisHandler` and should not appear as SQL queries in `system.query_log`.")
    st.write("`system.query_log` can flush with a delay; the demo check runs `SYSTEM FLUSH LOGS` before reading when possible.")
    st.write("`RedisHandler` logs depend on current server logging configuration and may require trace/debug logs.")

    st.subheader("Live SQL query_log check")
    if st.button("Check recent SQL query_log", type="primary"):
        flush_result = execute_clickhouse_http(settings["http_host"], settings["http_port"], "SYSTEM FLUSH LOGS")
        if flush_result.error:
            st.warning(f"Could not flush logs before reading query_log: {flush_result.error}")

        database_filter = quote_string(f"%{settings['database']}%")
        table_filter = quote_string(f"%{settings['table']}%")
        query = (
            "SELECT event_time, query_duration_ms, left(query, 160) AS query\n"
            "FROM system.query_log\n"
            f"WHERE event_time > now() - INTERVAL {int(settings['query_log_lookback_minutes'])} MINUTE\n"
            "  AND type = 'QueryFinish'\n"
            f"  AND query ILIKE {database_filter}\n"
            f"  AND query ILIKE {table_filter}\n"
            "ORDER BY event_time DESC\n"
            "LIMIT 10\n"
            "FORMAT TSVWithNames"
        )
        result = execute_clickhouse_http(settings["http_host"], settings["http_port"], query)
        st.code(query, language="sql")
        st.metric("Latency", f"{result.latency_ms:.3f} ms")
        if result.error:
            st.error(result.error)
        else:
            st.text_area("query_log result", display_value(result.value), height=260)


def render_benchmark_context_tab() -> None:
    st.subheader("Static benchmark context")
    st.write("No benchmarks are run from this UI.")

    st.markdown("**100k String-key benchmark**")
    st.dataframe([
        {"Metric": "HTTP SQL best QPS", "Value": "about 2827"},
        {"Metric": "ClickHouse Redis-compatible endpoint best QPS", "Value": "about 84491"},
        {"Metric": "Redis raw reference best QPS", "Value": "about 90535"},
        {"Metric": "p99, batch-size 1, concurrency 1, HTTP SQL", "Value": "about 1.021 ms"},
        {"Metric": "p99, batch-size 1, concurrency 1, Redis endpoint", "Value": "about 0.021 ms"},
        {"Metric": "p99, batch-size 1, concurrency 1, Redis raw reference", "Value": "about 0.015 ms"},
    ], use_container_width=True, hide_index=True)

    st.markdown("**`bench.kv_test` 10M-row `UInt64` benchmark**")
    st.dataframe([
        {"Metric": "Table", "Value": "bench.kv_test"},
        {"Metric": "Rows", "Value": "10M"},
        {"Metric": "Key", "Value": "UInt64"},
        {"Metric": "Value", "Value": "String"},
        {"Metric": "Extra columns", "Value": "extra1 UInt32, extra2 Float64"},
        {"Metric": "Engine", "Value": "EmbeddedRocksDB"},
        {"Metric": "HTTP SQL best QPS", "Value": "2763.65"},
        {"Metric": "Redis endpoint best QPS", "Value": "80652.74"},
        {"Metric": "p99, batch-size 1, concurrency 1, HTTP SQL", "Value": "1.059 ms"},
        {"Metric": "p99, batch-size 1, concurrency 1, Redis endpoint", "Value": "0.028 ms"},
        {"Metric": "p99, batch-size 100, concurrency 32, HTTP SQL", "Value": "52.755 ms"},
        {"Metric": "p99, batch-size 100, concurrency 32, Redis endpoint", "Value": "19.824 ms"},
    ], use_container_width=True, hide_index=True)

    st.subheader("Caveats")
    st.write("- These are hot-cache local-loopback microbenchmarks.")
    st.write("- QPS is per command/request, not per key.")
    st.write("- For `MGET`, approximate key throughput equals command QPS multiplied by batch size.")
    st.write("- The Redis-compatible endpoint is not a general-purpose Redis-compatible server.")
    st.write("- Redis is used as an external reference baseline, not something we claim to outperform generally.")
    st.write("- These numbers are live-demo context, not evidence for deployment-wide behavior.")


def main() -> None:
    st.set_page_config(page_title="ClickHouse Redis-compatible Endpoint Demo", layout="wide")
    st.title("ClickHouse Redis-compatible Endpoint Demo")
    st.write(
        "This demo compares the regular ClickHouse HTTP SQL path with the Redis-compatible "
        "read-only endpoint for prepared key-value tables."
    )
    st.warning(
        "This endpoint is not a general-purpose Redis-compatible server. It currently supports "
        "read-only GET/MGET over IKeyValueEntity-compatible tables."
    )
    st.info(
        "Latency shown here is measured by the demo client and is intended for live illustration, "
        "not as a benchmark replacement."
    )

    with st.sidebar:
        st.header("Connection")
        http_host = st.text_input("ClickHouse HTTP host", "127.0.0.1")
        http_port = st.number_input("ClickHouse HTTP port", min_value=1, max_value=65535, value=8123)
        redis_host = st.text_input("Redis endpoint host", "127.0.0.1")
        redis_port = st.number_input("Redis endpoint port", min_value=1, max_value=65535, value=9006)
        redis_db = st.number_input("Redis DB index", min_value=0, value=0)

        st.header("Table")
        database = st.text_input("Database", "bench")
        table = st.text_input("Table", "kv_test")
        key_column = st.text_input("Key column", "key")
        value_column = st.text_input("Value column", "value")
        key_type = st.selectbox("Key type", ["UInt64", "String"], index=0)

        st.header("Proof")
        query_log_lookback_minutes = st.number_input("Query log lookback minutes", min_value=1, max_value=120, value=5)
        log_path = st.text_input("ClickHouse server log path", "/var/log/clickhouse-server/clickhouse-server.log")

    settings = {
        "http_host": http_host,
        "http_port": int(http_port),
        "redis_host": redis_host,
        "redis_port": int(redis_port),
        "redis_db": int(redis_db),
        "database": database,
        "table": table,
        "key_column": key_column,
        "value_column": value_column,
        "key_type": key_type,
        "query_log_lookback_minutes": int(query_log_lookback_minutes),
        "log_path": log_path,
    }

    tabs = st.tabs([
        "Overview",
        "Compare GET/MGET",
        "Request Path",
        "Manual Console",
        "Proof / Live Verification",
        "Benchmark Context",
    ])
    with tabs[0]:
        render_overview_tab(settings)
    with tabs[1]:
        render_compare_tab(settings)
    with tabs[2]:
        render_request_path_tab()
    with tabs[3]:
        render_manual_console_tab(settings)
    with tabs[4]:
        render_proof_tab(settings)
    with tabs[5]:
        render_benchmark_context_tab()


if __name__ == "__main__":
    main()
