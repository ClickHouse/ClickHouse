#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" <<'PY'
import shlex
import subprocess
import sys
import uuid

client = shlex.split(sys.argv[1])
suffix = uuid.uuid4().hex
table = f"input_timezone_{suffix}"
users = [f"input_timezone_{suffix}_{index}" for index in range(2)]
options = {
    "session_timezone": None,
    "apply_settings_from_server": 0,
    "use_client_time_zone": 0,
    "async_insert": 0,
    "input_format_parallel_parsing": 0,
    "input_format_defaults_for_omitted_fields": 0,
}


def arguments(overrides):
    result = []
    index = 0
    aliases = {"u": "user"}
    while index < len(client):
        argument = client[index]
        name = argument.split("=", 1)[0].lstrip("-")
        name = aliases.get(name, name)
        if argument.startswith("-") and name in overrides:
            if "=" not in argument and index + 1 < len(client) and not client[index + 1].startswith("-"):
                index += 1
        else:
            result.append(argument)
        index += 1
    return result + [f"--{name}={value}" for name, value in overrides.items() if value is not None]


def run(query, *, data=None, overrides=None):
    result = subprocess.run(
        arguments(options | (overrides or {})) + ["--query", query],
        input=data,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (query, result.stderr)
    return result.stdout


database = run("SELECT currentDatabase() FORMAT TSV").strip().replace("`", "``")
qualified_table = f"`{database}`.{table}"
structure = "ts DateTime, precise DateTime64(3), fixed DateTime('UTC')"
structure_sql = structure.replace("'", "''")
payload = "2026-01-15 12:00:00,2026-01-15 12:00:00.123,2026-01-15 12:00:00\n"
read_values = f"SELECT toUInt32(ts), toUnixTimestamp64Milli(precise), toUInt32(fixed) FROM {qualified_table} FORMAT TSV"
created_users = []
try:
    run(f"CREATE TABLE {qualified_table} ({structure}) ENGINE=Memory")
    for index, (timezone, expected) in enumerate(
        (
            ("America/New_York", "1768496400\t1768496400123\t1768478400\n"),
            ("Asia/Tokyo", "1768446000\t1768446000123\t1768478400\n"),
        )
    ):
        user = users[index]
        # The timezone is known only to the server; parsing must use the dedicated update packet.
        run(f"CREATE USER {user} SETTINGS session_timezone='{timezone}', apply_settings_from_server=0")
        created_users.append(user)
        run(f"GRANT INSERT, SELECT ON {qualified_table} TO {user}")
        run(
            f"INSERT INTO {qualified_table} SELECT * FROM input('{structure_sql}') FORMAT CSV",
            data=payload,
            overrides={"user": user, "password": "", "input_format_defaults_for_omitted_fields": index},
        )
        actual = run(read_values)
        assert actual == expected, (timezone, actual, expected)
        run(f"TRUNCATE TABLE {qualified_table}")
    print("server timezone precedes input schema with and without column metadata")

    # Explicit SQL settings already reach the client before it sends the query.
    run(
        f"INSERT INTO {qualified_table} SELECT * FROM input('{structure_sql}') SETTINGS session_timezone='America/New_York' FORMAT CSV",
        data=payload,
    )
    assert run(read_values) == "1768496400\t1768496400123\t1768478400\n"
    print("explicit query timezone and timezone-qualified column preserved")
finally:
    for user in created_users:
        run(f"DROP USER {user}")
    run(f"DROP TABLE IF EXISTS {qualified_table}")
PY
