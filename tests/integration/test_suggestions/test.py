import logging
import pytest
import re
import time

from helpers.client import QueryRuntimeException
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.uclient import client, prompt


TEST_USER = "test_user"
TEST_PASSWORD = "123"
DEFAULT_ENCODING = "utf-8"
CR = "\r"
TAB = "\t"
SPACE = " "
BELL = '\x07'
CTRL_U = "\x15"
YES = "y"
PROMPT_RE = re.compile(":\\) ")
DISPLAY_ALL_RE = re.compile(r"Display all \d+ possibilities\? \(y or n\)")
MORE_RE = re.compile("--More--")
ANSI_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
CURSOR_MOVE_RE = re.compile(r"\x1b\[\d+[GHF]|\r(?!\n)")
TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
QUIET_WINDOW = 0.5

cluster = ClickHouseCluster(__file__)
system_completions_based_suggestions_server = cluster.add_instance(
    "forward",
    main_configs=["configs/macros.xml"],
    dictionaries=["configs/dictionary.xml"],
    user_configs=["configs/users.xml"]
)
system_tables_based_suggestions_server = cluster.add_instance(
    "backward",
    image="clickhouse/clickhouse-server",
    tag="25.7",
    with_installed_binary=True,
    main_configs=["configs/macros.xml"],
    dictionaries=["configs/dictionary.xml"],
    user_configs=["configs/users.xml"]
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def fetch_suggestions(client, prefix, timeout=5):
    word_to_complete = prefix.split(SPACE)[-1]
    timeleft = timeout

    client.send(prefix, eol="")
    client.expect(re.escape(prefix), timeout=QUIET_WINDOW)
    client.send(TAB, eol="")

    buffer = ""
    suggestions = ""
    while True:
        start_time = time.time()
        if timeleft < 0 :
            break

        data = client.read(timeout=QUIET_WINDOW)
        if data:
            buffer = (buffer + data) if buffer else data

        if m := DISPLAY_ALL_RE.search(buffer):
            suggestions += buffer[:m.start()]
            buffer = buffer[m.end():]
            client.send(YES, eol="")
        elif m := MORE_RE.search(buffer):
            suggestions += buffer[:m.start()]
            buffer = buffer[m.end():]
            client.send(SPACE, eol="")
        elif m:= PROMPT_RE.search(buffer):
            suggestions += buffer[:m.start()]
            return [token for token in TOKEN_RE.findall(ANSI_RE.sub("", suggestions)) if token.startswith(word_to_complete)]
        elif CURSOR_MOVE_RE.search(buffer) and (time.time() - start_time) >= QUIET_WINDOW:
            inline_suggestion = TOKEN_RE.findall(ANSI_RE.sub("", buffer))[0]
            if len(inline_suggestion) > len(word_to_complete):
                # Clean up the edited line and return to a fresh prompt
                client.send(CTRL_U, eol="")
                client.send(CR, eol="")
                client.expect(PROMPT_RE)
                return [inline_suggestion]
        elif BELL in buffer:
            time.sleep(0.05)

        timeleft -= time.time() - start_time

    raise Exception(f"Timeout while fetching suggestions for {prefix} (buffer={buffer}, suggestions={suggestions})")

def start_client_command(start_cluster, instance):
    return f"{start_cluster.get_client_cmd()} --host {instance.ip_address} --port 9000 -u {TEST_USER} --password {TEST_PASSWORD} --wait_for_suggestions_to_load --highlight=0"

def test_suggestions_backwards_compatibility_for_unique_suggestions_prefix(start_cluster):
    unique_suggestions_tokens = [
        # function
        "concatAssumeInjective",
        # table_engine
        "ReplacingMergeTree",
        # format
        "JSONEachRow",
        # table function
        "clusterAllReplicas",
        # data type
        "SimpleAggregateFunction",
        # setting
        "max_concurrent_queries_for_all_users",
        # macros
        "defaultMac",
        # policy
        "default",
        # aggregate function combinator pair
        "uniqCombined64ForEach",
        # keyword
        "CHANGEABLE_IN_READONLY",
        # database
        "system",
        # table
        "aggregate_function_combinators",
        # column
        "is_internal",
        # dictionary
        "default_dictionary"
    ]
    with client(command=start_client_command(start_cluster, system_tables_based_suggestions_server)) as backward_cl:
        with client(command=start_client_command(start_cluster, system_completions_based_suggestions_server)) as forward_cl:
            backward_cl.expect(prompt)
            backward_cl.send("describe table system.completions")
            backward_cl.expect("UNKNOWN_TABLE") # completions table doesn't exist in this version of the server
            backward_cl.expect(prompt)

            forward_cl.expect(prompt)
            for unique_suggestions_token in unique_suggestions_tokens:
                unique_suggestions_prefix = unique_suggestions_token[:-2]
                backward_server_completions = fetch_suggestions(backward_cl, unique_suggestions_prefix)
                forward_server_completions = fetch_suggestions(forward_cl, unique_suggestions_prefix)
                assert backward_server_completions == [unique_suggestions_token]
                assert forward_server_completions == [unique_suggestions_token]
