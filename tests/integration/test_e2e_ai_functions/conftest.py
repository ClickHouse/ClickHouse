"""Fixtures for the AI-function end-to-end suite.

Gating is two independent things (README.md section 2):

* `pytest.mark.e2e` in every test module keeps the suite out of CI, since `pytest.ini`
  deselects that marker by default.
* configuration decides whether the *live* modules run: `requires_live_endpoint` skips
  them with a readable reason when the resolved target has no usable credentials. There
  is no separate enable flag.

The mock-driven modules need neither credentials nor an endpoint.
"""

import json
import os
import shutil
import subprocess
import time

import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

from . import config as ai_config
from . import corpus as ai_corpus
from .asserts import AI_SETTINGS, read_ai_events, unique_query_id

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MOCK_PORT = 18124

# Resolved once at import: `requires_live_endpoint` is a mark, and marks are evaluated at
# collection time, so it cannot depend on a fixture.
CFG = ai_config.resolve()

requires_live_endpoint = pytest.mark.skipif(
    not CFG.live_configured,
    reason=f"live endpoint not configured: {CFG.live_skip_reason}",
)

# Model-decided assertions - "obeys this instruction", "honors `dimensions`" - cannot hold
# on a stand-in model, so they skip rather than fail. README.md section 5.
requires_capable_model = pytest.mark.skipif(
    CFG.toy_model,
    reason=f"target '{CFG.target.name}' is marked toy_model",
)


def _host_gateway():
    """An address of the docker host, reachable from inside a container.

    The design doc suggests reading the default route from inside the container, but the
    endpoint has to be known *before* the container starts, since it is passed as an
    environment variable. The host's `docker0` address is reachable from any bridge
    network and is resolvable up front, so use that.
    """
    for command in (
        ["ip", "-4", "-o", "addr", "show", "docker0"],
        ["hostname", "-I"],
    ):
        if not shutil.which(command[0]):
            continue
        try:
            output = subprocess.run(
                command, capture_output=True, text=True, timeout=10
            ).stdout
        except (OSError, subprocess.SubprocessError):
            continue
        for token in output.replace("/", " ").split():
            if token.count(".") == 3 and all(
                part.isdigit() for part in token.split(".")
            ):
                return token
    return ""


if CFG.needs_host_gateway:
    gateway = _host_gateway()
    if gateway:
        CFG = CFG.with_host_gateway(gateway)

# Always pass every variable the collections config references. An unset variable is not
# an error - `from_env` logs a warning and leaves the element as written - so a missing
# endpoint would surface much later as "collection must have 'endpoint'". Passing empty
# strings keeps that failure mode out of the picture.
INSTANCE_ENV = {
    "AI_E2E_CHAT_ENDPOINT": CFG.chat_endpoint,
    "AI_E2E_EMBED_ENDPOINT": CFG.embed_endpoint,
    "AI_E2E_CHAT_MODEL": CFG.chat_model,
    "AI_E2E_API_KEY": CFG.api_key,
}

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/ai_e2e_collections.xml"],
    env_variables=dict(INSTANCE_ENV),
    stay_alive=True,
)


class _RedactKey(logging.Filter):
    """Keep the API key out of captured log records.

    `helpers/cluster.py` logs the whole `env_variables` dict at DEBUG, `pytest.ini` sets
    `log_level = DEBUG`, and captured logs are attached to failures. Scrubbing the `.env`
    file on disk does nothing about that path.
    """

    def __init__(self, secret):
        super().__init__()
        self.secret = secret

    def filter(self, record):
        if not self.secret:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        if self.secret in message:
            record.msg = message.replace(self.secret, "[scrubbed]")
            record.args = ()
        return True


if CFG.api_key:
    logging.getLogger().addFilter(_RedactKey(CFG.api_key))
    for _name in ("root", "helpers.cluster", "docker"):
        logging.getLogger(_name).addFilter(_RedactKey(CFG.api_key))


def _scrub_env_files():
    """Remove the API key from the `.env` files the harness writes on the host.

    `env_variables` are written verbatim to `_instances-gwN/.env`, which is inside the
    tree the integration job packages into `logs.tar.gz` on failure. docker-compose reads
    that file only at `up` time, so rewriting it after the cluster is running is safe.
    """
    if not CFG.api_key:
        return
    for root, _dirs, files in os.walk(SCRIPT_DIR):
        for name in files:
            if name != ".env":
                continue
            path = os.path.join(root, name)
            try:
                with open(path) as handle:
                    content = handle.read()
                if CFG.api_key not in content:
                    continue
                with open(path, "w") as handle:
                    handle.write(content.replace(CFG.api_key, "[scrubbed]"))
            except OSError:
                pass


class MockControl:
    """Client for `latency_mock_server.py`, driven through the container.

    The mock listens on the container's loopback interface, which is what exempts it from
    the plaintext-endpoint check, so the test process talks to it with `curl` inside the
    container rather than over the network.
    """

    def __init__(self, instance, port):
        self.instance = instance
        self.port = port

    @property
    def chat_endpoint(self):
        return f"http://localhost:{self.port}/v1/chat/completions"

    @property
    def embed_endpoint(self):
        return f"http://localhost:{self.port}/v1/embeddings"

    def _curl(self, args):
        raw = self.instance.exec_in_container(["curl", "-sS"] + args)
        try:
            return json.loads(raw)
        except ValueError:
            raise RuntimeError(f"mock returned non-JSON: {raw[:200]}")

    def configure(self, **kwargs):
        return self._curl(
            [
                "-XPOST",
                "-H",
                "Content-Type: application/json",
                "-d",
                json.dumps(kwargs),
                f"http://localhost:{self.port}/config",
            ]
        )

    def reset(self):
        return self._curl(["-XPOST", "-d", "{}", f"http://localhost:{self.port}/reset"])

    def stats(self):
        return self._curl([f"http://localhost:{self.port}/stats"])


def _start_mock(instance, port):
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "latency_mock_server.py"),
        "/latency_mock_server.py",
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 /latency_mock_server.py {port} "
            f"> /var/log/clickhouse-server/latency_mock_server.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    try:
        wait_condition(
            lambda: instance.exec_in_container(
                ["curl", "-s", f"http://localhost:{port}/health"], nothrow=True
            ),
            lambda result: result == "OK",
            max_attempts=40,
            delay=0.5,
        )
    except Exception as error:
        log = instance.exec_in_container(
            ["cat", "/var/log/clickhouse-server/latency_mock_server.log"], nothrow=True
        )
        raise RuntimeError(f"mock server failed to start. Log:\n{log}") from error


def _create_mock_collections(instance, port):
    """Mock collections are plain DDL: their `api_key` is a literal, not a secret."""
    for name, path in (
        ("ai_e2e_mock_chat", "/v1/chat/completions"),
        ("ai_e2e_mock_embed", "/v1/embeddings"),
    ):
        instance.query(f"DROP NAMED COLLECTION IF EXISTS {name}")
        model = ", model = 'mock-model'" if name.endswith("chat") else ""
        instance.query(
            f"CREATE NAMED COLLECTION {name} AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{port}{path}', "
            f"api_key = 'mock-key'{model}"
        )


class Runner:
    """Runs a query, then reads its own AI counters back from `system.query_log`."""

    def __init__(self, instance, cfg):
        self.instance = instance
        self.cfg = cfg

    def settings(self, extra=None, counting=False, rows=0):
        settings = dict(AI_SETTINGS)
        settings["ai_function_request_timeout_sec"] = ai_config.request_timeout_sec(
            self.cfg
        )
        if counting:
            # Quotas are constructed per `executeImpl` call and embedding batches are
            # formed per block, so an exact call count is only well defined for a single
            # block read by a single thread.
            settings["max_block_size"] = max(rows, 1024)
            settings["max_threads"] = 1
            # `preferred_block_size_bytes` can split a block below `max_block_size`, which
            # would restart per-block batching and quota accounting mid-query.
            settings["preferred_block_size_bytes"] = 0
        if self.cfg.insecure_endpoint_needed:
            settings["ai_function_allow_insecure_endpoint"] = 1
        if extra:
            settings.update(extra)
        return settings

    def run(self, sql, case="query", settings=None, counting=False, rows=0, timeout=None):
        query_id = unique_query_id(case)
        result = self.instance.query(
            sql,
            settings=self.settings(settings, counting=counting, rows=rows),
            query_id=query_id,
            timeout=timeout,
        )
        events = read_ai_events(self.instance, query_id)
        return result, events

    def error(self, sql, case="query", settings=None, timeout=None):
        """Run a query that is expected to throw and return the error text.

        Deliberately returns no counters: the five AI ProfileEvents are incremented after
        the row loop, so a query that throws records none of them. Cases that need call
        counts for a failing query read the mock's /stats instead.
        """
        return self.instance.query_and_get_error(
            sql,
            settings=self.settings(settings),
            timeout=timeout,
        )


@pytest.fixture(scope="session")
def cfg():
    return CFG


@pytest.fixture(scope="session")
def started_cluster():
    try:
        cluster.start()
        _scrub_env_files()
        _start_mock(node, MOCK_PORT)
        _create_mock_collections(node, MOCK_PORT)
        yield cluster
    finally:
        _scrub_env_files()
        cluster.shutdown()


@pytest.fixture(scope="session")
def mock(started_cluster):
    control = MockControl(node, MOCK_PORT)
    control.reset()
    return control


@pytest.fixture()
def clean_mock(mock):
    """A mock with default behavior and zeroed counters, per test."""
    mock.reset()
    yield mock
    mock.reset()


@pytest.fixture(scope="session")
def q(started_cluster):
    return Runner(node, CFG)


@pytest.fixture(scope="session")
def instance(started_cluster):
    return node


def _session_has_live_items(session):
    """Whether this worker actually collected any live (paid) test."""
    live_modules = ("test_basic_e2e", "test_params", "test_concurrency", "test_latency_real")
    return any(
        any(module in item.nodeid for module in live_modules) for item in session.items
    )


@pytest.fixture(scope="session", autouse=True)
def preflight(started_cluster, request):
    """A0. Raises once, before any test, rather than letting every case time out.

    Session-scoped and autouse because module order is duration-derived under both xdist
    schedules, so no test file can be guaranteed to run first.

    The live checks run only when this worker collected a live module: an autouse fixture
    applies to every test in the directory, and the mock-driven modules need no endpoint,
    no key and no spend. Note that session scope means once per xdist worker, so A0-2
    costs one call per worker that runs live tests.
    """
    # A0-3: the binary under test must match the tree. A failed build leaves the previous
    # binary in place, so comparing the server version to the source tree is the only
    # check that catches it.
    _assert_binary_matches_tree()

    if not CFG.live_configured or not _session_has_live_items(request.session):
        return

    # A0-1: reachability, diagnosed from inside the container.
    status = node.exec_in_container(
        [
            "bash",
            "-c",
            f"curl -sS -o /dev/null -w '%{{http_code}}' --max-time 20 "
            f"-X POST {CFG.chat_endpoint} "
            f"-H 'Content-Type: application/json' "
            f"-H 'Authorization: Bearer {CFG.api_key}' "
            f"-d '{{\"model\":\"{CFG.chat_model}\",\"messages\":"
            f'[{{"role":"user","content":"ping"}}],"max_tokens":1}}\' || true',
        ],
        nothrow=True,
    ).strip()
    if status in ("", "000"):
        raise RuntimeError(
            f"cannot reach {CFG.chat_endpoint} from the container (curl wrote '{status}'): "
            "check container egress, DNS, and CA certificates"
        )
    if status in ("401", "403"):
        raise RuntimeError(f"endpoint rejected the key: HTTP {status}")

    # A0-4: refuse to start a run that would exceed the spend cap.
    texts = ai_corpus.all_live_texts(CFG.data_scale)
    chat_calls = (
        len(ai_corpus.arith(CFG.data_scale))
        + len(ai_corpus.classify()) * 2
        + len(ai_corpus.extract()) * 2
        + len(ai_corpus.translate())
    )
    estimate = ai_config.estimate_spend(CFG, texts, chat_calls)
    print(f"\n[ai-e2e] spend estimate: {estimate.format()}")
    if estimate.priced and estimate.usd > CFG.max_est_usd:
        raise RuntimeError(
            f"estimated ${estimate.usd:.4f} exceeds AI_E2E_MAX_EST_USD="
            f"{CFG.max_est_usd}; lower AI_E2E_DATA_SCALE or raise the cap"
        )

    # A0-2: one real call, so a broken collection fails here and not in every case.
    node.query(
        "SELECT aiGenerate('Reply with the word OK.', map('credentials', 'ai_e2e_chat'))",
        settings=Runner(node, CFG).settings(),
    )


def _assert_binary_matches_tree():
    """A0-3: the binary under test must be built from this tree.

    Compares git hashes, not `version()`: the version string only changes on a release
    bump, so it matches for every commit in a cycle - including a stale binary left behind
    by a failed build, which is the trap this exists to catch.
    """
    built_from = node.query(
        "SELECT value FROM system.build_options WHERE name = 'GIT_HASH'"
    ).strip()
    tree_sha = ""
    try:
        tree_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=SCRIPT_DIR,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return
    if not built_from or not tree_sha:
        return
    if built_from != tree_sha:
        print(
            f"\n[ai-e2e] WARNING: binary was built from {built_from[:12]}, tree is at "
            f"{tree_sha[:12]}. Results describe the built commit, not the checkout."
        )


def load_table(instance, name, columns, rows):
    """(Re)create a single-part MergeTree table and load `rows` into it."""
    instance.query(f"DROP TABLE IF EXISTS {name} SYNC")
    schema = ", ".join(f"{column} {type_}" for column, type_ in columns)
    instance.query(
        f"CREATE TABLE {name} ({schema}) ENGINE = MergeTree ORDER BY id"
    )
    if rows:
        values = []
        for row in rows:
            cells = []
            for column, type_ in columns:
                value = row[column]
                if type_.startswith("String") or type_.startswith("Nullable(String"):
                    cells.append("NULL" if value is None else _quote(value))
                else:
                    cells.append(str(value))
            values.append("(" + ", ".join(cells) + ")")
        instance.query(f"INSERT INTO {name} VALUES " + ", ".join(values))
    # Exact call counts assume one block from one part.
    instance.query(f"OPTIMIZE TABLE {name} FINAL")


def _quote(value):
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
