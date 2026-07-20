import subprocess
from types import SimpleNamespace

from ci.jobs import parser_memory_check


def test_master_profiler_url_uses_clickhouse_examples(monkeypatch):
    sha = "a" * 40
    checked_urls = []
    monkeypatch.setattr(
        parser_memory_check,
        "Info",
        lambda: SimpleNamespace(get_kv_data=lambda key: [sha]),
    )
    monkeypatch.setattr(
        parser_memory_check.Shell,
        "check",
        lambda command: checked_urls.append(command) or True,
    )

    url = parser_memory_check.get_merge_base_profiler_url()

    assert url.endswith(f"/REFs/master/{sha}/build_arm_binary/clickhouse-examples")
    assert url in checked_urls[0]


def test_profiler_uses_clickhouse_examples_multicall(tmp_path, monkeypatch):
    heap_before = tmp_path / "before.heap"
    heap_after = tmp_path / "after.heap"
    heap_before.touch()
    heap_after.touch()
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            args,
            0,
            stdout="8\t100\t124\t24\n",
            stderr=(f"Profile before: {heap_before}\n" f"Profile after: {heap_after}\n"),
        )

    monkeypatch.setattr(parser_memory_check.subprocess, "run", fake_run)

    result = parser_memory_check.run_profiler_collect_heap(
        "/tmp/clickhouse-examples", "SELECT 1", str(tmp_path / "profile")
    )

    assert result["error"] is None
    assert result["jemalloc_diff"] == 24
    assert calls[0][0] == [
        "/tmp/clickhouse-examples",
        "parser_memory_profiler",
        "--profile",
        str(tmp_path / "profile"),
    ]


def test_profiler_rejects_malformed_tsv(monkeypatch):
    monkeypatch.setattr(
        parser_memory_check.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args, 0, stdout="not-tsv\n", stderr=""
        ),
    )

    result = parser_memory_check.run_profiler_collect_heap(
        "/tmp/clickhouse-examples", "SELECT 1", "/tmp/profile"
    )

    assert result == {
        "error": "malformed profiler output: expected 4 TSV fields, got 1"
    }


def test_batch_symbolize_uses_clickhouse_examples_multicall(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="symbolized\n")

    monkeypatch.setattr(parser_memory_check.subprocess, "run", fake_run)

    assert parser_memory_check.batch_symbolize(
        "/tmp/clickhouse-examples", ["before.heap", "after.heap"]
    )
    assert calls[0][0] == [
        "/tmp/clickhouse-examples",
        "parser_memory_profiler",
        "--symbolize-batch",
        "before.heap",
        "after.heap",
    ]
