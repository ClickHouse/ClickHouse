import datetime
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../tests/integration"))

from full_log_plugin import FullLogPlugin, phase_sections, stamp


class _Report:
    def __init__(self, nodeid, when, sections):
        self.nodeid = nodeid
        self.when = when
        self.sections = sections


def test_stamp_prepends_only_when_missing():
    now = datetime.datetime(2020, 1, 2, 3, 4, 5, 678000)
    assert (
        stamp("plain print line", now) == "2020-01-02 03:04:05.678000 plain print line"
    )
    logged = "2019-12-31 23:59:59.000000 [ 1 ] INFO : already stamped"
    assert stamp(logged, now) == logged


def test_phase_sections_keeps_only_current_phase():
    report = _Report(
        "test.py::t",
        "call",
        [
            ("Captured stdout setup", "s"),
            ("Captured log setup", "s"),
            ("Captured stdout call", "c"),
            ("Captured log call", "c"),
        ],
    )
    assert phase_sections(report) == [
        ("Captured stdout call", "c"),
        ("Captured log call", "c"),
    ]


def test_plugin_writes_all_phases_deduped(tmp_path):
    path = tmp_path / "pytest.full.log"
    plugin = FullLogPlugin(str(path))
    # Every phase report carries all accumulated sections; the plugin must emit each once.
    all_sections = [
        (f"Captured stdout {p}", f"print-{p}") for p in ("setup", "call", "teardown")
    ]
    for when in ("setup", "call", "teardown"):
        plugin.pytest_runtest_logreport(
            _Report(
                "test.py::t",
                when,
                all_sections[: ("setup", "call", "teardown").index(when) + 1],
            )
        )
    plugin.pytest_unconfigure()

    text = path.read_text()
    assert text.count("print-setup") == 1
    assert text.count("print-call") == 1
    assert text.count("print-teardown") == 1
    assert "[setup]" in text and "[call]" in text and "[teardown]" in text
