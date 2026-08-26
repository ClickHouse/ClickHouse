import datetime
import re
from pathlib import Path

_HAS_TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def full_log_path(config):
    log_file = config.getoption("log_file", None) or config.getini("log_file")
    base = Path(log_file) if log_file else Path("pytest.log")
    workerid = getattr(config, "workerinput", {}).get("workerid", "")
    stem = base.stem + (
        f"-{workerid}" if workerid and workerid not in base.stem else ""
    )
    return base.with_name(stem + ".full.log")


def stamp(line, now=None):
    if _HAS_TS.match(line):
        return line
    now = now or datetime.datetime.now()
    return f"{now:%Y-%m-%d %H:%M:%S.%f} {line}"


def phase_sections(report):
    # Each phase report re-carries every accumulated section; restrict to this phase
    # so setup/call output is not written again under later phases.
    return [(t, c) for t, c in report.sections if t.endswith(report.when)]


class FullLogPlugin:
    """Write one plain-text log of every test's setup/call/teardown output, print() included."""

    def __init__(self, path):
        self._file = open(path, "w", encoding="utf-8", errors="replace")

    def pytest_runtest_logreport(self, report):
        sections = phase_sections(report)
        if not sections:
            return
        self._file.write(f"\n===== {report.nodeid} [{report.when}] =====\n")
        for title, content in sections:
            self._file.write(f"--- {title} ---\n")
            for line in content.splitlines():
                self._file.write(stamp(line) + "\n")
        self._file.flush()

    def pytest_unconfigure(self):
        self._file.close()
