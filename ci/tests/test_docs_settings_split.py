"""Run the generated settings splitter regression suite in the CI tests job."""

import subprocess
import sys
from pathlib import Path


SPLITTER_TEST = (
    Path(__file__).parents[1]
    / "jobs/scripts/docs/autogenerate/test_session_settings_split.py"
)


def test_generated_settings_split():
    autogenerate_docs_module = sys.modules.get("autogenerate_docs")
    result = subprocess.run(
        [sys.executable, str(SPLITTER_TEST)],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert sys.modules.get("autogenerate_docs") is autogenerate_docs_module
