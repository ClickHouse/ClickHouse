#!/usr/bin/env python3
"""Run the blocking Vale rule without producing GitHub annotations.

Vale's machine-readable output is captured instead of streamed. Findings are
then printed in a deliberately non-compiler-like format, so neither GitHub
workflow commands nor problem matchers turn them into inline annotations.
"""

import argparse
import json
from pathlib import Path
import subprocess
import sys


RULE_NAME = "ClickHouse.RelativeVersion"
# Skip translated, historical, and generated-only trees. Generated sections in
# otherwise author-maintained pages are handled by `BlockIgnores` in the Vale
# config. The Cloud API reference is generated entirely from its OpenAPI spec.
EXCLUDED_DOCS_GLOB = (
    "!**/{_includes,_migration,_site,_templates,ar,changelogs,es,fr,ja,ko,"
    "products/cloud/api-reference,pt-BR,ru,zh}/**/*"
)


def _safe_text(value):
    """Return one line that cannot start a GitHub workflow command."""
    text = " ".join(str(value).splitlines()).replace("::", "∶∶")
    return text.strip()


def _load_payload(result):
    raw = result.stdout.strip() or result.stderr.strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"runtime_output": raw}


def _findings(payload):
    findings = []
    if not isinstance(payload, dict) or "Code" in payload or "runtime_output" in payload:
        return findings

    for path, alerts in payload.items():
        if not isinstance(alerts, list):
            continue
        for alert in alerts:
            if isinstance(alert, dict):
                findings.append((path, alert))
    return findings


def _run_vale(config, paths, *, docs_scan=False):
    command = [
        "vale",
        f"--config={config}",
        "--output=JSON",
        f'--filter=.Name == "{RULE_NAME}"',
    ]
    if docs_scan:
        command.append(f"--glob={EXCLUDED_DOCS_GLOB}")
    command.extend(str(path) for path in paths)
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _self_test(config, fixture):
    result = _run_vale(config, [fixture])
    payload = _load_payload(result)
    findings = _findings(payload)
    matches = {str(alert.get("Match", "")).lower() for _, alert in findings}
    expected = {"this version", "previous versions", "next version"}
    if result.returncode == 1 and matches == expected:
        return True

    print("Vale rule self-test failed.")
    print(f"Self-test | exit={result.returncode}; matches={sorted(matches)}")
    return False


def _report_runtime_failure(result, payload):
    print(f"Vale runtime failure | exit={result.returncode}")
    if isinstance(payload, dict):
        detail = payload.get("Text") or payload.get("runtime_output") or payload
    else:
        detail = payload
    print(f"Vale runtime detail | {_safe_text(detail)}")


def _report_findings(findings):
    print(
        "Version-relative wording found. Name an exact release or use a "
        "context-specific term instead."
    )
    for number, (path, alert) in enumerate(findings, start=1):
        line = alert.get("Line", "?")
        column = alert.get("Span", ["?", "?"])
        if isinstance(column, list) and column:
            column = column[0]
        message = _safe_text(alert.get("Message", ""))
        match = _safe_text(alert.get("Match", ""))
        print(
            f"Finding {number} | document={json.dumps(str(path))}; "
            f"position=line-{line}/column-{column}; match={json.dumps(match)}; "
            f"message={json.dumps(message)}"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("docs_root", type=Path)
    args = parser.parse_args(argv)

    docs_root = args.docs_root.resolve()
    repo_root = Path(__file__).resolve().parents[4]
    config = docs_root / "_site/styles/.vale-ci.ini"
    fixture = repo_root / "ci/jobs/scripts/docs/testdata/vale_relative_version.md"

    if not _self_test(config, fixture):
        return 2

    result = _run_vale(config, [docs_root], docs_scan=True)
    payload = _load_payload(result)
    findings = _findings(payload)

    if result.returncode == 0 and not findings:
        print("OK: no ambiguous version-relative wording found.")
        return 0
    if result.returncode == 1 and findings:
        _report_findings(findings)
        return 1

    _report_runtime_failure(result, payload)
    return 2


if __name__ == "__main__":
    sys.exit(main())
