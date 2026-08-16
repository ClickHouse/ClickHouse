from ci.defs.job_configs import JobConfigs
from ci.jobs.docs_examples_job import report_info


def test_docs_examples_job_is_affected_by_server_source_changes():
    assert JobConfigs.docs_examples_job.is_affected_by(["src/Functions/bech32.cpp"])


def test_docs_examples_job_is_affected_by_its_runner_changes():
    assert JobConfigs.docs_examples_job.is_affected_by(["tests/docs_examples/runner.py"])


def test_docs_examples_report_info_uses_runner_verdicts():
    outcomes = [
        {"status": "error", "verdict": "known"},
        {"status": "ok", "verdict": "fixed"},
        {"status": "error", "verdict": "unstable"},
        {"status": "output", "verdict": "unexpected"},
    ]

    assert report_info(outcomes, ["Function/removed#0"]) == "fixed: 1, known: 1, stale: 1, unexpected: 1, unstable: 1"
