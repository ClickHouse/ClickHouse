from ci.defs.job_configs import JobConfigs


def test_docs_examples_job_is_affected_by_server_source_changes():
    assert JobConfigs.docs_examples_job.is_affected_by(["src/Functions/bech32.cpp"])


def test_docs_examples_job_is_affected_by_its_runner_changes():
    assert JobConfigs.docs_examples_job.is_affected_by(["tests/docs_examples/runner.py"])
