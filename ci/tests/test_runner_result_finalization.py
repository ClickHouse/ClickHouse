import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.runner import Runner
from ci.praktika.settings import Settings


def _use_tmp_result_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path))
    monkeypatch.setattr(Settings, "RUN_LOG", str(tmp_path / "job.log"))


def _job(name="test job", force_success=False):
    return SimpleNamespace(name=name, force_success=force_success)


def test_result_finalization_keeps_local_run_result_when_setup_and_prerun_skipped(
    tmp_path, monkeypatch
):
    _use_tmp_result_dir(tmp_path, monkeypatch)
    job = _job()
    Result(name=job.name, status=Result.Status.OK, start_time=1.0).dump()

    result = Runner()._get_result_object(
        job,
        setup_env_exit_code=None,
        prerun_exit_code=None,
        run_exit_code=0,
    )

    assert result.status == Result.Status.OK
    assert result.ext.get("errors") is None


def test_result_finalization_marks_ok_result_error_on_nonzero_run_exit(
    tmp_path, monkeypatch
):
    _use_tmp_result_dir(tmp_path, monkeypatch)
    job = _job()
    Result(name=job.name, status=Result.Status.OK, start_time=1.0).dump()

    result = Runner()._get_result_object(
        job,
        setup_env_exit_code=0,
        prerun_exit_code=0,
        run_exit_code=42,
    )

    assert result.status == Result.Status.ERROR
    assert "exit code [42]" in result.info
