import copy
import importlib.util
from pathlib import Path
from typing import Any, Dict

from praktika import Job
from praktika._settings import _USER_DEFINED_SETTINGS, _Settings
from praktika.utils import ContextManager, Utils


def _get_workflows(name=None, file=None):
    """
    Gets user's workflow configs
    """
    res = []

    with ContextManager.cd():
        directory = Path(_Settings.WORKFLOWS_DIRECTORY)
        for py_file in directory.glob("*.py"):
            if file and file not in str(py_file):
                continue
            module_name = py_file.name.removeprefix(".py")
            spec = importlib.util.spec_from_file_location(
                module_name, f"{_Settings.WORKFLOWS_DIRECTORY}/{module_name}"
            )
            assert spec
            foo = importlib.util.module_from_spec(spec)
            assert spec.loader
            spec.loader.exec_module(foo)
            try:
                for workflow in foo.WORKFLOWS:
                    if name:
                        if name == workflow.name:
                            print(f"Read workflow [{name}] config from [{module_name}]")
                            res = [workflow]
                            break
                        else:
                            continue
                    else:
                        res += foo.WORKFLOWS
                        print(f"Read workflow configs from [{module_name}]")
            except Exception as e:
                print(
                    f"WARNING: Failed to add WORKFLOWS config from [{module_name}], exception [{e}]"
                )
    if not res:
        Utils.raise_with_error(f"Failed to find workflow [{name or file}]")

    for workflow in res:
        # add native jobs
        _update_workflow_with_native_jobs(workflow)
        # fill in artifact properties, e.g. _provided_by
        _update_workflow_artifacts(workflow)
    return res


def _update_workflow_artifacts(workflow):
    artifact_job = {}
    for job in workflow.jobs:
        for artifact_name in job.provides:
            assert artifact_name not in artifact_job
            artifact_job[artifact_name] = job.name
    for artifact in workflow.artifacts:
        artifact._provided_by = artifact_job[artifact.name]


def _update_workflow_with_native_jobs(workflow):
    if workflow.dockers:
        from praktika.native_jobs import _docker_build_job

        print(f"Enable native job [{_docker_build_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_docker_build_job)
        if workflow.enable_cache:
            print(
                f"Add automatic digest config for [{aux_job.name}] job since cache is enabled"
            )
            docker_digest_config = Job.CacheDigestConfig()
            for docker_config in workflow.dockers:
                docker_digest_config.include_paths.append(docker_config.path)
            aux_job.digest_config = docker_digest_config

        workflow.jobs.insert(0, aux_job)
        for job in workflow.jobs[1:]:
            if not job.requires:
                job.requires = []
            job.requires.append(aux_job.name)

    if (
        workflow.enable_cache
        or workflow.enable_report
        or workflow.enable_merge_ready_status
    ):
        from praktika.native_jobs import _workflow_config_job

        print(f"Enable native job [{_workflow_config_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_workflow_config_job)
        workflow.jobs.insert(0, aux_job)
        for job in workflow.jobs[1:]:
            if not job.requires:
                job.requires = []
            job.requires.append(aux_job.name)

    if workflow.enable_merge_ready_status:
        from praktika.native_jobs import _final_job

        print(f"Enable native job [{_final_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_final_job)
        for job in workflow.jobs:
            aux_job.requires.append(job.name)
        workflow.jobs.append(aux_job)


def _get_user_settings() -> Dict[str, Any]:
    """
    Gets user's settings
    """
    res = {}  # type: Dict[str, Any]

    directory = Path(_Settings.SETTINGS_DIRECTORY)
    for py_file in directory.glob("*.py"):
        module_name = py_file.name.removeprefix(".py")
        spec = importlib.util.spec_from_file_location(
            module_name, f"{_Settings.SETTINGS_DIRECTORY}/{module_name}"
        )
        assert spec
        foo = importlib.util.module_from_spec(spec)
        assert spec.loader
        spec.loader.exec_module(foo)
        for setting in _USER_DEFINED_SETTINGS:
            try:
                value = getattr(foo, setting)
                res[setting] = value
                print(f"Apply user defined setting [{setting} = {value}]")
            except Exception as e:
                pass

    return res
