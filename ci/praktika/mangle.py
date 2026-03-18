import copy
import importlib.util
from pathlib import Path
from typing import List

from praktika import Workflow

from . import Job
from .settings import Settings
from .utils import Utils


def _get_workflows(
    name=None,
    file=None,
    _for_validation_check=False,
    _file_names_out=None,
    default=False,
) -> List[Workflow.Config]:
    """
    Gets user's workflow configs
    """
    res = []

    if not Path(Settings.WORKFLOWS_DIRECTORY).is_dir():
        Utils.raise_with_error(
            f"Workflow directory does not exist [{Settings.WORKFLOWS_DIRECTORY}]. cd to the repo's root?"
        )
    directory = Path(Settings.WORKFLOWS_DIRECTORY)
    for py_file in directory.glob("*.py"):
        if not default:
            if Settings.ENABLED_WORKFLOWS:
                if not any(
                    py_file.name == Path(enabled_wf_file).name
                    for enabled_wf_file in Settings.ENABLED_WORKFLOWS
                ):
                    print(
                        f"NOTE: Workflow [{py_file.name}] is not enabled in Settings.ENABLED_WORKFLOWS - skip"
                    )
                    continue
            if Settings.DISABLED_WORKFLOWS:
                if any(
                    py_file.name == Path(disabled_wf_file).name
                    for disabled_wf_file in Settings.DISABLED_WORKFLOWS
                ):
                    print(
                        f"NOTE: Workflow [{py_file.name}] is disabled via Settings.DISABLED_WORKFLOWS - skip"
                    )
                    continue
            if file and str(file) not in str(py_file):
                continue
        elif py_file.name != Settings.DEFAULT_LOCAL_TEST_WORKFLOW:
            print(
                f"--workflow is not set. Default workflow is [{Settings.DEFAULT_LOCAL_TEST_WORKFLOW}]. Skip [{py_file.name}]"
            )
            continue
        module_name = py_file.name.removeprefix(".py")
        spec = importlib.util.spec_from_file_location(
            module_name, f"{Settings.WORKFLOWS_DIRECTORY}/{module_name}"
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
                    res += [workflow]
                    print(
                        f"Read workflow configs from [{module_name}], workflow name [{workflow.name}]"
                    )
                if isinstance(_file_names_out, list):
                    _file_names_out.append(py_file.name.removeprefix(".py"))
        except Exception as e:
            print(
                f"WARNING: Failed to add WORKFLOWS config from [{module_name}], exception [{e}]"
            )
    if not res:
        Utils.raise_with_error(f"Failed to find [{name or file or 'any'}] workflow")

    if not _for_validation_check:
        for wf in res:
            # add native jobs
            _update_workflow_with_native_jobs(wf)
            # fill in artifact properties, e.g. _provided_by
            _update_workflow_artifacts(wf)
    return res


def _update_workflow_artifacts(workflow):
    artifact_job = {}
    for job in workflow.jobs:
        for artifact_name in job.provides:
            artifact_job[artifact_name] = job.name
    for artifact in workflow.artifacts:
        if artifact.name in artifact_job:
            artifact._provided_by = artifact_job[artifact.name]
        else:
            print(
                f"WARNING: Artifact [{artifact.name}] in workflow [{workflow.name}] has no job that provides it"
            )


def _update_workflow_with_native_jobs(workflow):
    if workflow.dockers and not workflow.disable_dockers_build:
        from .native_jobs import _docker_build_arm_linux_job, _docker_build_job

        workflow.jobs = [copy.deepcopy(j) for j in workflow.jobs]

        enable_arm_linux = True
        if Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB:
            for docker in workflow.dockers:
                if docker.has_arm_linux():
                    enable_arm_linux = True
                elif docker.has_amd_linux() or not docker.platforms:
                    continue
                else:
                    Utils.raise_with_error(
                        f"Unsupported docker platform [{docker.platforms}]"
                    )

        aux_job = copy.deepcopy(_docker_build_job)
        print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
        if workflow.enable_cache:
            print(f"Add automatic digest config for [{aux_job.name}] job")
            docker_digest_config = Job.CacheDigestConfig()
            for docker_config in workflow.dockers:
                docker_digest_config.include_paths.append(docker_config.path)
            aux_job.digest_config = docker_digest_config
        workflow.jobs.insert(0, aux_job)
        for job in workflow.jobs[1:]:
            job.requires.append(aux_job.name)

        if enable_arm_linux:
            aux_job = copy.deepcopy(_docker_build_arm_linux_job)
            print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                print(f"Add automatic digest config for [{aux_job.name}] job")
                docker_digest_config = Job.CacheDigestConfig()
                for docker_config in workflow.dockers:
                    docker_digest_config.include_paths.append(docker_config.path)
                aux_job.digest_config = docker_digest_config
            workflow.jobs.insert(0, aux_job)
            workflow.jobs[1].requires.append(aux_job.name)

    if (
        workflow.enable_cache
        or workflow.enable_report
        or workflow.enable_merge_ready_status
    ):
        from .native_jobs import _workflow_config_job

        print(f"Enable native job [{_workflow_config_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_workflow_config_job)
        workflow.jobs.insert(0, aux_job)
        for job in workflow.jobs[1:]:
            job.requires.append(aux_job.name)

    if workflow.enable_merge_ready_status or workflow.post_hooks:
        from .native_jobs import _final_job

        print(f"Enable native job [{_final_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_final_job)
        for job in workflow.jobs:
            aux_job.requires.append(job.name)
        workflow.jobs.append(aux_job)
