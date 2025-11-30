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


def _get_artifact_to_providing_job_map(workflow):
    artifact_job = {}
    for job in workflow.jobs:
        for artifact_name in job.provides:
            artifact_job[artifact_name] = job.name
    return artifact_job


def _update_workflow_artifacts(workflow):
    artifact_job = _get_artifact_to_providing_job_map(workflow)
    for artifact in workflow.artifacts:
        if artifact.name in artifact_job:
            artifact._provided_by = artifact_job[artifact.name]


def _update_workflow_with_native_jobs(workflow):
    if workflow.dockers and not workflow.disable_dockers_build:
        from .native_jobs import (
            _docker_build_amd_linux_job,
            _docker_build_arm_linux_job,
            _docker_build_manifest_job,
        )

        workflow.jobs = [copy.deepcopy(j) for j in workflow.jobs]

        docker_job_names = []
        docker_digest_config = Job.CacheDigestConfig()
        for docker_config in workflow.dockers:
            docker_digest_config.include_paths.append(docker_config.path)

        if not Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB:
            aux_job = copy.deepcopy(_docker_build_amd_linux_job)
            print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                print(f"Add automatic digest config for [{aux_job.name}] job")
                aux_job.digest_config = docker_digest_config
            workflow.jobs.insert(len(docker_job_names), aux_job)
            docker_job_names.append(aux_job.name)

            aux_job = copy.deepcopy(_docker_build_arm_linux_job)
            print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                print(f"Add automatic digest config for [{aux_job.name}] job")
                aux_job.digest_config = docker_digest_config
            workflow.jobs.insert(len(docker_job_names), aux_job)
            docker_job_names.append(aux_job.name)

        if (
            workflow.enable_dockers_manifest_merge
            or Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB
        ):
            aux_job = copy.deepcopy(_docker_build_manifest_job)
            print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                print(f"Add automatic digest config for [{aux_job.name}] job")
                aux_job.digest_config = docker_digest_config
            aux_job.requires = copy.deepcopy(docker_job_names)
            workflow.jobs.insert(len(docker_job_names), aux_job)
            docker_job_names.append(aux_job.name)

        assert docker_job_names, "Docker job names are empty, BUG?"
        for job in workflow.jobs[len(docker_job_names) :]:
            job.requires.extend(docker_job_names)

    if workflow._enabled_workflow_config():
        from .native_jobs import _workflow_config_job

        print(f"Enable native job [{_workflow_config_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_workflow_config_job)
        workflow.jobs.insert(0, aux_job)
        for job in workflow.jobs[1:]:
            job.requires.append(aux_job.name)

    if (
        workflow.enable_merge_ready_status
        or workflow.post_hooks
        or workflow.enable_automerge
        or workflow.enable_cidb  # to write cpu and storage usage summary into cidb
    ):
        from .native_jobs import _final_job

        print(f"Enable native job [{_final_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_final_job)
        for job in workflow.jobs:
            aux_job.requires.append(job.name)
        workflow.jobs.append(aux_job)

    # Propagate transitive dependencies: if B depends on A and C depends on B, then C should also depend on A
    # To enable complex scenarios in GH Actions: do not block pipeline on failed job/action
    job_map = {job.name: job for job in workflow.jobs}
    artifact_map = _get_artifact_to_providing_job_map(workflow)
    workflow.jobs = copy.deepcopy(
        workflow.jobs
    )  # same Job.Config objects may be used in other workflows, thus deep copy
    for job in workflow.jobs:
        all_deps = set(
            job.requires
        )  # init with original content to preserve artifacts (vs job names) set in .requires
        to_visit = list(job.requires)
        visited = set()
        while to_visit:
            dep_name = to_visit.pop()
            if dep_name in visited:
                continue
            visited.add(dep_name)
            if dep_name in job_map:
                to_visit.extend(job_map[dep_name].requires)
                all_deps.add(dep_name)
            elif dep_name in artifact_map:
                to_visit.extend(job_map[artifact_map[dep_name]].requires)
                all_deps.add(artifact_map[dep_name])
            else:
                assert False, f"dependency [{dep_name}] not found"
        job.requires = sorted(list(all_deps))
