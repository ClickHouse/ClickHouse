import copy
import importlib.util
from pathlib import Path
from typing import List

from praktika import Workflow

from . import Job
from .info import Info
from .settings import Settings
from .utils import Utils
from .version import current_praktika_version


def _is_local_run():
    """Check if running locally. Returns False if can't determine (e.g., during workflow generation)."""
    try:
        return Info().is_local_run
    except Exception:
        # During workflow generation, Info can't be initialized - treat as CI mode
        return False


def _is_default_local_workflow(py_file: Path, workflow_name: str) -> bool:
    configured = Settings.DEFAULT_LOCAL_TEST_WORKFLOW
    if not configured:
        return False
    configured_values = {
        configured,
        Path(configured).name,
        Path(configured).stem,
    }
    workflow_values = {
        workflow_name,
        py_file.name,
        py_file.stem,
    }
    return bool(configured_values & workflow_values)


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
        module_name = py_file.name.removeprefix(".py")
        spec = importlib.util.spec_from_file_location(
            module_name, f"{Settings.WORKFLOWS_DIRECTORY}/{module_name}"
        )
        assert spec
        foo = importlib.util.module_from_spec(spec)
        assert spec.loader
        spec.loader.exec_module(foo)
        try:
            matched_default_workflow = False
            for workflow in foo.WORKFLOWS:
                if default and not _is_default_local_workflow(py_file, workflow.name):
                    continue
                matched_default_workflow = True
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
            if default and not matched_default_workflow and not _is_local_run():
                print(f"Skip [{py_file.name}]")
        except Exception:
            pass
            # print(
            #     f"WARNING: Failed to add WORKFLOWS config from [{module_name}], exception [{e}]"
            # )
    if not res:
        Utils.raise_with_error(f"Failed to find [{name or file or 'any'}] workflow")

    if not _for_validation_check:
        for wf in res:
            # add native jobs
            _update_workflow_with_native_jobs(wf)
            # fill in artifact properties, e.g. _provided_by
            _update_workflow_artifacts(wf)
    return res


def _get_infra_projects():
    """
    Returns the infra configs imported as:
    Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH import PROJECTS
    """
    from pathlib import Path

    if not Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH:
        Utils.raise_with_error(
            "Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH is not set. "
            "Please define it in your ci/settings/*.py file."
        )

    config_path = Path(Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH)

    if not config_path.exists():
        Utils.raise_with_error(
            f"Infrastructure config file does not exist [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}]"
        )

    module_name = config_path.stem
    spec = importlib.util.spec_from_file_location(module_name, config_path)

    if not spec or not spec.loader:
        Utils.raise_with_error(
            f"Failed to load infrastructure config from [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}]: "
            f"path must point to a Python (.py) file, not a directory"
        )

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except TypeError as e:
        message = str(e)
        if "__init__()" in message and "unexpected keyword argument" in message:
            Utils.raise_with_error(
                "Infrastructure config cannot be loaded because there is a mismatch "
                "between the Praktika version and the infrastructure config.\n"
                f"Config file: {Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}\n"
                f"Running Praktika version: {current_praktika_version()}\n"
                f"Import error: {message}\n"
                "The config likely uses newer infrastructure fields that this "
                "Praktika runtime does not understand. Run the Praktika package "
                "version that matches this config, then retry."
            )
        raise

    try:
        projects = list(getattr(module, "PROJECTS"))
        if not projects:
            Utils.raise_with_error(
                f"PROJECTS in [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}] must contain at least one config"
            )
        seen_names = set()
        for project in projects:
            if not getattr(project, "name", ""):
                Utils.raise_with_error(
                    f"All PROJECTS in [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}] must have non-empty names"
                )
            if project.name in seen_names:
                Utils.raise_with_error(
                    f"Duplicate infrastructure project name [{project.name}] in [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}]"
                )
            seen_names.add(project.name)
            project._settings = Settings
        print(
            f"Loaded {len(projects)} infrastructure project(s) from [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}]"
        )
        return projects
    except AttributeError:
        Utils.raise_with_error(
            f"PROJECTS variable not found in [{Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH}]"
        )

    return None


def _get_infra_config(project_name=None, require_project: bool = False):
    projects = _get_infra_projects()
    if require_project and not project_name:
        names = ", ".join(sorted(project.name for project in projects))
        Utils.raise_with_error(
            f"Infrastructure destroy requires --project NAME. Available projects: {names}"
        )

    if len(projects) == 1:
        only = projects[0]
        if project_name and project_name != only.name:
            Utils.raise_with_error(
                f"Unknown infrastructure project [{project_name}]. Only configured project is [{only.name}]"
            )
        return only

    if not project_name:
        names = ", ".join(sorted(project.name for project in projects))
        Utils.raise_with_error(
            f"Multiple infrastructure projects configured [{names}]. Use --project NAME."
        )

    for project in projects:
        if project.name == project_name:
            return project

    Utils.raise_with_error(
        f"Unknown infrastructure project [{project_name}]. "
        f"Available projects: {', '.join(sorted(project.name for project in projects))}"
    )


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


def _native_job_runs_on(workflow):
    return copy.deepcopy(workflow.native_job_runs_on or Settings.CI_CONFIG_RUNS_ON)


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
            if not _is_local_run():
                print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                if not _is_local_run():
                    print(f"Add automatic digest config for [{aux_job.name}] job")
                aux_job.digest_config = docker_digest_config
            workflow.jobs.insert(len(docker_job_names), aux_job)
            docker_job_names.append(aux_job.name)

            aux_job = copy.deepcopy(_docker_build_arm_linux_job)
            if not _is_local_run():
                print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                if not _is_local_run():
                    print(f"Add automatic digest config for [{aux_job.name}] job")
                aux_job.digest_config = docker_digest_config
            workflow.jobs.insert(len(docker_job_names), aux_job)
            docker_job_names.append(aux_job.name)

        if (
            workflow.enable_dockers_manifest_merge
            or Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB
        ):
            aux_job = copy.deepcopy(_docker_build_manifest_job)
            if not _is_local_run():
                print(f"Enable praktika job [{aux_job.name}] for [{workflow.name}]")
            if workflow.enable_cache:
                if not _is_local_run():
                    print(f"Add automatic digest config for [{aux_job.name}] job")
                aux_job.digest_config = docker_digest_config
            aux_job.requires = copy.deepcopy(docker_job_names)
            workflow.jobs.insert(len(docker_job_names), aux_job)
            docker_job_names.append(aux_job.name)

        assert docker_job_names, "Docker job names are empty, BUG?"
        for job in workflow.jobs[len(docker_job_names) :]:
            job.run_after.extend(docker_job_names)

    if workflow._enabled_workflow_config():
        from .native_jobs import _workflow_config_job

        if not _is_local_run():
            print(
                f"Enable native job [{_workflow_config_job.name}] for [{workflow.name}]"
            )
        aux_job = copy.deepcopy(_workflow_config_job)
        aux_job.runs_on = _native_job_runs_on(workflow)
        workflow.jobs.insert(0, aux_job)
        for job in workflow.jobs[1:]:
            job.run_after.append(aux_job.name)

    if (
        workflow.enable_merge_ready_status
        or workflow.post_hooks
        or workflow.enable_automerge
        or workflow.enable_cidb  # to write cpu and storage usage summary into cidb
    ):
        from .native_jobs import _final_job

        if not _is_local_run():
            print(f"Enable native job [{_final_job.name}] for [{workflow.name}]")
        aux_job = copy.deepcopy(_final_job)
        aux_job.runs_on = _native_job_runs_on(workflow)
        for job in workflow.jobs:
            aux_job.run_after.append(job.name)
        workflow.jobs.append(aux_job)

    if workflow.runs_on_label_prefix:
        prefix = workflow.runs_on_label_prefix
        for job in workflow.jobs:
            if not job.runs_on:
                continue
            job.runs_on = [
                label if label == "self-hosted" else f"{prefix}{label}"
                for label in job.runs_on
            ]
