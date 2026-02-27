import dataclasses
import json
import platform
import sys
import traceback
from pathlib import Path
from typing import Dict

from . import Job, Workflow
from ._environment import _Environment
from .cidb import CIDB
from .digest import Digest
from .docker import Docker
from .gh import GH
from .hook_cache import CacheRunnerHooks
from .hook_html import HtmlRunnerHooks
from .info import Info
from .mangle import _get_workflows
from .result import Result, ResultInfo, _ResultS3
from .runtime import RunConfig
from .s3 import S3
from .settings import Settings
from .utils import Shell, Utils

assert Settings.CI_CONFIG_RUNS_ON


# TODO: find the right place to not dublicate
def _GH_Auth(workflow, force=False):
    if not Settings.USE_CUSTOM_GH_AUTH:
        return
    from .gh_auth import GHAuth

    if force or not Shell.check(f"gh auth status", verbose=True):
        pem = workflow.get_secret(Settings.SECRET_GH_APP_PEM_KEY).get_value()
        app_id = workflow.get_secret(Settings.SECRET_GH_APP_ID).get_value()
        GHAuth.auth(app_id=app_id, app_key=pem)


_workflow_config_job = Job.Config(
    name=Settings.CI_CONFIG_JOB_NAME,
    runs_on=Settings.CI_CONFIG_RUNS_ON,
    job_requirements=(
        Job.Requirements(
            python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
            python_requirements_txt=Settings.INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS,
        )
        if Settings.INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS
        else None
    ),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.CI_CONFIG_JOB_NAME}'",
    timeout=600,
)

_docker_build_manifest_job = Job.Config(
    name=Settings.DOCKER_BUILD_MANIFEST_JOB_NAME,
    runs_on=Settings.DOCKER_MERGE_RUNS_ON,
    job_requirements=Job.Requirements(
        python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
        python_requirements_txt="",
    ),
    timeout=int(5.5 * 3600),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.DOCKER_BUILD_MANIFEST_JOB_NAME}'",
)

_docker_build_amd_linux_job = Job.Config(
    name=Settings.DOCKER_BUILD_AMD_LINUX_JOB_NAME,
    runs_on=Settings.DOCKER_BUILD_AMD_RUNS_ON,
    job_requirements=Job.Requirements(
        python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
        python_requirements_txt="",
    ),
    timeout=int(5.5 * 3600),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.DOCKER_BUILD_AMD_LINUX_JOB_NAME}'",
)

_docker_build_arm_linux_job = Job.Config(
    name=Settings.DOCKER_BUILD_ARM_LINUX_JOB_NAME,
    runs_on=Settings.DOCKER_BUILD_ARM_RUNS_ON,
    job_requirements=Job.Requirements(
        python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
        python_requirements_txt="",
    ),
    timeout=int(5.5 * 3600),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.DOCKER_BUILD_ARM_LINUX_JOB_NAME}'",
)

_final_job = Job.Config(
    name=Settings.FINISH_WORKFLOW_JOB_NAME,
    runs_on=Settings.CI_CONFIG_RUNS_ON,
    job_requirements=Job.Requirements(
        python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
        python_requirements_txt="",
    ),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.FINISH_WORKFLOW_JOB_NAME}'",
    run_unless_cancelled=True,
)


def _is_praktika_job(job_name):
    if job_name in (
        Settings.CI_CONFIG_JOB_NAME,
        Settings.DOCKER_BUILD_MANIFEST_JOB_NAME,
        Settings.DOCKER_BUILD_ARM_LINUX_JOB_NAME,
        Settings.DOCKER_BUILD_AMD_LINUX_JOB_NAME,
        Settings.FINISH_WORKFLOW_JOB_NAME,
    ):
        return True
    return False


def _build_dockers(workflow, job_name):
    print(f"Start [{job_name}], workflow [{workflow.name}]")
    dockers = workflow.dockers
    ready = []
    results = []
    job_status = Result.Status.SUCCESS
    job_info = ""
    dockers = Docker.sort_in_build_order(dockers)
    for d in dockers:
        if isinstance(d.platforms, str):
            d.platforms = [d.platforms]
    docker_digests = {}  # type: Dict[str, str]
    arm_only = False
    amd_only = False
    if not Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB:
        cpu_arch = platform.processor()
        if cpu_arch in ("arm", "aarch64"):
            arm_only = True
        elif cpu_arch == "x86_64":
            amd_only = True
        else:
            Utils.raise_with_error(
                f"Not supported CPU architecture for docker build [{cpu_arch}]"
            )

    for docker in dockers:
        docker_digests[docker.name] = Digest().calc_docker_digest(docker, dockers)

    if not Shell.check(
        "docker buildx inspect --bootstrap | grep -q docker-container", verbose=True
    ):
        print("Install docker container driver")
        if not Shell.check(
            "docker buildx create --use --name mybuilder --driver docker-container",
            verbose=True,
        ):
            job_status = Result.Status.FAILED
            job_info = "Failed to install docker buildx driver"

    if job_status == Result.Status.SUCCESS:
        if not Info().is_local_run and not Docker.login(
            Settings.DOCKERHUB_USERNAME,
            user_password=workflow.get_secret(Settings.DOCKERHUB_SECRET).get_value(),
        ):
            job_status = Result.Status.FAILED
            job_info = "Failed to login to dockerhub"

    if (
        job_status == Result.Status.SUCCESS
        and job_name != Settings.DOCKER_BUILD_MANIFEST_JOB_NAME
    ):
        for docker in dockers:
            if amd_only and Docker.Platforms.AMD not in docker.platforms:
                continue
            elif arm_only and Docker.Platforms.ARM not in docker.platforms:
                continue
            platforms = (
                docker.platforms
                if isinstance(docker.platforms, list)
                else [docker.platforms]
            )
            if any(p not in Docker.Platforms.arm_amd for p in platforms):
                Utils.raise_with_error(
                    f"TODO: add support for all docker platforms [{docker.platforms}]"
                )
            assert (
                docker.name not in ready
            ), f"All docker names must be uniq [{dockers}]"

            results.append(
                Docker.build(
                    docker,
                    digests=docker_digests,
                    amd_only=amd_only,
                    arm_only=arm_only,
                    disable_push=Info().is_local_run,
                )
            )
            if results[-1].is_ok():
                ready.append(docker.name)
            else:
                job_status = Result.Status.FAILED
                break

    if (
        job_status == Result.Status.SUCCESS
        and job_name == Settings.DOCKER_BUILD_MANIFEST_JOB_NAME
    ):
        print("Start docker manifest merge")
        for docker in dockers:
            results.append(
                Docker.merge_manifest(
                    config=docker,
                    digests=docker_digests,
                    with_log=True,
                    add_latest=workflow.set_latest_for_docker_merged_manifest,
                )
            )

    return Result.create_from(results=results, info=job_info)


def _clean_buildx_volumes():
    Shell.check("docker buildx rm --all-inactive --force", verbose=True)
    Shell.check(
        "docker ps -a --filter name=buildx_buildkit -q | xargs -r docker rm -f",
        verbose=True,
    )
    Shell.check(
        "docker volume ls -q | grep buildx_buildkit | xargs -r docker volume rm",
        verbose=True,
    )


def _config_workflow(workflow: Workflow.Config, job_name) -> Result:
    # debug info
    GH.print_log_in_group("GITHUB envs", Shell.get_output("env | grep GITHUB"))

    def _check_yaml_up_to_date():
        print("Check workflows are up to date")
        commands = [
            f"{Settings.PYTHON_INTERPRETER} -m praktika yaml",
            f'sh -c \'changed=$(git diff-index --name-only HEAD -- {Settings.WORKFLOW_PATH_PREFIX}); if [ -n "$changed" ]; then echo "ERROR: workflows are outdated. Changed files:"; printf "%s\\n" "$changed"; exit 1; fi\'',
        ]

        return Result.from_commands_run(
            name="Check Workflows",
            command=commands,
            fail_fast=True,
        )

    def _check_secrets(secrets):
        print("Check Secrets")
        stop_watch = Utils.Stopwatch()
        infos = []
        for secret_config in secrets:
            value = secret_config.get_value()
            if not value:
                info = f"ERROR: Failed to read secret [{secret_config.name}]"
                infos.append(info)
                print(info)

        info = "\n".join(infos)
        return Result(
            name="Check Secrets",
            status=(Result.Status.FAILED if infos else Result.Status.SUCCESS),
            start_time=stop_watch.start_time,
            duration=stop_watch.duration,
            info=info,
        )

    def _check_db(workflow):
        stop_watch = Utils.Stopwatch()
        res, info = CIDB(
            workflow.get_secret(Settings.SECRET_CI_DB_URL).get_value(),
            workflow.get_secret(Settings.SECRET_CI_DB_USER).get_value(),
            workflow.get_secret(Settings.SECRET_CI_DB_PASSWORD).get_value(),
        ).check()
        return Result(
            name="Check CI DB",
            status=(Result.Status.FAILED if not res else Result.Status.SUCCESS),
            start_time=stop_watch.start_time,
            duration=stop_watch.duration,
            info=info,
        )

    print(f"Start [{job_name}], workflow [{workflow.name}]")
    files = []
    env = _Environment.get()

    # Ensure the local repository has full history (not a shallow clone).
    # Some Praktika features require complete git metadata, such as:
    # - all authors who contributed to the PR
    # - the original commit messages before GitHub's ephemeral merge commit
    commands = [
        f"git rev-parse --is-shallow-repository | grep -q true && git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin HEAD ||:",
    ]
    if env.BASE_BRANCH and env.PR_NUMBER:
        commands.append(
            f"git fetch --prune --no-recurse-submodules --filter=tree:0 origin {env.BASE_BRANCH} ||:"
        )
    results = [
        Result.from_commands_run(
            name="repo unshallow",
            command=commands,
        ),
    ]

    if not env.COMMIT_MESSAGE:
        env.COMMIT_MESSAGE = Shell.get_output(
            f"git log -1 --pretty=%s {env.SHA}", verbose=True
        )
        env.dump()

    try:
        _GH_Auth(workflow, force=True)
    except Exception as e:
        print(f"WARNING: Failed to auth with GH: [{e}]")

    # refresh PR data
    if env.PR_NUMBER > 0:
        title, body, labels = GH.get_pr_title_body_labels()
        print(f"NOTE: PR title: {title}")
        print(f"NOTE: PR labels: {labels}")
        if title:
            if title != env.PR_TITLE:
                print("PR title has been changed")
                env.PR_TITLE = title
            if env.PR_BODY != body:
                print("PR body has been changed")
                env.PR_BODY = body
            if env.PR_LABELS != labels:
                print("PR labels have been changed")
                env.PR_LABELS = labels
            env.dump()

    if workflow.enable_report:
        print("Push pending CI report")
        HtmlRunnerHooks.push_pending_ci_report(workflow)

        info = Info()
        report_url_latest_sha = info.get_report_url(latest=True)
        report_url_current_sha = info.get_report_url(latest=False)
        body = f"Workflow [[{workflow.name}]({report_url_latest_sha})], commit [{env.SHA[:8]}]"
        res2 = not bool(env.PR_NUMBER) or GH.post_updateable_comment(
            comment_tags_and_bodies={"report": body, "summary": ""},
        )
        res1 = GH.post_commit_status(
            name=workflow.name,
            status=Result.Status.PENDING,
            description="",
            url=report_url_current_sha,
        )
        if not (res1 or res2):
            Utils.raise_with_error(
                "Failed to set both GH commit status and PR comment with Workflow Status, cannot proceed"
            )

    _ = RunConfig(
        name=workflow.name,
        digest_jobs={},
        digest_dockers={},
        sha=env.SHA,
        cache_success=[],
        cache_success_base64=[],
        cache_artifacts={},
        cache_jobs={},
        filtered_jobs={},
        custom_data={},
    ).dump()

    if workflow.pre_hooks:
        sw_ = Utils.Stopwatch()
        res_ = []
        for pre_check in workflow.pre_hooks:
            if callable(pre_check):
                name = pre_check.__name__
            else:
                name = str(pre_check)
            res_.append(Result.from_commands_run(name=name, command=pre_check))

        results.append(
            Result.create_from(name="Pre Hooks", results=res_, stopwatch=sw_)
        )
        # reread env object in case some new dada (JOB_KV_DATA) has been added in .pre_hooks
        env = _Environment.get()

    # checks:
    if not results or results[-1].is_ok():
        result_ = _check_yaml_up_to_date()
        if result_.status != Result.Status.SUCCESS:
            print("ERROR: yaml files are outdated - regenerate, commit and push")
        results.append(result_)

    # TODO: commented out to decrease risk of throttling:
    #       An error occurred (ThrottlingException) when calling the GetParameter operation (reached max retries: 2): Rate exceeded
    # if results[-1].is_ok() and workflow.secrets:
    #     result_ = _check_secrets(workflow.secrets)
    #     if result_.status != Result.Status.SUCCESS:
    #         print(f"ERROR: Invalid secrets in workflow [{workflow.name}]")
    #     results.append(result_)

    if results[-1].is_ok() and workflow.enable_cidb:
        result_ = _check_db(workflow)
        results.append(result_)

    if workflow.enable_merge_commit:
        assert False, "NOT implemented"

    # read object from fs after .pre_hooks as some users's custom data may be added there
    workflow_config = RunConfig.from_fs(workflow.name)

    if results[-1].is_ok() and workflow.dockers:
        sw_ = Utils.Stopwatch()
        print("Calculate docker's digests")
        dockers = workflow.dockers
        dockers = Docker.sort_in_build_order(dockers)
        for docker in dockers:
            workflow_config.digest_dockers[docker.name] = Digest().calc_docker_digest(
                docker, dockers
            )
        workflow_config.dump()
        results.append(
            Result.create_from(
                name="Calculate docker digests",
                status=Result.Status.SUCCESS,
                stopwatch=sw_,
            )
        )

    if results[-1].is_ok() and workflow.workflow_filter_hooks:
        sw_ = Utils.Stopwatch()
        try:
            for job in workflow.jobs:
                if _is_praktika_job(job.name):
                    continue
                for hook in workflow.workflow_filter_hooks:
                    should_skip, reason = hook(job.name)
                    if should_skip:
                        print(
                            f"Job [{job.name}] set to skipped by custom hook [{hook.__name__}], reason [{reason}]"
                        )
                        workflow_config.set_job_as_filtered(job.name, reason)
                        continue
            status = Result.Status.SUCCESS
            workflow_config.dump()
            info = ""
        except Exception as e:
            status = Result.Status.ERROR
            print(f"ERROR: Exception in workflow config hook: {e}")
            traceback.print_exc()
            info = f"{traceback.print_exc()}"
        results.append(
            Result.create_from(
                name="Filter Hooks", status=status, stopwatch=sw_, info=info
            )
        )

    if workflow.enable_job_filtering_by_changes and results[-1].is_ok():
        print("Filter not affected jobs")

        def check_affected_jobs():
            changed_files = Info().get_changed_files()
            if changed_files is None:
                print(
                    "WARNING: Failed to get changed files — jobs won't be filtered by changed files list"
                )

            all_affected_dockers = Docker.find_affected_docker_images(
                workflow.dockers, changed_files
            )
            if all_affected_dockers:
                print(f"Affected docker images [{all_affected_dockers}]")

            affected_artifacts = []
            unaffected_jobs_with_artifacts = {}
            all_required_artifacts = set()

            # Build set of all job names for quick lookup
            job_names = {j.name for j in workflow.jobs}

            for job in workflow.jobs:
                # Skip native Praktika jobs
                if _is_praktika_job(job.name):
                    continue

                is_affected = False

                if any(dep in affected_artifacts for dep in job.requires):
                    print(f"Job [{job.name}] requires affected artifacts")
                    is_affected = True
                elif job.get_docker_image_name() in all_affected_dockers:
                    print(
                        f"Job [{job.name}] runs in affected Docker image [{job.run_in_docker}]"
                    )
                    is_affected = True
                elif job.is_affected_by(changed_files):
                    print(f"Job [{job.name}] is directly affected by changed files")
                    is_affected = True

                if is_affected:
                    affected_artifacts.extend(job.provides)
                    if job.provides:
                        # for cases when artifact report is used instead of real artifacts
                        affected_artifacts.append(job.name)
                    # Only add artifact names to all_required_artifacts.
                    # Job names in requirements are ordering-only dependencies unless
                    # needs_jobs_from_requires is set, in which case the required job
                    # must run (cannot be skipped as unaffected).
                    for req in job.requires:
                        if req not in job_names:
                            # Not a job name, must be an artifact name
                            all_required_artifacts.add(req)
                        elif job.needs_jobs_from_requires:
                            print(
                                f"NOTE: [{job.name}] requires [{req}] (job name) - treating as hard dependency"
                            )
                            all_required_artifacts.add(req)
                        else:
                            print(
                                f"NOTE: [{job.name}] requires [{req}] (job name) - treating as ordering-only dependency"
                            )
                else:
                    print(f"Job [{job.name}] is not affected by the change")
                    if not job.provides:
                        workflow_config.set_job_as_filtered(
                            job.name, "Not affected by the changed files"
                        )
                    else:
                        print(
                            f"NOTE: Job [{job.name}] is not affected, but may provide required artifacts"
                        )
                        unaffected_jobs_with_artifacts[job.name] = job.provides

            print(f"All required artifacts [{all_required_artifacts}]")
            print(f"Affected artifacts [{affected_artifacts}]")
            for job_name, artifacts in unaffected_jobs_with_artifacts.items():
                if (
                    any(a in all_required_artifacts for a in artifacts)
                    or job_name in all_required_artifacts
                ):
                    print(
                        f"NOTE: Job [{job_name}] provides required artifacts - cannot be skipped"
                    )
                else:
                    workflow_config.set_job_as_filtered(
                        job_name,
                        "Not affected by the changed files, and artifacts are not required",
                    )

            workflow_config.dump()

        results.append(
            Result.from_commands_run(
                name="Filter not affected jobs",
                command=check_affected_jobs,
            )
        )

    if results[-1].is_ok() and workflow.enable_cache:
        print("Cache Lookup")
        stop_watch = Utils.Stopwatch()
        info = ""
        try:
            workflow_config = CacheRunnerHooks.configure(workflow)
            files.append(RunConfig.file_name_static(workflow.name))
            res = True
        except Exception as e:
            res = False
            traceback.print_exc()
            info = traceback.format_exc()

        results.append(
            Result(
                name="Cache Lookup",
                status=Result.Status.SUCCESS if res else Result.Status.FAILED,
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
                info=info,
            )
        )

    if workflow.enable_slack_feed:
        if env.PR_NUMBER:
            commit_authors = set()
            try:
                # Find the first merge commit (going backwards from SHA) that has no parent from BASE_BRANCH
                # This indicates a merge from outside the base branch, where we should stop (support complex git scenarious with forks syncronization)
                stop_commit = ""
                merge_commits = Shell.get_output(
                    f"git rev-list --merges origin/{env.BASE_BRANCH}..{env.SHA}",
                    verbose=True,
                ).strip()

                if merge_commits:
                    for merge_commit in reversed(merge_commits.split("\n")):
                        if not merge_commit:
                            continue
                        # Get parents of this merge commit
                        parents = Shell.get_output(
                            f"git rev-parse {merge_commit}^@", verbose=False
                        ).strip()

                        # Check if any parent is reachable from BASE_BRANCH
                        has_base_parent = False
                        for parent in parents.split("\n"):
                            if not parent:
                                continue
                            # Check if parent is ancestor of BASE_BRANCH or BASE_BRANCH is ancestor of parent
                            try:
                                Shell.check(
                                    f"git merge-base --is-ancestor {parent} origin/{env.BASE_BRANCH}",
                                    verbose=False,
                                )
                                has_base_parent = True
                                break
                            except:
                                pass

                        if not has_base_parent:
                            # Found a merge with no parent from BASE_BRANCH - stop here
                            stop_commit = merge_commit
                            print(
                                f"Found merge commit without BASE_BRANCH parent: {merge_commit[:8]}"
                            )
                            break

                # Get commit author emails, excluding merge commits, up to stop point
                if stop_commit:
                    end_ref = f"{stop_commit}^"
                else:
                    end_ref = env.SHA

                commits_emails = Shell.get_output(
                    f"git log --format='%ae' --no-merges origin/{env.BASE_BRANCH}..{end_ref}",
                    verbose=True,
                ).strip()

                if commits_emails:
                    # Validate emails contain @ symbol
                    commit_authors = set(
                        email
                        for email in commits_emails.split("\n")
                        if email and "@" in email and "+" not in email
                    )
            except Exception as e:
                print(f"WARNING: Failed to extract commit authors from git: {e}")
            print(f"Found {len(commit_authors)} commit authors")
            if commit_authors:
                env.COMMIT_AUTHORS = list(commit_authors)
                env.JOB_KV_DATA["commit_authors"] = list(commit_authors)
                env.dump()

    print(f"WorkflowRuntimeConfig: [{workflow_config.to_json(pretty=True)}]")
    workflow_config.dump()
    env.WORKFLOW_CONFIG = dataclasses.asdict(workflow_config)
    env.dump()

    if results[-1].is_ok() and workflow.enable_report:
        print("Init report")
        stop_watch = Utils.Stopwatch()
        HtmlRunnerHooks.configure(workflow)
        results.append(
            Result(
                name="Init Report",
                status=Result.Status.SUCCESS,
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
            )
        )
        files.append(Result.file_name_static(workflow.name))

    return Result.create_from(name=job_name, results=results, files=files)


def _check_and_link_open_issues(result: Result, job_name: str) -> bool:
    """
    Downloads flaky test catalog from S3 and marks matching test results as flaky
    or infrastructure issues. Can be called on a single job result.

    Args:
        result: The result object to check and mark
        job_name: The job name for infrastructure job pattern matching

    Returns:
        True if successful, False if catalog download failed
    """
    from .issue import TestCaseIssueCatalog

    if result.is_ok():
        print("Result succeeded, no flaky test check needed.")
        return True

    print("Checking for flaky tests and infrastructure issues...")

    # Download catalog from S3
    issue_catalog = TestCaseIssueCatalog.from_s3()
    if not issue_catalog:
        print("ERROR: Could not load issue catalog")
        return False
    print(f"Loaded {len(issue_catalog.active_test_issues)} active issues")

    # Check all issues against the result tree
    for issue in issue_catalog.active_test_issues:
        issue.check_result(result, job_name)
    result.dump()
    print("Flaky test check and infrastructure issue check completed")
    return True


def _finish_workflow(workflow, job_name):
    print(f"Start [{job_name}], workflow [{workflow.name}]")
    env = _Environment.get()
    stop_watch = Utils.Stopwatch()

    workflow_job_data = {}
    try:
        if Path(Settings.WORKFLOW_STATUS_FILE).is_file():
            with open(Settings.WORKFLOW_STATUS_FILE, "r", encoding="utf8") as f:
                workflow_job_data = json.load(f)
    except Exception as e:
        print(
            f"ERROR: failed to read workflow status file [{Settings.WORKFLOW_STATUS_FILE}]: {e}"
        )

    print("Check Actions statuses")
    print(env.get_needs_statuses())

    print("Check Workflow results")
    version = _ResultS3.copy_result_from_s3_with_version(
        Result.file_name_static(workflow.name),
    )
    workflow_result = Result.from_fs(workflow.name)

    if (
        workflow.enable_merge_ready_status
        or workflow.enable_open_issues_check
        or workflow.post_hooks
    ):
        _GH_Auth(workflow)

    update_final_report = False
    results = []
    if workflow.post_hooks:
        sw_ = Utils.Stopwatch()
        update_final_report = True
        results_ = []
        for check in workflow.post_hooks:
            if callable(check):
                name = check.__name__
            else:
                name = str(check)
            results_.append(Result.from_commands_run(name=name, command=check))

        results.append(
            Result.create_from(name="Post Hooks", results=results_, stopwatch=sw_)
        )

    ready_for_merge_status = Result.Status.SUCCESS
    ready_for_merge_description = ""
    failed_results = []
    dropped_results = []

    if results and any(not result.is_ok() for result in results):
        failed_results.append("Workflow Post Hook")

    for result in workflow_result.results:
        if result.name == job_name:
            continue
        if result.status == Result.Status.SUCCESS:
            continue
        if result.status == Result.Status.SKIPPED:
            continue
        if result.status == Result.Status.DROPPED:
            dropped_results.append(result.name)
            continue
        if workflow.enable_open_issues_check and (
            (
                result.has_label(Result.Label.ISSUE)
                and not result.has_label(Result.Label.BLOCKER)
            )
            or (
                result.results
                and all(
                    (
                        sub_result.has_label(Result.Label.ISSUE)
                        and not sub_result.has_label(Result.Label.BLOCKER)
                    )
                    for sub_result in result.results
                )
            )
        ):
            print(
                f"NOTE: All failures are known and have open issues - do not block merge by [{result.name}]"
            )
            continue
        if not result.is_completed():
            normalized_name = Utils.normalize_string(result.name)
            gh_job = workflow_job_data.get(normalized_name, {})
            gh_job_result = (gh_job.get("result") or "").lower()
            if gh_job_result in ("cancelled", "canceled"):
                print(
                    f"NOTE: not finished job [{result.name}] in the workflow but GitHub status is [{gh_job_result}] - set status to dropped"
                )
                result.status = Result.Status.DROPPED
                workflow_result.dump()
                workflow_result.ext["is_cancelled"] = True
                update_final_report = True
                dropped_results.append(result.name)
                continue
            elif gh_job_result == "success":
                print(
                    f"NOTE: not finished job [{result.name}] in the workflow but GitHub status is [{gh_job_result}] - set status to success"
                )
                result.status = Result.Status.SUCCESS
                workflow_result.dump()
                update_final_report = True
                continue
            else:
                print(
                    f"ERROR: not finished job [{result.name}] in the workflow - set status to error"
                )
                result.status = Result.Status.ERROR
                # dump workflow result after update - to have an updated result in post
                workflow_result.dump()
                # add error into env - should appear in the report on the main page
                env.add_info(f"{result.name}: {ResultInfo.NOT_FINALIZED}")
                # add error info to job info as well
                result.set_info(ResultInfo.NOT_FINALIZED)
                update_final_report = True
        job = workflow.get_job(result.name)
        if not job or not job.allow_merge_on_failure:
            print(
                f"NOTE: Result for [{result.name}] has not ok status [{result.status}]"
            )
            failed_results.append(result.name)

    if failed_results or dropped_results:
        ready_for_merge_status = Result.Status.FAILED
        failed_jobs_csv = ",".join(failed_results)
        if failed_jobs_csv and len(failed_jobs_csv) < 80:
            ready_for_merge_description = f"Failed: {failed_jobs_csv}"
        else:
            ready_for_merge_description = f"Failed: {len(failed_results)}"
        if dropped_results:
            ready_for_merge_description += f", Dropped: {len(dropped_results)}"

    if workflow.enable_merge_ready_status:
        if not GH.post_commit_status(
            name=Settings.READY_FOR_MERGE_CUSTOM_STATUS_NAME
            or f"Ready For Merge [{workflow.name}]",
            status=ready_for_merge_status,
            description=ready_for_merge_description,
            url="",
        ):
            print(f"ERROR: failed to set ReadyForMerge status")
            env.add_info(ResultInfo.GH_STATUS_ERROR)

    if update_final_report:
        _ResultS3.copy_result_to_s3_with_version(workflow_result, version + 1)

    if results:
        return Result.create_from(results=results, stopwatch=stop_watch)
    else:
        return Result.create_from(status=Result.Status.SUCCESS, stopwatch=stop_watch)


if __name__ == "__main__":
    job_name = sys.argv[1]
    assert job_name, "Job name must be provided as input argument"
    sw = Utils.Stopwatch()
    try:
        workflow = _get_workflows(name=_Environment.get().WORKFLOW_NAME)[0]
        if job_name in (
            Settings.DOCKER_BUILD_MANIFEST_JOB_NAME,
            Settings.DOCKER_BUILD_ARM_LINUX_JOB_NAME,
            Settings.DOCKER_BUILD_AMD_LINUX_JOB_NAME,
        ):
            result = _build_dockers(workflow, job_name)
            _clean_buildx_volumes()
        elif job_name == Settings.CI_CONFIG_JOB_NAME:
            result = _config_workflow(workflow, job_name)
        elif job_name == Settings.FINISH_WORKFLOW_JOB_NAME:
            result = _finish_workflow(workflow, job_name)
        else:
            assert False, f"BUG, job name [{job_name}]"
    except Exception as e:
        error_traceback = traceback.format_exc()
        print("Failed with Exception:")
        print(error_traceback)
        result = Result.create_from(
            name=job_name,
            status=Result.Status.ERROR,
            stopwatch=sw,
            # try out .info generated in runner._run() which works for all jobs automatically
            # info=f"Failed with Exception [{e}]\n{error_traceback}",
        )

    result.dump().complete_job(with_job_summary_in_info=False)
