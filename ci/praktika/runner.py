import dataclasses
import datetime
import glob
import hashlib
import json
import os
import re
import shlex
import site
import shutil
import tempfile
import sys
import textwrap
import traceback
from pathlib import Path

from ._environment import _Environment
from .artifact import Artifact
from .cidb import CIDB
from .digest import Digest
from .event import EventFeed
from .gh import GH
from .hook_cache import CacheRunnerHooks
from .hook_html import HtmlRunnerHooks
from .info import Info
from .native_jobs import _check_and_link_open_issues, _is_praktika_job
from .result import Result, ResultInfo
from .runtime import RunConfig
from .s3 import S3
from .settings import Settings
from .usage import ComputeUsage, StorageUsage
from .utils import Shell, TeePopen, Utils
from .workflow import Workflow

_WRAP_WIDTH = 160
_TIMESTAMP_INDENT = len(
    datetime.datetime(2000, 1, 1).strftime("[%Y-%m-%d %H:%M:%S] ")
)


def _job_python_env() -> dict:
    env = os.environ.copy()

    # Keep the runtime environment ahead of the checkout. Jobs reference their
    # config as ``ci.*`` modules, so only the repo root ("."; the parent of the
    # ``ci`` package) belongs on PYTHONPATH — NOT "./ci". Putting "./ci" on the
    # path makes a bare ``import praktika`` resolve to the repo's vendored
    # ``ci/praktika`` copy (often stale) instead of the installed package. This
    # matches the GH Actions path (see yaml_generator: ``export PYTHONPATH=.``).
    site_paths = []
    if hasattr(site, "getsitepackages"):
        site_paths.extend(site.getsitepackages())
    if hasattr(site, "getusersitepackages"):
        site_paths.append(site.getusersitepackages())

    pythonpath_entries = []
    for entry in site_paths:
        if entry and entry not in pythonpath_entries:
            pythonpath_entries.append(entry)

    if "." not in pythonpath_entries:
        pythonpath_entries.append(".")

    for entry in (env.get("PYTHONPATH") or "").split(os.pathsep):
        if not entry:
            continue
        if entry not in pythonpath_entries:
            pythonpath_entries.append(entry)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)
    env["PYTHONSAFEPATH"] = "1"
    return env


def _praktika_package_dir() -> str:
    # Use the installed package location, not the checkout root. In a
    # production runner this points at the system path that contains the
    # praktika package the host is actually using.
    return str(Path(__file__).resolve().parent)


def _staged_praktika_package_dir() -> str:
    package_dir = Path(_praktika_package_dir())
    stage_root = Path("/private/tmp") if sys.platform == "darwin" else Path(tempfile.gettempdir())
    stage_root = stage_root / "praktika-docker-packages"
    stage_root.mkdir(parents=True, exist_ok=True)

    # Keep the staged path stable per installed package location and a coarse
    # content freshness signal so repeated job runs reuse the same mirror.
    package_stat = package_dir.stat()
    cache_key = hashlib.sha1(
        f"{package_dir.resolve().as_posix()}:{package_stat.st_mtime_ns}".encode()
    ).hexdigest()[:16]
    staged_site_packages = stage_root / f"site-packages-{cache_key}"
    staged_package = staged_site_packages / "praktika"
    if not staged_package.exists():
        staged_site_packages.mkdir(parents=True, exist_ok=True)
        shutil.copytree(package_dir, staged_package)
    return str(staged_site_packages)


def _should_post_commit_status(workflow):
    return workflow.engine != Workflow.Engine.PRAKTIKA


class _TimestampedStream:
    """Prepends a [YYYY-MM-DD HH:MM:SS] timestamp to each output line."""

    def __init__(self, stream):
        self._stream = stream
        self._at_line_start = True

    def write(self, data):
        if not data:
            return
        parts = data.split("\n")
        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            if self._at_line_start and part:
                ts = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
                self._stream.write(ts + part)
            elif part:
                self._stream.write(part)
            if not is_last:
                self._stream.write("\n")
                self._at_line_start = True
            else:
                self._at_line_start = not part

    def flush(self):
        self._stream.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


class _TeeStream:
    """Writes to a terminal stream (with line wrapping) and a log file (without wrapping)."""

    def __init__(self, terminal, log, subsequent_indent=0):
        self._terminal = terminal
        self._log = log
        self._subsequent_indent = " " * subsequent_indent
        self._at_line_start = True

    def write(self, data):
        if not data:
            return
        self._log.write(data)
        parts = data.split("\n")
        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            if self._at_line_start and part:
                self._terminal.write(
                    textwrap.fill(
                        part,
                        width=_WRAP_WIDTH,
                        subsequent_indent=self._subsequent_indent,
                        break_long_words=False,
                        break_on_hyphens=False,
                        expand_tabs=False,
                        replace_whitespace=False,
                        drop_whitespace=False,
                    )
                )
            elif part:
                self._terminal.write(part)
            if not is_last:
                self._terminal.write("\n")
                self._at_line_start = True
            else:
                self._at_line_start = not part

    def flush(self):
        self._terminal.flush()
        self._log.flush()

    def __getattr__(self, name):
        return getattr(self._terminal, name)


_GH_authenticated = False


def _GH_Auth():
    global _GH_authenticated
    if _GH_authenticated:
        return True
    if not Settings.USE_CUSTOM_GH_AUTH:
        # GitHub Actions provides auth through the action runtime. Outside
        # GitHub Actions, callers that merely post optional statuses/comments
        # should keep skipping GH calls unless custom Praktika auth is enabled.
        return bool(os.environ.get("GITHUB_ACTIONS"))
    from .gh_auth import GHAuth

    try:
        GHAuth.auth_from_settings()
        _GH_authenticated = True
        return True
    except Exception as e:
        print(f"WARNING: GH auth failed: {e}")
        return False


class Runner:
    @staticmethod
    def generate_local_run_environment(workflow, job, pr=None, sha=None, branch=None):
        print("WARNING: Generate dummy env for local test")
        Shell.check(f"mkdir -p {Settings.TEMP_DIR}", strict=True)
        os.environ["JOB_NAME"] = job.name
        os.environ["CHECK_NAME"] = job.name
        assert (bool(pr) ^ bool(branch)) or (not pr and not branch)
        pr = pr or -1
        if branch:
            pr = 0
        digest_dockers = {}
        for docker in workflow.dockers:
            digest_dockers[docker.name] = Digest().calc_docker_digest(
                docker, workflow.dockers
            )
        workflow_config = RunConfig(
            name=workflow.name,
            digest_jobs={},
            digest_dockers=digest_dockers,
            sha="",
            cache_success=[],
            cache_success_base64=[],
            cache_artifacts={},
            cache_jobs={},
            filtered_jobs={},
            submodule_cache_hash="",
            custom_data={},
        )
        # Extract repository name from git remote (format: owner/repo)
        repo_url = Shell.get_output("git config --get remote.origin.url")
        repo_name = ""
        if repo_url:
            repo_name = GH._repo_name_from_git_remote_url(repo_url)

        _Environment(
            WORKFLOW_NAME=workflow.name,
            JOB_NAME=job.name,
            REPOSITORY=repo_name,
            BRANCH=branch,
            SHA=sha or Shell.get_output("git rev-parse HEAD"),
            PR_NUMBER=pr if not branch else 0,
            EVENT_TYPE=workflow.event,
            JOB_OUTPUT_STREAM="",
            EVENT_FILE_PATH="",
            CHANGE_URL="",
            COMMIT_URL="",
            BASE_BRANCH="",
            RUN_URL="",
            RUN_ID="",
            INSTANCE_ID="",
            INSTANCE_TYPE="",
            INSTANCE_LIFE_CYCLE="",
            LOCAL_RUN=True,
            PR_BODY="",
            PR_TITLE="",
            USER_LOGIN="",
            FORK_NAME="",
            PR_LABELS=[],
            EVENT_TIME="",
            WORKFLOW_CONFIG=workflow_config,
        ).dump()

        if pr and pr > 0:
            changed_files = GH.get_changed_files()
            if changed_files is not None:
                print(f"Storing {len(changed_files)} changed files in JOB_KV_DATA")
                Info().store_kv_data("changed_files", changed_files)
            else:
                print("WARNING: Failed to fetch changed files for PR")

        Result.create_from(name=job.name, status=Result.Status.PENDING).dump()

    def _setup_env(self, _workflow, job):
        # source env file to write data into fs (workflow config json, workflow status json)
        Shell.check(f". {Settings.ENV_SETUP_SCRIPT}", verbose=True, strict=True)

        # parse the same env script and apply envs from python so that this process sees them
        with open(Settings.ENV_SETUP_SCRIPT, "r") as f:
            content = f.read()
        export_pattern = re.compile(
            r"export (\w+)=\$\(cat<<\'EOF\'\n(.*?)EOF\n\)", re.DOTALL
        )
        matches = export_pattern.findall(content)
        for key, value in matches:
            value = value.strip()
            os.environ[key] = value
            print(f"Set environment variable {key}.")

        if (
            job.name == Settings.CI_CONFIG_JOB_NAME
            or _workflow.jobs[0].name != Settings.CI_CONFIG_JOB_NAME
        ):
            # Settings.CI_CONFIG_JOB_NAME initializes the workflow environment by reading it
            # directly from the GitHub context. For workflows without this config job, each
            # job reads the environment from the GitHub context independently.
            print("Read GH Environment from GH context")
            env = _Environment.from_env()
        else:
            print("Read GH Environment from workflow data")
            env = _Environment.from_workflow_data()
        env.JOB_KV_DATA_BASE_KEYS = list(env.JOB_KV_DATA.keys())
        env.JOB_NAME = job.name
        os.environ["JOB_NAME"] = job.name
        os.environ["CHECK_NAME"] = job.name
        env.JOB_CONFIG = job
        env.dump()
        print(env)

        return 0

    def _pre_run(self, workflow, job, local_job_run=False):
        if job.name == Settings.CI_CONFIG_JOB_NAME:
            GH.print_actions_debug_info()
        dirty = Shell.get_output("git status --short", verbose=False) or ""
        if dirty:
            print(f"NOTE: Dirty repo state before job start:\n{dirty}")
        else:
            print("NOTE: Repo state is clean before job start")
        env = _Environment.get()

        result = Result(
            name=job.name,
            status=Result.Status.RUNNING,
            start_time=Utils.timestamp(),
        )
        if env.WORKFLOW_JOB_DATA:
            result.add_ext_key_value(
                "run_url", f"{env.RUN_URL}/job/{env.WORKFLOW_JOB_DATA['check_run_id']}"
            )
        result.dump()

        if not local_job_run:
            if workflow.enable_report and job.name != Settings.CI_CONFIG_JOB_NAME:
                print("Update Job and Workflow Report")
                HtmlRunnerHooks.pre_run(workflow, job)

        if job.requires and not _is_praktika_job(job.name):
            print("Download required artifacts")
            required_artifacts = []
            job_names_with_provides = {
                j.name for j in workflow.jobs if j.provides
            }
            for requires_artifact_name in job.requires:
                for artifact in workflow.artifacts:
                    if (
                        artifact.name == requires_artifact_name
                        and artifact.type == Artifact.Type.S3
                    ):
                        required_artifacts.append(artifact)
                        break
                else:
                    if requires_artifact_name in job_names_with_provides:
                        print(
                            f"Artifact report for [{requires_artifact_name}] will be downloaded"
                        )
                        required_artifacts.append(
                            Artifact.Config(
                                name=requires_artifact_name,
                                type=Artifact.Type.PHONY,
                                path=f"artifact_report_{Utils.normalize_string(requires_artifact_name)}.json",
                                _provided_by=requires_artifact_name,
                            )
                        )

            print(f"--- Job requires s3 artifacts [{required_artifacts}]")
            if workflow.enable_cache:
                prefixes = CacheRunnerHooks.pre_run(
                    _job=job, _workflow=workflow, _required_artifacts=required_artifacts
                )
            else:
                prefixes = [env.get_s3_prefix()] * len(required_artifacts)
            for artifact, prefix in zip(required_artifacts, prefixes):
                if artifact.compress_zst:
                    assert not isinstance(
                        artifact.path, (tuple, list)
                    ), "Not yes supported for compressed artifacts"
                    artifact.path = f"{Path(artifact.path).name}.zst"

                if isinstance(artifact.path, (tuple, list)):
                    artifact_paths = artifact.path
                else:
                    artifact_paths = [artifact.path]

                for artifact_path in artifact_paths:
                    recursive = False
                    include_pattern = ""
                    if "*" in artifact_path:
                        s3_path = f"{Settings.S3_ARTIFACT_BUCKET}/{prefix}/{Utils.normalize_string(artifact._provided_by)}/"
                        recursive = True
                        include_pattern = Path(artifact_path).name
                        assert "*" in include_pattern
                    else:
                        s3_path = f"{Settings.S3_ARTIFACT_BUCKET}/{prefix}/{Utils.normalize_string(artifact._provided_by)}/{Path(artifact_path).name}"
                    S3.copy_file_from_s3(
                        s3_path=s3_path,
                        local_path=Settings.INPUT_DIR,
                        recursive=recursive,
                        include_pattern=include_pattern,
                    )

                    if artifact.compress_zst:
                        Utils.decompress_file(Path(Settings.INPUT_DIR) / artifact_path)

        if not local_job_run and job.needs_submodules and Settings.ENABLE_SUBMODULE_CACHE:
            self._restore_submodule_cache()

        return 0

    @staticmethod
    def _restore_submodule_cache():
        try:
            wf_config = RunConfig.from_workflow_data()
            cache_hash = wf_config.submodule_cache_hash
            if not cache_hash:
                print("NOTE: No submodule cache hash in workflow config, skipping restore")
                return
            s3_path = f"{Settings.CACHE_S3_PATH}/submodules/{cache_hash}.tar.zst"
            local_archive = f"{Settings.TEMP_DIR}/submodules_{cache_hash}.tar.zst"
            print(f"Restoring submodule cache: {s3_path}")
            S3.copy_file_from_s3(s3_path=s3_path, local_path=local_archive, no_strict=True)
            if Path(local_archive).exists():
                if Shell.check(
                    f"zstd -d {local_archive} --stdout | tar -xf - -C .",
                    verbose=True,
                ):
                    Shell.check(f"rm -f {local_archive}")
                    print("Submodule cache restored successfully")
                else:
                    print("WARNING: Submodule cache extraction failed, cleaning up")
                    Shell.check("rm -rf .git/modules")
                    Shell.check(f"rm -f {local_archive}")
            else:
                print("WARNING: Submodule cache download failed, will clone from GitHub")
        except Exception as e:
            print(f"WARNING: Submodule cache restore failed: {e}, will clone from GitHub")
            traceback.print_exc()

    def _run(
        self,
        workflow,
        job,
        docker="",
        no_docker=False,
        param=None,
        test="",
        count=None,
        debug=False,
        path="",
        path_1="",
        workers=None,
    ):
        # re-set envs for local run
        env = _Environment.get()
        env.JOB_NAME = job.name
        env.dump()
        preserve_stdio = sys.stdout.isatty() and sys.stdin.isatty()
        if preserve_stdio:
            print("WARNING: Preserving stdio")

        # work around for old clickhouse jobs
        os.environ["PRAKTIKA"] = "1"
        if env.WORKFLOW_CONFIG:
            try:
                os.environ["DOCKER_TAG"] = json.dumps(
                    RunConfig.from_workflow_data().digest_dockers
                )
            except Exception as e:
                traceback.print_exc()
                print(f"WARNING: Failed to set DOCKER_TAG, ex [{e}]")

        if param:
            if not isinstance(param, str):
                Utils.raise_with_error(
                    f"Custom param for local tests must be of type str, got [{type(param)}]"
                )

        if job.enable_gh_auth:
            if not _GH_Auth():
                Utils.raise_with_error("GH auth failed - required by job")

        print("INFO: disk status before running a job:")
        Shell.run("df -h")
        timeout_shell_cleanup = job.timeout_shell_cleanup
        if job.run_in_docker and not no_docker:
            Shell.run("docker system df")
            job.run_in_docker, docker_settings = (
                job.run_in_docker.split("+")[0],
                job.run_in_docker.split("+")[1:],
            )
            from_root = "root" in docker_settings
            settings = [s for s in docker_settings if s.startswith("-")]
            if ":" in job.run_in_docker:
                docker_name, docker_tag = job.run_in_docker.split(":")
                print(
                    f"WARNING: Job [{job.name}] use custom docker image with a tag - praktika won't control docker version"
                )
            else:
                docker_name, docker_tag = (
                    job.run_in_docker,
                    RunConfig.from_workflow_data().digest_dockers[job.run_in_docker],
                )
                if Utils.is_arm():
                    docker_tag += "_arm"
                elif Utils.is_amd():
                    docker_tag += "_amd"
                else:
                    raise RuntimeError("Unsupported CPU architecture")

            docker = docker or f"{docker_name}:{docker_tag}"
            current_dir = os.getcwd()
            # Derive a stable container name from the worktree path and job name
            # so that different jobs (or the same job in different worktrees)
            # never share a name, while the name is deterministic enough to
            # allow cleanup of stale containers from previous runs.
            container_name = (
                "praktika_"
                + hashlib.sha1(
                    (Path(current_dir).resolve().as_posix() + ":" + job.name).encode()
                ).hexdigest()[:12]
            )
            if not timeout_shell_cleanup:
                timeout_shell_cleanup = f"docker rm -f {container_name}"
            for setting in settings:
                if setting.startswith("--volume"):
                    volume = setting.removeprefix("--volume=").split(":")[0]
                    if not Path(volume).exists():
                        print(
                            "WARNING: Create mount dir point in advance to have the same owner"
                        )
                        Shell.check(f"mkdir -p {volume}", verbose=True, strict=True)
                elif setting.startswith("--workdir"):
                    print(
                        f"NOTE: Job [{job.name}] use custom workdir - praktika won't control workdir"
                    )
            if Shell.check(
                f"docker ps --format '{{{{.Names}}}}' | grep -qx {container_name}",
                verbose=False,
            ):
                raise RuntimeError(
                    f"Docker container '{container_name}' is already running. "
                    f"Another instance of job [{job.name}] may be active in this worktree."
                )
            if Shell.check(
                f"docker ps -a --format '{{{{.Names}}}}' | grep -qx {container_name}",
                verbose=False,
            ):
                print(
                    f"Found stopped container '{container_name}' from a previous run — removing it"
                )
                if not Shell.check(f"docker rm {container_name}", verbose=True):
                    raise RuntimeError(
                        f"Failed to remove stopped container '{container_name}'"
                    )
            if job.enable_gh_auth:
                # pass gh auth seamlessly into the docker container
                gh_mount = "--volume ~/.config/gh:/ghconfig -e GH_CONFIG_DIR=/ghconfig"
            else:
                gh_mount = ""
            # enable tty mode & interactive for docker if we have real tty
            tty = ""
            if preserve_stdio:
                tty = "-it"

            # mount extra paths provided via --path_X  if they are outside current directory
            extra_mounts = ""
            for p_ in [path, path_1]:
                if p_ and Path(p_).exists() and p_.startswith("/"):
                    extra_mounts += f" --volume {p_}:{p_}"

            # PRAKTIKA_HOST_WORKDIR overrides the host-side path used in
            # --volume for the checkout mount. Two main use cases:
            #   1. Point the mount at an arbitrary host directory.
            #   2. Docker-in-Docker: the inner Docker daemon needs the real
            #      host path (not the outer container's CWD) so the job
            #      container can see the host checkout and ci/ tree.
            # When unset, defaults to the current directory.
            staged_package_dir = _staged_praktika_package_dir()
            host_dir = os.environ.get("PRAKTIKA_HOST_WORKDIR", "./")
            host_dir_q = shlex.quote(host_dir)
            package_dir_q = shlex.quote(staged_package_dir)
            current_dir_q = shlex.quote(current_dir)
            # PYTHONPATH must carry BOTH: the staged Praktika package dir (so a
            # bare `import praktika` resolves to the installed copy, not the
            # repo's vendored ci/praktika) AND the checkout root (so the repo's
            # own `ci.*` modules are importable). Staged dir first so Praktika
            # always wins; the checkout root is the container workdir mount.
            container_pythonpath_q = shlex.quote(
                f"{staged_package_dir}:{current_dir}"
            )

            # Rewrite relative host paths in user-supplied --volume settings
            # so that they resolve correctly when PRAKTIKA_HOST_WORKDIR is set
            # (e.g. in Docker-in-Docker scenarios).
            if host_dir != "./":
                rewritten_settings = []
                for s in settings:
                    if s.startswith("--volume="):
                        vol_arg = s.removeprefix("--volume=")
                        src, sep, rest = vol_arg.partition(":")
                        if src == ".":
                            src = host_dir.rstrip("/")
                        elif src.startswith("./"):
                            src = host_dir.rstrip("/") + src[1:]
                        s = f"--volume={src}{sep}{rest}"
                    rewritten_settings.append(s)
                settings = rewritten_settings

            local_env_flag = f"--env-file {self.LOCAL_ENV_FILE}" if Path(self.LOCAL_ENV_FILE).exists() else ""
            cmd = f"docker run {tty} --rm --name {container_name} {'--user $(id -u):$(id -g)' if not from_root else ''} -e PYTHONUNBUFFERED=1 -e PYTHONSAFEPATH=1 -e PYTHONPATH={container_pythonpath_q} {local_env_flag} --volume {host_dir_q}:{current_dir_q} --volume {package_dir_q}:{package_dir_q} {extra_mounts} {gh_mount} --workdir={current_dir_q} {' '.join(settings)} {docker} {job.command}"
        else:
            cmd = job.command
        job_env = _job_python_env()

        if param:
            print(f"Custom --param [{param}] will be passed to job's script")
            cmd += f" --param {param}"
        if test:
            print(f"Custom --test [{test}] will be passed to job's script")
            cmd += f" --test {test}"
        if count is not None:
            print(f"Custom --count [{count}] will be passed to job's script")
            cmd += f" --count {count}"
        if debug:
            print("Custom --debug will be passed to job's script")
            cmd += " --debug"
        if path:
            print(f"Custom --path [{path}] will be passed to job's script")
            cmd += f" --path {path}"
        if path_1:
            print(f"Custom --path_1 [{path_1}] will be passed to job's script")
            cmd += f" --path_1 {path_1}"
        if workers is not None:
            print(f"Custom --workers [{workers}] will be passed to job's script")
            cmd += f" --workers {workers}"
        print(f"--- Run command [{cmd}]")

        with TeePopen(
            cmd,
            env=job_env,
            timeout=job.timeout,
            preserve_stdio=preserve_stdio,
            timeout_shell_cleanup=timeout_shell_cleanup,
        ) as process:
            exit_code = process.wait()

            result = Result.from_fs(job.name)
            if exit_code != 0:
                if not result.is_completed():
                    if process.timeout_exceeded:
                        print(
                            f"WARNING: Job timed out: [{job.name}], timeout [{job.timeout}], exit code [{exit_code}]"
                        )
                        result.add_error(ResultInfo.TIMEOUT)
                        result.set_status(Result.Status.ERROR)
                        result.set_info(process.get_latest_log(max_lines=20))
                    elif workflow.enable_exit_code_result:
                        # Simple mode: workflow opted out of an explicit
                        # Result, so a clean non-zero exit is a job-level
                        # FAIL, not an infra-level ERROR/KILLED.
                        info = f"Job exited with code [{exit_code}]"
                        print(f"NOTE: {info}")
                        result.set_status(Result.Status.FAIL).set_info(info)
                    else:
                        if result.is_running():
                            info = f"Job killed, exit code [{exit_code}]"
                        else:
                            info = f"Invalid status [{result.status}] for exit code [{exit_code}]"
                        print(f"ERROR: {info}")
                        result.add_error(info)
                        result.set_status(Result.Status.ERROR)
                        result.set_info(process.get_latest_log(max_lines=20))
            result.dump()

        print("INFO: disk status after running a job:")
        Shell.run("df -h")

        # When running Docker containers as root (non-rootless mode), any files created
        # by the job will be owned by root. This causes issues when:
        # 1. Files need to be read/compressed/uploaded by subsequent steps
        # 2. Root-owned files remain in the repository working directory
        # The ownership fix below ensures all root-owned files are changed to the current user
        if job.run_in_docker and not no_docker and from_root:
            print("--- Fixing file ownership after running docker as root")
            # Get host user's UID and GID (not from inside the container)
            uid = os.getuid()
            gid = os.getgid()
            chown_cmd = f"docker run --rm --user root --volume {host_dir_q}:{current_dir_q} --workdir={current_dir_q} {docker} chown -R {uid}:{gid} {Settings.TEMP_DIR}"
            Shell.run(chown_cmd)

        return exit_code

    def _get_result_object(
        self, job, setup_env_exit_code, prerun_exit_code, run_exit_code,
        enable_exit_code_result=False,
    ) -> Result:
        result_exist = Result.exist(job.name)

        if setup_env_exit_code != 0:
            print(f"ERROR: {ResultInfo.SETUP_ENV_JOB_FAILED}")
            Result(
                name=job.name,
                status=Result.Status.ERROR,
                start_time=Utils.timestamp(),
                duration=0.0,
            ).add_error(ResultInfo.SETUP_ENV_JOB_FAILED).dump()
        elif prerun_exit_code != 0:
            print(f"ERROR: {ResultInfo.PRE_JOB_FAILED}")
            Result(
                name=job.name,
                status=Result.Status.ERROR,
                start_time=Utils.timestamp(),
                duration=0.0,
            ).add_error(ResultInfo.PRE_JOB_FAILED).dump()
        elif not result_exist:
            if enable_exit_code_result:
                status = Result.Status.OK if run_exit_code == 0 else Result.Status.FAIL
                print(
                    f"NOTE: no Result on disk; synthesizing [{status}] from exit code [{run_exit_code}]"
                )
                synthesized = Result(
                    name=job.name,
                    start_time=Utils.timestamp(),
                    duration=None,
                    status=status,
                )
                if run_exit_code != 0:
                    synthesized.set_info(f"Job exited with code [{run_exit_code}]")
                synthesized.dump()
            else:
                print(f"ERROR: {ResultInfo.NOT_FOUND_IMPOSSIBLE}")
                Result(
                    name=job.name,
                    start_time=Utils.timestamp(),
                    duration=None,
                    status=Result.Status.ERROR,
                ).add_error(ResultInfo.NOT_FOUND_IMPOSSIBLE).dump()

        try:
            result = Result.from_fs(job.name)
        except Exception as e:  # json.decoder.JSONDecodeError
            print(f"ERROR: Failed to read Result json from fs, ex: [{e}]")
            traceback.print_exc()
            result = Result.create_from(
                status=Result.Status.ERROR,
                info=f"Failed to read Result json, ex: [{e}]",
            ).dump()

        if not result.is_completed():
            if enable_exit_code_result:
                status = Result.Status.OK if run_exit_code == 0 else Result.Status.FAIL
                print(
                    f"NOTE: Result not finalized by job; synthesizing [{status}] from exit code [{run_exit_code}]"
                )
                result.set_status(status)
                if run_exit_code != 0:
                    result.set_info(f"Job exited with code [{run_exit_code}]")
                result.dump()
            else:
                print(f"ERROR: {ResultInfo.KILLED}")
                result.add_error(ResultInfo.KILLED).set_status(Result.Status.ERROR).dump()

        if result.is_error() and result.get_on_error_hook():
            print(f"--- Run on_error_hook [{result.get_on_error_hook()}]")
            # Add hook timeout once it's needed
            Shell.check(result.get_on_error_hook(), verbose=True)

        result.update_duration()
        result.set_files([Settings.RUN_LOG], strict=False)
        if job.force_success and not result.is_ok():
            print("NOTE: Job has force_success=True - overriding status to OK")
            result.set_status(Result.Status.OK)
        return result

    @staticmethod
    def _skip_missing_optional_artifact(artifact, artifact_path) -> bool:
        """Whether a providing artifact that matched no file may be skipped."""
        if artifact.optional:
            print(
                f"WARNING: optional artifact [{artifact.name}:{artifact_path}] "
                f"produced no file - skipping upload"
            )
            return True
        return False

    def _post_run(
        self, result, workflow, job, run_exit_code,
    ) -> bool:
        env = _Environment.get()
        is_ok = True

        is_final_job = job.name == Settings.FINISH_WORKFLOW_JOB_NAME
        is_initial_job = job.name == Settings.CI_CONFIG_JOB_NAME

        if run_exit_code == 0 or result.do_not_block_pipeline_on_failure():
            providing_artifacts = []
            if job.provides and workflow.artifacts:
                for provides_artifact_name in job.provides:
                    for artifact in workflow.artifacts:
                        if (
                            artifact.name == provides_artifact_name
                            and artifact.type == Artifact.Type.S3
                        ):
                            providing_artifacts.append(artifact)
            if providing_artifacts:
                print(f"Job provides s3 artifacts [{providing_artifacts}]")
                artifact_links = []
                s3_path = f"{Settings.S3_ARTIFACT_BUCKET}/{env.get_s3_prefix()}/{Utils.normalize_string(env.JOB_NAME)}"
                for artifact in providing_artifacts:
                    if artifact.compress_zst:
                        if isinstance(artifact.path, (tuple, list)):
                            Utils.raise_with_error(
                                "TODO: list of paths is not supported with compress = True"
                            )
                        if "*" in artifact.path:
                            Utils.raise_with_error(
                                "TODO: globe is not supported with compress = True"
                            )
                        print(f"Compress artifact file [{artifact.path}]")
                        artifact.path = Utils.compress_zst(artifact.path)

                    if isinstance(artifact.path, (tuple, list)):
                        artifact_paths = artifact.path
                    else:
                        artifact_paths = [artifact.path]
                    for artifact_path in artifact_paths:
                        try:
                            matched = glob.glob(artifact_path)
                            if not matched:
                                if self._skip_missing_optional_artifact(
                                    artifact, artifact_path
                                ):
                                    continue
                                raise FileNotFoundError(
                                    f"Artifact {artifact_path} not found"
                                )
                            Shell.check(f"ls -l {artifact_path}", verbose=True)
                            for file_path in matched:
                                link = S3.copy_file_to_s3(
                                    s3_path=s3_path,
                                    local_path=file_path,
                                    tags=artifact.ext.get("tags"),
                                )
                                result.set_link(link)
                                artifact_links.append(link)
                        except Exception as e:
                            error = f"Failed to upload artifact [{artifact.name}:{artifact_path}], ex [{e}]"
                            print(f"ERROR: {error}")
                            env.add_workflow_error(error)
                            result.set_status(Result.Status.ERROR)
                            is_ok = False
                if artifact_links:
                    artifact_report = {"build_urls": artifact_links}
                    print(
                        f"Artifact report will be uploaded: [{artifact_report}]"
                    )
                    artifact_report_file = f"{Settings.TEMP_DIR}/artifact_report_{Utils.normalize_string(env.JOB_NAME)}.json"
                    with open(artifact_report_file, "w", encoding="utf-8") as f:
                        json.dump(artifact_report, f)
                    link = S3.copy_file_to_s3(
                        s3_path=s3_path, local_path=artifact_report_file
                    )
                    result.set_link(link)

        # run after post hooks as they might modify workflow kv data
        base_keys = set(env.JOB_KV_DATA_BASE_KEYS or [])
        job_outputs = {
            k: v for k, v in env.JOB_KV_DATA.items() if k not in base_keys
        }
        print(f"Job's output: [{list(job_outputs.keys())}]")
        if is_initial_job:
            output = dataclasses.asdict(env)
            output["pipeline_status"] = "success"
            output["PR_BODY"] = ""
            output["PR_TITLE"] = ""
            output["COMMIT_MESSAGE"] = ""
            output["JOB_KV_DATA"] = Utils.to_base64(json.dumps(env.JOB_KV_DATA))
        else:
            output = job_outputs
        # JOB_OUTPUT_STREAM is GitHub Actions' $GITHUB_OUTPUT; it is empty in
        # praktika-native (SQS) mode. Only write it when set, otherwise open("")
        # raises FileNotFoundError and fails the job in _post_run.
        if env.JOB_OUTPUT_STREAM:
            with open(env.JOB_OUTPUT_STREAM, "a", encoding="utf8") as f:
                print(
                    f"data={json.dumps(output)}",
                    file=f,
                )

        ci_db = None
        if workflow.enable_cidb and not Settings.SECRET_CI_DB_CONNECTION:
            # Clear, non-fatal message instead of a cryptic
            # "Failed to find secret []" from get_secret("").
            print(
                "NOTE: CIDB is enabled but Settings.SECRET_CI_DB_CONNECTION is "
                "not set — skipping CIDB insert."
            )
        elif workflow.enable_cidb:
            print("Insert results to CIDB")
            try:
                conn_secret = workflow.get_secret(Settings.SECRET_CI_DB_CONNECTION)
                assert conn_secret
                ci_db = CIDB.from_connection_secret(conn_secret.get_value()).insert(
                    result, result_name_for_cidb=job.result_name_for_cidb
                )
            except Exception as ex:
                traceback.print_exc()
                error = f"Failed to insert data into CI DB, exception [{ex}]"
                print(f"ERROR: {error}")
                env.add_workflow_error(error)

            try:
                test_cases_result = result.get_sub_result_by_name(
                    name=job.result_name_for_cidb
                )
                if test_cases_result and not test_cases_result.is_ok() and ci_db:
                    for test_case_result in test_cases_result.results:
                        if not test_case_result.is_ok():
                            test_case_result.set_label(
                                Result.Label.CIDB,
                                link=ci_db.get_link_to_test_case_statistics(
                                    test_case_result.name,
                                    failure_patterns=Settings.TEST_FAILURE_PATTERNS,
                                    test_output=test_case_result.info,
                                    url=Settings.CI_DB_READ_URL,
                                    user=Settings.CI_DB_READ_USER,
                                    job_name=job.name,
                                    pr_base_branches=[
                                        env.BASE_BRANCH,
                                        Settings.MAIN_BRANCH,
                                    ],
                                ),
                            )
                    result.dump()
            except Exception as ex:
                traceback.print_exc()
                error = f"Failed to set CIDB label for test cases, exception [{ex}]"
                print(f"ERROR: {error}")
                env.add_workflow_error(error)

        if env.TRACEBACKS:
            result.set_info("===\n" + "---\n".join(env.TRACEBACKS))
        result.dump()

        # always in the end
        if workflow.enable_cache:
            print("Run CI cache hook")
            if result.is_ok():
                CacheRunnerHooks.post_run(workflow, job)

        if workflow.enable_open_issues_check:
            # should be done before HtmlRunnerHooks.post_run(workflow, job)
            #   to upload updated job and workflow results to S3
            try:
                if is_final_job:
                    # re-check entire workflow in the final job as some new issues may appear
                    workflow_result = Result.from_fs(workflow.name)
                    _check_and_link_open_issues(workflow_result, job_name="")
                else:
                    _check_and_link_open_issues(result, job_name=job.name)
            except Exception as e:
                print(f"ERROR: failed to check open issues: {e}")
                traceback.print_exc()
                if is_final_job:
                    env.add_workflow_error(ResultInfo.OPEN_ISSUES_CHECK_ERROR)

        info = Info()
        report_url = info.get_job_report_url(latest=False)

        if _should_post_commit_status(workflow) and (
            workflow.enable_commit_status_on_failure
            and not result.is_ok()
            or job.enable_commit_status
        ):
            if _GH_Auth():
                if not GH.post_commit_status(
                    name=job.name,
                    status=result.status,
                    description=result.info.splitlines()[0] if result.info else "",
                    url=report_url,
                ):
                    env.add_workflow_error(
                        "Failed to post GH commit status for the job"
                    )
                    print("ERROR: Failed to post commit status for the job")

        # Always run report generation at the end to finalize workflow status with latest job result
        if workflow.enable_report:
            print("Run html report hook")
            status_updated = HtmlRunnerHooks.post_run(workflow, job)
            if status_updated and _should_post_commit_status(workflow):
                print(f"Update GH commit status [{result.name}]: [{status_updated}]")
                if _GH_Auth():
                    GH.post_commit_status(
                        name=workflow.name,
                        status=status_updated,
                        description="",
                        url=Info().get_report_url(latest=False),
                    )

            workflow_result = Result.from_fs(workflow.name)
            if is_final_job and ci_db:
                # run after HtmlRunnerHooks.post_run(), when Workflow Result has up-to-date storage_usage data
                workflow_storage_usage = StorageUsage.from_dict(
                    workflow_result.ext.get("storage_usage", {})
                )
                workflow_compute_usage = ComputeUsage.from_dict(
                    workflow_result.ext.get("compute_usage", {})
                )
                if workflow_storage_usage:
                    print(
                        "NOTE: storage_usage is found in workflow Result - insert into CIDB"
                    )
                    ci_db.insert_storage_usage(workflow_storage_usage)
                if workflow_compute_usage:
                    print(
                        "NOTE: compute_usage is found in workflow Result - insert into CIDB"
                    )
                    ci_db.insert_compute_usage(workflow_compute_usage)

        if workflow.enable_gh_summary_comment and (
            job.name == Settings.FINISH_WORKFLOW_JOB_NAME or not result.is_ok()
        ) and _GH_Auth():
            workflow_result = Result.from_fs(workflow.name)
            try:
                summary_body = GH.ResultSummaryForGH.from_result(
                    workflow_result
                ).to_markdown()
                if not GH.post_updateable_comment(
                    comment_tags_and_bodies={"summary": summary_body},
                    only_update=True,
                ):
                    print("ERROR: failed to post CI summary")
            except Exception as e:
                print(f"ERROR: failed to post CI summary, ex: {e}")
                traceback.print_exc()

        if workflow.enable_report:
            # to make it visible in GH Actions annotations
            print(f"::notice ::Job report: {report_url}")

        if (
            workflow.enable_automerge
            and is_final_job
            and workflow.is_event_pull_request()
        ):
            try:
                _GH_Auth()
                workflow_result = Result.from_fs(workflow.name)
                if workflow_result.is_ok():
                    if not GH.merge_pr():
                        print("ERROR: Failed to merge the PR")
                else:
                    print(
                        f"NOTE: Workflow status [{workflow_result.status}] - do not merge"
                    )
            except Exception as e:
                print(f"ERROR: Failed to merge the PR: [{e}]")
                traceback.print_exc()

        # finally, set the status flag for GH Actions
        # These are GH Actions output values matched by workflow YAML conditions,
        # not Result.Status values — must stay lowercase "success"/"failure".
        pipeline_status = "success"
        if not result.is_ok():
            if result.is_failure() and result.do_not_block_pipeline_on_failure():
                # job explicitly says to not block ci even though result is failure
                pass
            else:
                pipeline_status = "failure"
        # Empty in praktika-native mode (see the JOB_OUTPUT_STREAM note above).
        if env.JOB_OUTPUT_STREAM:
            with open(env.JOB_OUTPUT_STREAM, "a", encoding="utf8") as f:
                print(
                    f"pipeline_status={pipeline_status}",
                    file=f,
                )

        # Send Slack notifications after workflow status is finalized by HtmlRunnerHooks.post_run()
        if workflow.enable_slack_feed and (
            is_final_job or is_initial_job or not result.is_ok()
        ):
            updated_emails = []
            commit_authors = info.get_kv_data("commit_authors")
            for commit_author in commit_authors:
                try:
                    EventFeed.update(
                        commit_author,
                        workflow_result.to_event(info=info),
                        s3_path=Settings.EVENT_FEED_S3_PATH,
                    )
                    updated_emails.append(commit_author)
                except Exception as e:
                    traceback.print_exc()
                    print(f"ERROR: failed to update events for {commit_author}: {e}")

            # Invoke Lambda once with all successfully updated emails
            if updated_emails:
                try:
                    EventFeed.notify_slack_users(updated_emails)
                except Exception as e:
                    traceback.print_exc()
                    print(f"ERROR: failed to notify Slack users: {e}")

        dirty = Shell.get_output("git status --short", verbose=False) or ""
        if dirty:
            print(f"NOTE: Dirty repo state after job:\n{dirty}")
        else:
            print("NOTE: Repo state is clean after job")

        return is_ok

    LOCAL_ENV_FILE = "ci/local.env"

    @classmethod
    def _load_local_env(cls):
        """Load environment variables from a gitignored local env file (KEY=VALUE format)."""
        try:
            with open(cls.LOCAL_ENV_FILE) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, _, value = line.partition("=")
                        os.environ[key.strip()] = value.strip()
        except FileNotFoundError:
            pass

    def run(
        self,
        workflow,
        job,
        docker="",
        local_job_run=False,
        local_orchestrator_run=False,
        no_docker=False,
        param=None,
        test="",
        pr=None,
        sha=None,
        branch=None,
        count=None,
        debug=False,
        path="",
        path_1="",
        workers=None,
        timestamp=False,
    ):
        """Execute one job — public entry. Tees stdout/stderr to
        ``Settings.RUN_LOG`` so every engine (GHA, orchestrator) gets the
        same job log file without having to wire up the redirect itself.
        """
        log_dir = os.path.dirname(Settings.RUN_LOG)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        log_file = open(Settings.RUN_LOG, "w", buffering=1)
        try:
            tee = _TeeStream(
                original_stdout,
                log_file,
                subsequent_indent=_TIMESTAMP_INDENT if timestamp else 0,
            )
            sys.stdout = _TimestampedStream(tee) if timestamp else tee
            sys.stderr = sys.stdout
            return self._run_one(
                workflow=workflow,
                job=job,
                docker=docker,
                local_job_run=local_job_run,
                local_orchestrator_run=local_orchestrator_run,
                no_docker=no_docker,
                param=param,
                test=test,
                pr=pr,
                sha=sha,
                branch=branch,
                count=count,
                debug=debug,
                path=path,
                path_1=path_1,
                workers=workers,
            )
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            log_file.close()

    def _run_one(
        self,
        workflow,
        job,
        docker="",
        local_job_run=False,
        local_orchestrator_run=False,
        no_docker=False,
        param=None,
        test="",
        pr=None,
        sha=None,
        branch=None,
        count=None,
        debug=False,
        path="",
        path_1="",
        workers=None,
    ):
        """Execute one job.

        Three modes, controlled by the two ``local_*`` flags:

          * ``local_job_run=True`` — `praktika run` dev sandbox used by
            project (job) developers. Generates a dummy local environment
            and runs the job command. Skips hooks, skips pre/post-run
            scaffolding entirely. Artifact download stays available via
            the legacy ``--pr/--branch + --sha`` escape hatch.
          * ``local_orchestrator_run=True`` — orchestrator-dispatched local
            run for CI developers in this project. Full pipeline against
            the local-fs S3 backend: pre-run, post-run, hooks, artifact
            upload/download, the lot.
          * both False — real CI (GitHub Actions or orchestrator on EC2).
            Full pipeline.

        The two local flags are mutually exclusive.
        """
        assert not (
            local_job_run and local_orchestrator_run
        ), "local_job_run and local_orchestrator_run are mutually exclusive"

        self._load_local_env()

        res = True
        post_res = True
        result = None
        setup_env_code = -10
        prerun_code = -10
        run_code = -10

        if local_orchestrator_run:
            # Orchestrator (CI or local) has already dumped environment.json;
            # nothing to do here.
            setup_env_code = 0
        elif local_job_run:
            self.generate_local_run_environment(
                workflow, job, pr=pr, sha=sha, branch=branch
            )
        else:
            print(
                f"\n\n=== Setup env script [{job.name}], workflow [{workflow.name}] ==="
            )
            try:
                setup_env_code = self._setup_env(workflow, job)
                # Source the bash script and capture the environment variables
                res = setup_env_code == 0
                if not res:
                    print(
                        f"ERROR: Setup env script failed with exit code [{setup_env_code}]"
                    )
            except Exception as e:
                print(f"ERROR: Setup env script failed with exception [{e}]")
                traceback.print_exc()
                Info().store_traceback()
            print("=== Setup env finished ===\n\n")

        # Pre-run dumps the running Result and pulls required artifacts from S3.
        # The dev-sandbox path skips it unless the user passed the legacy
        # ``--pr/--branch + --sha`` escape hatch to wire up artifact download.
        if res and (not local_job_run or ((pr or branch) and sha)):
            res = False
            print(f"=== Pre run script [{job.name}], workflow [{workflow.name}] ===")
            try:
                prerun_code = self._pre_run(workflow, job, local_job_run=local_job_run)
                res = prerun_code == 0
                if not res:
                    print(f"ERROR: Pre-run failed with exit code [{prerun_code}]")
            except Exception as e:
                print(f"ERROR: Pre-run script failed with exception [{e}]")
                traceback.print_exc()
                Info().store_traceback()
            print("=== Pre run finished ===\n\n")

        prehook_result = None
        if res and not local_job_run and job.pre_hooks:
            print(f"=== Pre-hooks [{job.name}], workflow [{workflow.name}] ===")
            sw_ = Utils.Stopwatch()
            results_ = []
            for check in job.pre_hooks:
                if callable(check):
                    name = check.__name__
                else:
                    name = str(check)
                # Run hooks with the job python env so PYTHONPATH carries the
                # checkout root ("."), letting hooks import the repo's `ci.*`
                # modules (they otherwise inherit an env without it and fail
                # with ModuleNotFoundError: No module named 'ci').
                results_.append(
                    Result.from_commands_run(
                        name=name, command=check, env=_job_python_env()
                    )
                )
            prehook_result = Result.create_from(
                name="Pre Hooks",
                results=results_,
                stopwatch=sw_,
                with_info_from_results=True,
            )

        if res:
            print(f"=== Run script [{job.name}], workflow [{workflow.name}] ===")
            run_code = None
            try:
                run_code = self._run(
                    workflow,
                    job,
                    docker=docker,
                    no_docker=no_docker,
                    param=param,
                    test=test,
                    count=count,
                    debug=debug,
                    path=path,
                    path_1=path_1,
                    workers=workers,
                )
                res = run_code == 0
                if not res:
                    print(f"ERROR: Run failed with exit code [{run_code}]")
            except Exception as e:
                print(f"ERROR: Run script failed with exception [{e}]")
                traceback.print_exc()
                Info().store_traceback()
                res = False

            result = Result.from_fs(job.name)
            if not res and result.is_ok():
                # TODO: It happens due to invalid timeout handling (forceful termination by timeout does not work) - fix
                result.set_status(Result.Status.ERROR).set_info(
                    f"Job got terminated with an error, exit code [{run_code}]"
                ).dump()

            print("=== Run script finished ===\n\n")

        # Post-run wraps up the Result, runs job post_hooks, and uploads
        # provides=[...] artifacts so downstream jobs can consume them. Only
        # the dev-sandbox path opts out — orchestrator-local and CI both need
        # the full hand-off.
        if not local_job_run:
            result = self._get_result_object(
                job, setup_env_code, prerun_code, run_code,
                enable_exit_code_result=workflow.enable_exit_code_result,
            )

            if prehook_result:
                result.results.append(prehook_result)
            if job.post_hooks:
                print(f"=== Post hooks [{job.name}], workflow [{workflow.name}] ===")
                sw_ = Utils.Stopwatch()
                results_ = []
                for check in job.post_hooks:
                    if callable(check):
                        name = check.__name__
                    else:
                        name = str(check)
                    # Run hooks with the job python env so PYTHONPATH carries the
                # checkout root ("."), letting hooks import the repo's `ci.*`
                # modules (they otherwise inherit an env without it and fail
                # with ModuleNotFoundError: No module named 'ci').
                results_.append(
                    Result.from_commands_run(
                        name=name, command=check, env=_job_python_env()
                    )
                )
                result.results.append(
                    Result.create_from(
                        name="Post Hooks",
                        results=results_,
                        stopwatch=sw_,
                        with_info_from_results=True,
                    )
                )
                print("=== Post hooks finished ===")

            print(f"=== Post run script [{job.name}], workflow [{workflow.name}] ===")
            post_res = self._post_run(
                result, workflow, job, run_code
            )
            print("=== Post run script finished ===")

        if not post_res or result is None or (not result.is_ok() and not job.force_success):
            sys.exit(1)
