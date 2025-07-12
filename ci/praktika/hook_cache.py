import os
from concurrent.futures import ThreadPoolExecutor

from ._environment import _Environment
from .cache import Cache
from .runtime import RunConfig
from .settings import Settings
from .utils import Utils


class CacheRunnerHooks:
    @classmethod
    def configure(cls, workflow):
        workflow_config = RunConfig.from_fs(workflow.name)
        docker_digests = workflow_config.digest_dockers
        cache = Cache()
        print(f"Workflow Configure, workflow [{workflow.name}]")
        assert (
            workflow.enable_cache
        ), f"Outdated yaml pipelines or BUG. Configuration must be run only for workflow with enabled cache, workflow [{workflow.name}]"
        artifact_digest_map = {}
        job_digest_map = {}
        artifact_name_config_map = {}
        for a in workflow.artifacts:
            artifact_name_config_map[a.name] = a

        for job in workflow.jobs:
            digest = cache.digest.calc_job_digest(
                job_config=job,
                docker_digests=docker_digests,
                artifact_configs=artifact_name_config_map,
            )
            job_digest_map[job.name] = digest
            if job.provides:
                # assign the job digest also to the artifacts it provides
                for artifact in job.provides:
                    artifact_digest_map[artifact] = digest
                # TODO: remove together with artifact_report_ hack
                artifact_digest_map[job.name] = digest
        for job in workflow.jobs:
            digests_combined_list = []
            if job.requires and job.digest_config:
                # include digest of required artifact to the job digest, so that they affect job state
                for artifact_name in job.requires:
                    if artifact_name in artifact_digest_map:
                        digests_combined_list.append(artifact_digest_map[artifact_name])
            digests_combined_list.append(job_digest_map[job.name])
            final_digest = "-".join(digests_combined_list)
            workflow_config.digest_jobs[job.name] = final_digest

        assert (
            workflow_config.digest_jobs
        ), f"BUG, Workflow with enabled cache must have job digests after configuration, wf [{workflow.name}]"

        print("Check remote cache")

        def fetch_record(job_name, job_digest, cache_):
            if job_digest == "f" * Settings.CACHE_DIGEST_LEN:
                return None
            """Fetch a single record from the cache."""
            record = cache_.fetch_success(job_name=job_name, job_digest=job_digest)
            if record:
                return job_name, record
            return None

        # implement algorithm to skip dependee jobs if dependency is not in the cache
        # Step 1: Fetch records concurrently
        fetched_records = []
        if os.environ.get("DISABLE_CI_CACHE", "0") == "1":
            print("NOTE: CI Cache disabled via GH Variable DISABLE_CI_CACHE=1")
        else:
            with ThreadPoolExecutor(max_workers=200) as executor:
                futures = {
                    executor.submit(fetch_record, job_name, job_digest, cache): job_name
                    for job_name, job_digest in workflow_config.digest_jobs.items()
                    if job_digest != cache.digest.get_null_digest()  # not being cached
                    and job_name
                    not in workflow_config.filtered_jobs  # skipped by user's hook
                }

                for future in futures:
                    result = future.result()
                    if result:  # If a record was found, add it to the fetched list
                        fetched_records.append(result)

        env = _Environment.get()
        # Step 2: Apply the fetched records sequentially
        for job_name, record in fetched_records:
            assert Utils.normalize_string(job_name) not in workflow_config.cache_success
            if workflow.is_event_push() and record.branch != env.BRANCH:
                # TODO: make this behaviour configurable?
                print(
                    f"NOTE: Result for [{job_name}] cached from branch [{record.branch}] - skip for workflow with event=PUSH"
                )
                continue
            workflow_config.cache_success.append(job_name)
            workflow_config.cache_success_base64.append(Utils.to_base64(job_name))
            workflow_config.cache_jobs[job_name] = record
        # single threaded variant
        # for job_name, job_digest in workflow_config.digest_jobs.items():
        #     record = cache.fetch_success(job_name=job_name, job_digest=job_digest)
        #     if record:
        #         assert (
        #             Utils.normalize_string(job_name)
        #             not in workflow_config.cache_success
        #         )
        #         workflow_config.cache_success.append(job_name)
        #         workflow_config.cache_success_base64.append(Utils.to_base64(job_name))
        #         workflow_config.cache_jobs[job_name] = record

        print("Check artifacts to reuse")
        for job in workflow.jobs:
            if (
                job.name in workflow_config.cache_success
                and job.name not in workflow_config.filtered_jobs
            ):
                if job.provides:
                    for artifact_name in job.provides:
                        workflow_config.cache_artifacts[artifact_name] = (
                            workflow_config.cache_jobs[job.name]
                        )
                    if Settings.ENABLE_ARTIFACTS_REPORT:
                        workflow_config.cache_artifacts[job.name] = (
                            workflow_config.cache_jobs[job.name]
                        )

        print(f"Write config to GH's job output")
        with open(env.JOB_OUTPUT_STREAM, "a", encoding="utf8") as f:
            print(
                f"DATA={workflow_config.to_json()}",
                file=f,
            )
        print(f"WorkflowRuntimeConfig: [{workflow_config.to_json(pretty=True)}]")
        print(
            "Dump WorkflowConfig to fs, the next hooks in this job might want to see it"
        )
        workflow_config.dump()

        return workflow_config

    @classmethod
    def pre_run(cls, _workflow, _job, _required_artifacts=None):
        path_prefixes = []
        if _job.name == Settings.CI_CONFIG_JOB_NAME:
            # SPECIAL handling
            return path_prefixes
        env = _Environment.get()
        runtime_config = RunConfig.from_fs(_workflow.name)
        required_artifacts = []
        if _required_artifacts:
            required_artifacts = _required_artifacts
        for artifact in required_artifacts:
            if artifact.name in runtime_config.cache_artifacts:
                record = runtime_config.cache_artifacts[artifact.name]
                print(f"Reuse artifact [{artifact.name}] from [{record}]")
                path_prefixes.append(
                    env.get_s3_prefix_static(
                        record.pr_number, record.branch, record.sha
                    )
                )
            else:
                path_prefixes.append(env.get_s3_prefix())
        return path_prefixes

    @classmethod
    def run(cls, workflow, job):
        pass

    @classmethod
    def post_run(cls, workflow, job):
        if job.name == Settings.CI_CONFIG_JOB_NAME:
            return
        if job.digest_config:
            # cache is enabled, and it's a job that supposed to be cached (has defined digest config)
            workflow_runtime = RunConfig.from_fs(workflow.name)
            job_digest = workflow_runtime.digest_jobs[job.name]
            # if_not_exist=workflow.is_event_pull_request() - to not overwrite record from "push" workflow, as it can reuse only from push, "pull_request" - from both
            Cache.push_success_record(
                job.name,
                job_digest,
                workflow_runtime.sha,
                if_not_exist=workflow.is_event_pull_request(),
            )
