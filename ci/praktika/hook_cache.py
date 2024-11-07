from praktika._environment import _Environment
from praktika.cache import Cache
from praktika.mangle import _get_workflows
from praktika.runtime import RunConfig
from praktika.settings import Settings
from praktika.utils import Utils


class CacheRunnerHooks:
    @classmethod
    def configure(cls, _workflow):
        workflow_config = RunConfig.from_fs(_workflow.name)
        cache = Cache()
        assert _Environment.get().WORKFLOW_NAME
        workflow = _get_workflows(name=_Environment.get().WORKFLOW_NAME)[0]
        print(f"Workflow Configure, workflow [{workflow.name}]")
        assert (
            workflow.enable_cache
        ), f"Outdated yaml pipelines or BUG. Configuration must be run only for workflow with enabled cache, workflow [{workflow.name}]"
        artifact_digest_map = {}
        job_digest_map = {}
        for job in workflow.jobs:
            if not job.digest_config:
                print(
                    f"NOTE: job [{job.name}] has no Config.digest_config - skip cache check, always run"
                )
            digest = cache.digest.calc_job_digest(job_config=job)
            job_digest_map[job.name] = digest
            if job.provides:
                # assign the job digest also to the artifacts it provides
                for artifact in job.provides:
                    artifact_digest_map[artifact] = digest
        for job in workflow.jobs:
            digests_combined_list = []
            if job.requires:
                # include digest of required artifact to the job digest, so that they affect job state
                for artifact_name in job.requires:
                    if artifact_name not in [
                        artifact.name for artifact in workflow.artifacts
                    ]:
                        # phony artifact assumed to be not affecting jobs that depend on it
                        continue
                    digests_combined_list.append(artifact_digest_map[artifact_name])
            digests_combined_list.append(job_digest_map[job.name])
            final_digest = "-".join(digests_combined_list)
            workflow_config.digest_jobs[job.name] = final_digest

        assert (
            workflow_config.digest_jobs
        ), f"BUG, Workflow with enabled cache must have job digests after configuration, wf [{workflow.name}]"

        print("Check remote cache")
        job_to_cache_record = {}
        for job_name, job_digest in workflow_config.digest_jobs.items():
            record = cache.fetch_success(job_name=job_name, job_digest=job_digest)
            if record:
                assert (
                    Utils.normalize_string(job_name)
                    not in workflow_config.cache_success
                )
                workflow_config.cache_success.append(job_name)
                workflow_config.cache_success_base64.append(Utils.to_base64(job_name))
                job_to_cache_record[job_name] = record

        print("Check artifacts to reuse")
        for job in workflow.jobs:
            if job.name in workflow_config.cache_success:
                if job.provides:
                    for artifact_name in job.provides:
                        workflow_config.cache_artifacts[artifact_name] = (
                            job_to_cache_record[job.name]
                        )

        print(f"Write config to GH's job output")
        with open(_Environment.get().JOB_OUTPUT_STREAM, "a", encoding="utf8") as f:
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
            Cache.push_success_record(job.name, job_digest, workflow_runtime.sha)
