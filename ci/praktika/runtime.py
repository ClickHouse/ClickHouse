from dataclasses import dataclass
from typing import Dict, List

from .cache import Cache
from .settings import Settings
from .utils import MetaClasses, Utils


@dataclass
class RunConfig(MetaClasses.Serializable):
    name: str
    digest_jobs: Dict[str, str]
    digest_dockers: Dict[str, str]
    cache_success: List[str]
    # there are might be issue with special characters in job names if used directly in yaml syntax - create base64 encoded list to avoid this
    cache_success_base64: List[str]
    cache_artifacts: Dict[str, Cache.CacheRecord]
    cache_jobs: Dict[str, Cache.CacheRecord]
    sha: str

    @classmethod
    def from_dict(cls, obj):
        cache_artifacts = obj["cache_artifacts"]
        cache_jobs = obj["cache_jobs"]
        cache_artifacts_deserialized = {}
        cache_jobs_deserialized = {}
        for artifact_name, cache_artifact in cache_artifacts.items():
            cache_artifacts_deserialized[artifact_name] = Cache.CacheRecord.from_dict(
                cache_artifact
            )
        obj["cache_artifacts"] = cache_artifacts_deserialized
        for job_name, cache_jobs in cache_jobs.items():
            cache_jobs_deserialized[job_name] = Cache.CacheRecord.from_dict(cache_jobs)
        obj["cache_jobs"] = cache_artifacts_deserialized
        return RunConfig(**obj)

    @classmethod
    def file_name_static(cls, name):
        return (
            f"{Settings.TEMP_DIR}/workflow_config_{Utils.normalize_string(name)}.json"
        )
