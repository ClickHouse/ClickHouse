import dataclasses
import os
from pathlib import Path
from typing import Any, Dict, List

from .settings import Settings
from .utils import MetaClasses


@dataclasses.dataclass
class StorageUsage(MetaClasses.SerializableSingleton):
    downloaded: int = 0
    uploaded: int = 0
    downloaded_details: Dict[str, int] = dataclasses.field(default_factory=dict)
    uploaded_details: Dict[str, int] = dataclasses.field(default_factory=dict)
    ext: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def merge_with(self, storage_usage: "StorageUsage"):
        self.downloaded += storage_usage.downloaded
        self.uploaded += storage_usage.uploaded
        for k, v in storage_usage.downloaded_details.items():
            if k in self.downloaded_details:
                self.downloaded_details[k] += v
            else:
                self.downloaded_details[k] = v
        for k, v in storage_usage.uploaded_details.items():
            if k in self.uploaded_details:
                self.uploaded_details[k] += v
            else:
                self.uploaded_details[k] = v
        return self

    @classmethod
    def file_name_static(cls):
        return f"{Settings.TEMP_DIR}/storage_usage.json"

    @classmethod
    def _init(cls):
        if not StorageUsage.exist():
            print("NOTE: UsageStorage data will be initialized")
            StorageUsage(
                downloaded=0, uploaded=0, downloaded_details={}, uploaded_details={}
            ).dump()

    @classmethod
    def add_downloaded(cls, file_path):
        cls._init()
        if not Path(file_path).exists():
            return
        file_name = str(file_path).split("/")[-1]
        usage = cls.from_fs()
        file_zize = cls.get_size_bytes(file_path)
        usage.downloaded += file_zize
        if file_name in usage.downloaded_details:
            print(f"WARNING: Duplicated download for filename [{file_name}]")
            usage.downloaded_details[file_name] += file_zize
        else:
            usage.downloaded_details[file_name] = file_zize
        usage.dump()

    @classmethod
    def add_uploaded(cls, file_path):
        cls._init()
        if not Path(file_path).exists():
            return
        file_name = str(file_path).split("/")[-1]
        usage = cls.from_fs()
        file_zize = cls.get_size_bytes(file_path)
        usage.uploaded += file_zize
        if file_name in usage.uploaded_details:
            print(f"WARNING: Duplicated upload for filename [{file_name}]")
            usage.uploaded_details[file_name] += file_zize
        else:
            usage.uploaded_details[file_name] = file_zize
        usage.dump()

    @classmethod
    def get_size_bytes(cls, file_path):
        return os.path.getsize(file_path)


@dataclasses.dataclass
class ComputeUsage(MetaClasses.SerializableSingleton):
    # map runner type to usage time
    runners_usage: Dict[str, int] = dataclasses.field(default_factory=dict)
    # map runner type to list of jobs
    details: Dict[str, List[str]] = dataclasses.field(default_factory=dict)
    ext: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def merge_with(self, usage: "ComputeUsage"):
        for k, v in usage.runners_usage.items():
            jobs = usage.details[k]
            assert len(jobs) == 1
            self.add_usage(k, v, jobs[0])
        return self

    @classmethod
    def file_name_static(cls):
        return f"{Settings.TEMP_DIR}/compute_usage.json"

    @classmethod
    def _init(cls):
        if not ComputeUsage.exist():
            ComputeUsage().dump()

    def set_usage(self, runner_str, duration, job_name):
        self.runners_usage[runner_str] = duration
        self.details[runner_str] = [job_name]
        return self

    def add_usage(self, runner_str, duration, job_name):
        if runner_str in self.runners_usage:
            self.runners_usage[runner_str] += duration
            self.details[runner_str].append(job_name)
        else:
            self.set_usage(runner_str, duration, job_name)
        return self
