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
            StorageUsage(downloaded=0, uploaded=0, downloaded_details={}, uploaded_details={}).dump()

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


@dataclasses.dataclass
class PipelineUtilization(MetaClasses.SerializableSingleton):
    """Whole-pipeline CPU/RAM utilization and CPU/mem/io stall, accumulated over
    all jobs that qualified for host-metrics labelling.

    Utilization (cpu, mem) is weighted by provisioned resource-time -
    core-seconds (cpu_count * duration) for CPU, GB-seconds
    (mem_total_gb * duration) for RAM - so it is a true efficiency ratio
    (used resource-time / provisioned resource-time) and a bigger runner and/or
    a longer job impacts it more.

    Stalls come in two PSI flavours. ``some`` (time at least one task was
    stalled) is a wall-clock pressure fraction independent of machine size, so
    it is duration-weighted - the average share of qualifying job-time spent
    under pressure. Note this is NOT a single elapsed-workflow fraction: jobs
    run in parallel on different runners, so two 10s jobs where only one stalls
    the whole time read as 50%, not 100%. ``full`` (all tasks stalled at once,
    so the box made zero progress; exists for mem and io, not cpu) wastes the
    whole machine, so it is weighted by core-seconds - the wasted-compute share
    of provisioned CPU.

    Values are kept as running sums so merging across jobs is associative. The
    per-job utilization contributions are reconstructed from the rounded,
    sampled-window averages, so they are estimates (not bit-exact). Percentages
    are derived on read (see to_summary).
    """

    jobs: int = 0
    wall_time_s: float = 0.0  # sum(duration) - the "some"-stall weight/denominator
    cpu_core_s: float = 0.0  # sum(cpu_count * duration) - provisioned core-seconds
    mem_gb_s: float = 0.0  # sum(mem_total_gb * duration) - provisioned GB-seconds
    disk_gb_s: float = 0.0  # sum(disk_total_gb * duration) - provisioned disk GB-seconds
    cpu_used_area: float = 0.0  # sum(avg_cpu% * cpu_count * duration)
    mem_used_area: float = 0.0  # sum(avg_mem% * mem_total_gb * duration)
    disk_used_area: float = 0.0  # sum(peak_disk% * disk_total_gb * duration)
    cpu_stall_area: float = 0.0  # sum(cpu_some_s * 100)  (duration-weighted)
    mem_stall_area: float = 0.0  # sum(mem_some_s * 100)
    io_stall_area: float = 0.0  # sum(io_some_s * 100)
    mem_full_area: float = 0.0  # sum(mem_full_s * cpu_count * 100)  (core-weighted)
    io_full_area: float = 0.0  # sum(io_full_s * cpu_count * 100)
    ext: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def merge_with(self, other: "PipelineUtilization"):
        self.jobs += other.jobs
        self.wall_time_s += other.wall_time_s
        self.cpu_core_s += other.cpu_core_s
        self.mem_gb_s += other.mem_gb_s
        self.disk_gb_s += other.disk_gb_s
        self.cpu_used_area += other.cpu_used_area
        self.mem_used_area += other.mem_used_area
        self.disk_used_area += other.disk_used_area
        self.cpu_stall_area += other.cpu_stall_area
        self.mem_stall_area += other.mem_stall_area
        self.io_stall_area += other.io_stall_area
        self.mem_full_area += other.mem_full_area
        self.io_full_area += other.io_full_area
        return self

    @classmethod
    def file_name_static(cls):
        return f"{Settings.TEMP_DIR}/pipeline_utilization.json"

    @classmethod
    def from_job_metrics(cls, metrics: Dict[str, Any]) -> "PipelineUtilization":
        """Build the single-job contribution from a compacted metrics dict.

        A short job contributes small resource-time weight even when it
        qualified via runner size, so it naturally impacts the KPI less.
        """
        duration = metrics.get("duration") or 0
        cores = metrics.get("cpu_count") or 1
        gb = metrics.get("mem_total_gb") or 0
        averages = metrics.get("averages", {})
        psi = metrics.get("psi", {})
        cpu_avg = averages.get("cpu") or 0
        mem_avg = averages.get("mem") or 0
        # Disk uses the peak footprint (you size a disk for its high-water mark),
        # weighted like RAM by GB-time.
        disk_gb = metrics.get("disk_total_gb") or 0
        disk_peak = (metrics.get("peaks") or {}).get("disk") or 0
        # Stalls are wall-clock PSI fractions; the duration-weighted numerator is
        # stall% * duration = (stall_s / duration * 100) * duration = stall_s * 100,
        # divided by wall_time_s on read.
        cpu_stall_s = psi.get("cpu_s", 0) or 0
        mem_stall_s = psi.get("mem_some_s", 0) or 0
        io_stall_s = psi.get("io_some_s", 0) or 0
        # full stall wastes every core -> weighted by cpu_count.
        mem_full_s = psi.get("mem_full_s", 0) or 0
        io_full_s = psi.get("io_full_s", 0) or 0
        return cls(
            jobs=1,
            wall_time_s=duration,
            cpu_core_s=cores * duration,
            mem_gb_s=gb * duration,
            disk_gb_s=disk_gb * duration,
            cpu_used_area=cpu_avg * cores * duration,
            mem_used_area=mem_avg * gb * duration,
            disk_used_area=disk_peak * disk_gb * duration,
            cpu_stall_area=cpu_stall_s * 100.0,
            mem_stall_area=mem_stall_s * 100.0,
            io_stall_area=io_stall_s * 100.0,
            mem_full_area=mem_full_s * cores * 100.0,
            io_full_area=io_full_s * cores * 100.0,
        )

    def to_summary(self) -> Dict[str, Any]:
        """Derive the human-facing utilization/stall percentages."""

        def pct(area, weight):
            # A utilization/stall percentage is a fraction of time, so clamp to
            # 100 - guards against any residual sampling jitter.
            return round(min(100.0, area / weight), 1) if weight else 0.0

        return {
            "jobs": self.jobs,
            "wall_time_s": round(self.wall_time_s, 1),
            # Provisioned capacity-time (cores/GB * duration), in hours.
            "cpu_hours": round(self.cpu_core_s / 3600.0, 1),
            "mem_gb_hours": round(self.mem_gb_s / 3600.0, 1),
            "disk_gb_hours": round(self.disk_gb_s / 3600.0, 1),
            "cpu_util_pct": pct(self.cpu_used_area, self.cpu_core_s),
            "mem_util_pct": pct(self.mem_used_area, self.mem_gb_s),
            "disk_util_pct": pct(self.disk_used_area, self.disk_gb_s),
            "cpu_stall_pct": pct(self.cpu_stall_area, self.wall_time_s),
            "mem_stall_pct": pct(self.mem_stall_area, self.wall_time_s),
            "io_stall_pct": pct(self.io_stall_area, self.wall_time_s),
            "mem_full_pct": pct(self.mem_full_area, self.cpu_core_s),
            "io_full_pct": pct(self.io_full_area, self.cpu_core_s),
        }
