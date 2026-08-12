import json
import os
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .settings import Settings


class HostMetricsCollector:
    """Samples whole-VM CPU, RAM and disk usage in a background thread.

    Dependency-free: reads ``/proc/stat`` and ``/proc/meminfo`` and calls
    ``os.statvfs`` directly, so it reflects the load of the whole host (not just
    this process) regardless of whether the job itself runs inside Docker.

    Three usage series are tracked (all as 0-100%): ``cpu`` (busy%), ``mem``
    (used%) and ``disk`` (used% of the workspace filesystem).

    Spike handling. Inputs are read at a fine cadence
    (``HOST_METRICS_FINE_INTERVAL_SEC``) but the timeline emits one aggregated
    point per reporting window (``HOST_METRICS_SAMPLE_INTERVAL_SEC``) carrying
    both the window average and its peak, so a short CPU burst or a transient
    RAM allocation shows up as the window's peak instead of being averaged away.
    Two signals are peak-exact and independent of the sampling rate:

    * ``peaks`` - the maximum CPU%/RAM%/disk% seen across every fine sample.
    * ``psi`` - Linux Pressure Stall Information (``/proc/pressure/*``, for cpu,
      memory and io). The kernel accumulates stalled time continuously, so
      reading the totals once at start and once at stop captures all contention
      during the run, even stalls shorter than the sampling interval.

    Each aggregated sample is appended to a ``jsonl`` file so partial data
    survives if the runner is killed (e.g. on an OOM or a hard timeout). On
    ``stop`` the samples are decimated with a per-bucket pass that preserves the
    envelope of both lines (highest/lowest average and highest peak), and
    returned as a plain dict ready to be stored in ``Result.ext["metrics"]``.

    On a non-Linux host (no ``/proc``) the collector is a no-op and ``stop``
    returns ``None``.
    """

    _CPU_STAT = "/proc/stat"
    _MEMINFO = "/proc/meminfo"
    _PSI_CPU = "/proc/pressure/cpu"
    _PSI_MEM = "/proc/pressure/memory"
    _PSI_IO = "/proc/pressure/io"

    # Utilization label thresholds (see classify). Percentages unless noted.
    _UNDER_MEM_PCT = 40.0  # peak RAM below this -> RAM over-provisioned
    _UNDER_CPU_PCT = 20.0  # average CPU below this -> CPU over-provisioned
    _RAM_PRESSURE_PCT = 95.0  # peak RAM at/above this -> RAM under-provisioned
    _MEM_STALL_RATIO = 0.02  # mem full-stall / duration above this -> RAM pressure
    _CPU_STALL_RATIO = 0.5  # cpu stall seconds / duration above this -> CPU-bound
    _IO_STALL_RATIO = 0.4  # io stall seconds / duration above this -> IO-bound
    _DISK_WARN_PCT = 85.0  # peak disk at/above this -> almost full

    def __init__(
        self,
        out_file: str = Settings.HOST_METRICS_FILE,
        interval: float = Settings.HOST_METRICS_SAMPLE_INTERVAL_SEC,
        fine_interval: float = Settings.HOST_METRICS_FINE_INTERVAL_SEC,
        max_points: int = Settings.HOST_METRICS_MAX_POINTS,
        disk_path: str = Settings.HOST_METRICS_DISK_PATH,
    ):
        self._out_file = out_file
        self._report_interval = max(0.1, float(interval))
        # Fine cadence never exceeds the reporting window and stays sane.
        self._fine_interval = min(self._report_interval, max(0.05, float(fine_interval)))
        self._max_points = max(2, int(max_points))
        self._disk_path = disk_path
        self._available = Path(self._CPU_STAT).exists() and Path(self._MEMINFO).exists()
        self._psi_available = Path(self._PSI_CPU).exists()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Aggregated per-window samples: {t, cpu, mem, disk, *_peak}.
        self._samples: List[Dict[str, float]] = []
        self._mem_total_kb = 0
        self._disk_total_kb = 0
        self._cpu_count = 0
        # Set in start() by probing disk_path; a bad path disables only the disk
        # series, never CPU/RAM/PSI.
        self._disk_available = False
        # Exact global peaks over every fine sample (never decimated away).
        self._cpu_peak = 0.0
        self._mem_peak = 0.0
        self._disk_peak = 0.0
        # Time-weighted running sums for whole-run averages (area = value * dt).
        self._cpu_area = 0.0
        self._mem_area = 0.0
        self._disk_area = 0.0
        self._dt_total = 0.0
        self._disk_dt_total = 0.0
        self._n_fine = 0
        self._psi_start: Optional[Dict[str, int]] = None
        self._psi_end: Optional[Dict[str, int]] = None
        # Monotonic start reference and the true elapsed span, used as the job
        # duration so stall% is normalized by real wall time (not by the last
        # sample timestamp, which lags when the sampler is starved under load).
        self._t0 = 0.0
        self._elapsed_s = 0.0
        self._started = False
        self._stopped = False

    def start(self) -> "HostMetricsCollector":
        if not Settings.HOST_METRICS_ENABLED:
            return self
        if not self._available:
            print("NOTE: Host metrics collection is not available on this platform (no /proc) - skipping")
            return self
        if self._started:
            return self
        self._started = True
        self._cpu_count = self._read_cpu_count()
        # Truncate any stale file from a previous run in the same workspace.
        try:
            Path(self._out_file).parent.mkdir(parents=True, exist_ok=True)
            open(self._out_file, "w", encoding="utf8").close()
        except OSError as e:
            print(f"WARNING: Failed to init host metrics file [{self._out_file}]: {e}")
            self._available = False
            return self
        # Probe the disk path once up front. If it is missing/inaccessible the
        # disk series is simply disabled - it must never blank the CPU/RAM/PSI
        # collection, which shares the sampling loop.
        try:
            self._read_disk_percent()
            self._disk_available = True
        except OSError as e:
            print(f"WARNING: disk path [{self._disk_path}] is not accessible ({e}) - disk series disabled")
            self._disk_available = False
        self._t0 = time.monotonic()
        self._psi_start = self._read_psi()
        self._thread = threading.Thread(target=self._run, name="host-metrics", daemon=True)
        self._thread.start()
        print(
            f"NOTE: Host metrics collection started (fine {self._fine_interval}s, window {self._report_interval}s, disk {'on' if self._disk_available else 'off'}, psi {'on' if self._psi_available else 'off'}, file [{self._out_file}])"
        )
        return self

    def stop(self) -> Optional[Dict]:
        """Stop sampling and return the compacted metrics, or None if disabled."""
        if not self._started or self._stopped:
            return None
        self._stopped = True
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._report_interval * 2 + 5)
        # Fallback if the thread was starved / timed out before recording these.
        # Capturing the real elapsed span here (not the last sample timestamp) is
        # what keeps stall% <= 100%: a starved sampler emits stale sample times,
        # but the PSI delta always spans the true wall-clock interval.
        if self._psi_end is None:
            self._psi_end = self._read_psi()
            self._elapsed_s = time.monotonic() - self._t0
        if not self._samples:
            return None
        return self._compact(self._samples)

    def _run(self):
        # The first /proc/stat read only establishes a baseline; CPU% needs a
        # delta between two reads, so no sample is emitted for it.
        prev_cpu = self._read_cpu_times()
        start = self._t0
        window_start = start
        last_time = start
        # Each entry is (value, dt) so the window average can be time-weighted:
        # the forced tail sample after stop() may cover only a few ms and must
        # not carry the same weight as a full fine interval.
        win_cpu: List[Tuple[float, float]] = []
        win_mem: List[Tuple[float, float]] = []
        win_disk: List[Tuple[float, float]] = []

        def wmean(pairs: List[Tuple[float, float]]) -> float:
            total_dt = sum(dt for _, dt in pairs)
            if total_dt <= 0:
                return round(sum(v for v, _ in pairs) / len(pairs), 1)
            return round(sum(v * dt for v, dt in pairs) / total_dt, 1)

        def flush_window(now: float):
            if not win_cpu:
                return
            sample = {
                "t": round(now - start, 1),
                "cpu": wmean(win_cpu),
                "mem": wmean(win_mem),
                "cpu_peak": round(max(v for v, _ in win_cpu), 1),
                "mem_peak": round(max(v for v, _ in win_mem), 1),
            }
            if win_disk:
                sample["disk"] = wmean(win_disk)
                sample["disk_peak"] = round(max(v for v, _ in win_disk), 1)
            self._samples.append(sample)
            self._append_to_fs(sample)
            win_cpu.clear()
            win_mem.clear()
            win_disk.clear()

        # Sample-then-check so at least one fine sample is always taken, even if
        # stop() fires before the first wait completes - otherwise a very short
        # job (or a start()/stop() race) would collect nothing and discard the
        # peaks/PSI entirely.
        while True:
            # Sleep first so the emitted sample has a valid CPU delta over one
            # fine interval; returns immediately once stop() is requested.
            stopped = self._stop_event.wait(self._fine_interval)
            try:
                cur_cpu = self._read_cpu_times()
                cpu_pct = self._cpu_percent(prev_cpu, cur_cpu)
                prev_cpu = cur_cpu
                mem_pct = self._mem_percent()
            except Exception as e:
                # A transient /proc read failure must not kill the collector.
                print(f"WARNING: Host metrics sample failed: {e}")
                traceback.print_exc()
                if stopped:
                    break
                continue
            now = time.monotonic()
            dt = now - last_time
            last_time = now
            win_cpu.append((cpu_pct, dt))
            win_mem.append((mem_pct, dt))
            self._n_fine += 1
            # Exact global peaks, immune to windowing/decimation.
            self._cpu_peak = max(self._cpu_peak, cpu_pct)
            self._mem_peak = max(self._mem_peak, mem_pct)
            # Time-weighted running totals for the whole-run averages.
            self._cpu_area += cpu_pct * dt
            self._mem_area += mem_pct * dt
            self._dt_total += dt
            # Disk is sampled independently: a failure here disables only the
            # disk series and leaves CPU/RAM/PSI intact.
            if self._disk_available:
                try:
                    disk_pct = self._read_disk_percent()
                except OSError as e:
                    print(f"WARNING: disk read failed ({e}) - disabling disk series")
                    self._disk_available = False
                else:
                    win_disk.append((disk_pct, dt))
                    self._disk_peak = max(self._disk_peak, disk_pct)
                    self._disk_area += disk_pct * dt
                    self._disk_dt_total += dt
            if now - window_start >= self._report_interval:
                flush_window(now)
                window_start = now
            if stopped:
                break

        # Emit the trailing partial window and capture PSI as close to the real
        # end of the job as possible. Record the true elapsed span (>= the PSI
        # delta interval) so stall percentages stay bounded by 100%.
        flush_window(time.monotonic())
        self._psi_end = self._read_psi()
        self._elapsed_s = time.monotonic() - self._t0

    def _append_to_fs(self, sample: Dict[str, float]):
        try:
            with open(self._out_file, "a", encoding="utf8") as f:
                f.write(json.dumps(sample) + "\n")
        except OSError as e:
            print(f"WARNING: Failed to append host metrics sample: {e}")

    def _read_cpu_count(self) -> int:
        """Number of logical CPUs from the per-cpu lines in ``/proc/stat``.

        Matches the whole-VM denominator of the aggregate ``cpu`` line, so it is
        the right capacity for weighting CPU utilization by "core-seconds".
        """
        try:
            count = 0
            with open(self._CPU_STAT, "r", encoding="utf8") as f:
                for line in f:
                    if line.startswith("cpu") and len(line) > 3 and line[3].isdigit():
                        count += 1
            return count or 1
        except OSError:
            return 1

    def _read_cpu_times(self) -> Tuple[int, int]:
        """Return (idle_all, total) jiffies from the aggregate ``cpu`` line."""
        with open(self._CPU_STAT, "r", encoding="utf8") as f:
            line = f.readline()
        # cpu  user nice system idle iowait irq softirq steal guest guest_nice
        fields = [int(x) for x in line.split()[1:]]
        idle = fields[3] + (fields[4] if len(fields) > 4 else 0)  # idle + iowait
        total = sum(fields)
        return idle, total

    @staticmethod
    def _cpu_percent(prev: Tuple[int, int], cur: Tuple[int, int]) -> float:
        idle_delta = cur[0] - prev[0]
        total_delta = cur[1] - prev[1]
        if total_delta <= 0:
            return 0.0
        busy = 100.0 * (total_delta - idle_delta) / total_delta
        return round(max(0.0, min(100.0, busy)), 1)

    def _mem_percent(self) -> float:
        total_kb = 0
        available_kb = 0
        with open(self._MEMINFO, "r", encoding="utf8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    total_kb = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    available_kb = int(line.split()[1])
                if total_kb and available_kb:
                    break
        if not total_kb:
            return 0.0
        self._mem_total_kb = total_kb
        used = total_kb - available_kb
        return round(max(0.0, min(100.0, 100.0 * used / total_kb)), 1)

    def _read_disk_percent(self) -> float:
        """Return the workspace filesystem used%, matching ``df`` semantics."""
        st = os.statvfs(self._disk_path)
        if not st.f_blocks:
            return 0.0
        self._disk_total_kb = st.f_blocks * st.f_frsize // 1024
        used = st.f_blocks - st.f_bfree
        # df's percentage excludes root-reserved blocks: used / (used + avail).
        denom = used + st.f_bavail
        if denom <= 0:
            return 0.0
        return round(max(0.0, min(100.0, 100.0 * used / denom)), 1)

    def _read_psi(self) -> Optional[Dict[str, int]]:
        """Read cumulative PSI stall totals (microseconds), or None if absent.

        ``/proc/pressure/cpu`` has only a ``some`` line; ``/proc/pressure/memory``
        and ``/proc/pressure/io`` have both ``some`` (any task stalled) and
        ``full`` (all tasks stalled).
        """
        if not self._psi_available:
            return None
        try:

            def total_for(path: str, kind: str) -> int:
                # A single missing resource (e.g. io pressure) must not drop the
                # others, so treat an absent file as zero rather than failing.
                try:
                    f = open(path, "r", encoding="utf8")
                except FileNotFoundError:
                    return 0
                with f:
                    for line in f:
                        parts = line.split()
                        if parts and parts[0] == kind:
                            for p in parts[1:]:
                                if p.startswith("total="):
                                    return int(p.split("=", 1)[1])
                return 0

            return {
                "cpu_some": total_for(self._PSI_CPU, "some"),
                "mem_some": total_for(self._PSI_MEM, "some"),
                "mem_full": total_for(self._PSI_MEM, "full"),
                "io_some": total_for(self._PSI_IO, "some"),
                "io_full": total_for(self._PSI_IO, "full"),
            }
        except (OSError, ValueError) as e:
            print(f"WARNING: Failed to read PSI: {e}")
            return None

    def _psi_delta(self) -> Optional[Dict[str, float]]:
        if not self._psi_start or not self._psi_end:
            return None

        def secs(key: str) -> float:
            return round(max(0, self._psi_end[key] - self._psi_start[key]) / 1e6, 2)

        return {
            "cpu_s": secs("cpu_some"),
            "mem_some_s": secs("mem_some"),
            "mem_full_s": secs("mem_full"),
            "io_some_s": secs("io_some"),
            "io_full_s": secs("io_full"),
        }

    def _compact(self, samples: List[Dict[str, float]]) -> Dict:
        mem_total_gb = round(self._mem_total_kb / 1024.0 / 1024.0, 2)
        peaks = {
            "cpu": round(self._cpu_peak, 1),
            "mem": round(self._mem_peak, 1),
            "mem_gb": round(mem_total_gb * self._mem_peak / 100.0, 2),
        }
        averages = {
            "cpu": round(self._cpu_area / self._dt_total, 1) if self._dt_total else 0.0,
            "mem": round(self._mem_area / self._dt_total, 1) if self._dt_total else 0.0,
        }
        if self._disk_dt_total:
            averages["disk"] = round(self._disk_area / self._disk_dt_total, 1)
        series = {
            "cpu": self._decimate([(s["t"], s["cpu"], s["cpu_peak"]) for s in samples], self._max_points),
            "mem": self._decimate([(s["t"], s["mem"], s["mem_peak"]) for s in samples], self._max_points),
        }
        result = {
            "interval": self._report_interval,
            "fine_interval": self._fine_interval,
            "duration": round(self._elapsed_s, 1)
            if self._elapsed_s
            else (samples[-1]["t"] if samples else 0),
            "mem_total_gb": mem_total_gb,
            "cpu_count": self._cpu_count,
            "n_raw": self._n_fine,
            "n_windows": len(samples),
            "peaks": peaks,
            "averages": averages,
            "series": series,
        }
        # Disk is optional: present only when the disk path was sampled.
        disk_points = [(s["t"], s["disk"], s["disk_peak"]) for s in samples if "disk" in s]
        if disk_points:
            disk_total_gb = round(self._disk_total_kb / 1024.0 / 1024.0, 2)
            series["disk"] = self._decimate(disk_points, self._max_points)
            result["disk_total_gb"] = disk_total_gb
            peaks["disk"] = round(self._disk_peak, 1)
            peaks["disk_gb"] = round(disk_total_gb * self._disk_peak / 100.0, 2)
        psi = self._psi_delta()
        if psi:
            result["psi"] = psi
        return result

    @staticmethod
    def _decimate(points: List[Tuple[float, float, float]], max_points: int) -> List[List[float]]:
        """Downsample (t, avg, peak) triples preserving both lines' envelope.

        The first and last samples are always kept. The middle is split into
        buckets; from each bucket three representatives are emitted in time
        order - the highest ``avg``, the lowest ``avg`` and the highest ``peak``
        - so a sustained high average, an idle dip, and a short spike all
        survive. Keeping the highest ``avg`` matters because the chart draws the
        average as its solid line: dropping it would let a spike in one window
        hide sustained load in another.
        """
        n = len(points)
        if n <= max_points:
            return [[t, a, p] for t, a, p in points]

        first, last = points[0], points[-1]
        middle = points[1:-1]
        # Up to three points per bucket, plus the fixed first/last.
        n_buckets = max(1, (max_points - 2) // 3)
        bucket_size = len(middle) / n_buckets

        result: List[Tuple[float, float, float]] = [first]
        for b in range(n_buckets):
            lo = int(b * bucket_size)
            hi = int((b + 1) * bucket_size) if b < n_buckets - 1 else len(middle)
            chunk = middle[lo:hi]
            if not chunk:
                continue
            reps = {
                max(chunk, key=lambda p: p[1]),  # highest average (sustained load)
                min(chunk, key=lambda p: p[1]),  # lowest average (idle dips)
                max(chunk, key=lambda p: p[2]),  # highest peak (short spikes)
            }
            # Emit the distinct representatives in time order.
            result.extend(sorted(reps, key=lambda p: p[0]))
        result.append(last)
        return [[t, a, p] for t, a, p in result]

    @staticmethod
    def qualifies(metrics: Optional[Dict]) -> bool:
        """Whether a job is substantial enough to label / count for the pipeline
        utilization KPI: it ran long enough OR on a host with enough RAM. Short
        jobs on small runners are too noisy and not worth right-sizing.
        """
        if not metrics:
            return False
        duration = metrics.get("duration") or 0
        mem_gb = metrics.get("mem_total_gb") or 0
        return duration >= Settings.HOST_METRICS_MIN_LABEL_DURATION_SEC or mem_gb > Settings.HOST_METRICS_MIN_LABEL_MEM_GB

    @classmethod
    def classify(cls, metrics: Optional[Dict]) -> List[Tuple[str, str]]:
        """Derive over/under-utilization labels from a compacted metrics dict.

        Returns a list of ``(label, hint)`` pairs for ``Result.set_label``.
        Only jobs that ran at least ``HOST_METRICS_MIN_LABEL_DURATION_SEC`` are
        labelled - shorter runs are noisy and not worth right-sizing. RAM uses
        the peak (you provision for the worst case); CPU uses the whole-run
        average (brief bursts do not justify more cores); a large CPU stall or a
        near-full disk are surfaced as their own warnings.
        """
        labels: List[Tuple[str, str]] = []
        if not cls.qualifies(metrics):
            return labels

        duration = metrics.get("duration") or 0
        mem_gb = metrics.get("mem_total_gb") or 0
        peaks = metrics.get("peaks", {})
        averages = metrics.get("averages", {})
        psi = metrics.get("psi", {})
        mem_peak = peaks.get("mem")
        cpu_avg = averages.get("cpu")
        disk_peak = peaks.get("disk")
        disk_gb = metrics.get("disk_total_gb")
        mem_full_s = psi.get("mem_full_s", 0)
        cpu_s = psi.get("cpu_s", 0)
        io_s = psi.get("io_some_s", 0)

        # RAM: pressure (under-provisioned) takes precedence over idle headroom.
        # Only a sustained memory full-stall counts - a brief blip (e.g. 0.01s
        # over a long build) is noise, not pressure, so require a fraction of
        # the runtime rather than any non-zero value.
        mem_full_ratio = mem_full_s / duration if duration else 0
        if (mem_peak is not None and mem_peak >= cls._RAM_PRESSURE_PCT) or mem_full_ratio > cls._MEM_STALL_RATIO:
            labels.append(
                (
                    "ram-pressure",
                    f"Peak RAM {mem_peak}% of {mem_gb}GB, memory stalled {mem_full_s}s ({round(100 * mem_full_ratio)}% of runtime) - consider more RAM",
                )
            )
        elif mem_peak is not None and mem_peak < cls._UNDER_MEM_PCT:
            labels.append(
                (
                    "under-utilized RAM",
                    f"Peak RAM only {mem_peak}% of {mem_gb}GB - consider a smaller runner",
                )
            )

        # CPU: sustained contention (CPU-bound) vs mostly-idle cores.
        stall_ratio = cpu_s / duration if duration else 0
        if stall_ratio > cls._CPU_STALL_RATIO:
            labels.append(
                (
                    "cpu-bound",
                    f"CPU stalled {cpu_s}s ({round(100 * stall_ratio)}% of runtime) - consider more CPU",
                )
            )
        elif cpu_avg is not None and cpu_avg < cls._UNDER_CPU_PCT:
            labels.append(
                (
                    "under-utilized CPU",
                    f"Average CPU only {cpu_avg}% - consider a smaller runner",
                )
            )

        # IO: sustained stall waiting on storage.
        io_ratio = io_s / duration if duration else 0
        if io_ratio > cls._IO_STALL_RATIO:
            labels.append(
                (
                    "io-bound",
                    f"IO stalled {io_s}s ({round(100 * io_ratio)}% of runtime) - storage may be the bottleneck",
                )
            )

        # Disk: nearly full.
        if disk_peak is not None and disk_peak >= cls._DISK_WARN_PCT:
            labels.append(
                (
                    "disk-almost-full",
                    f"Peak disk {disk_peak}% of {disk_gb}GB",
                )
            )

        return labels
