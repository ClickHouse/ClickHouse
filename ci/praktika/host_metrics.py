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
    ``stop`` the samples are decimated with a min/max-per-bucket pass that
    preserves peaks and troughs, and returned as a plain dict ready to be stored
    in ``Result.ext["metrics"]``.

    On a non-Linux host (no ``/proc``) the collector is a no-op and ``stop``
    returns ``None``.
    """

    _CPU_STAT = "/proc/stat"
    _MEMINFO = "/proc/meminfo"
    _PSI_CPU = "/proc/pressure/cpu"
    _PSI_MEM = "/proc/pressure/memory"
    _PSI_IO = "/proc/pressure/io"

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
        # Exact global peaks over every fine sample (never decimated away).
        self._cpu_peak = 0.0
        self._mem_peak = 0.0
        self._disk_peak = 0.0
        self._n_fine = 0
        self._psi_start: Optional[Dict[str, int]] = None
        self._psi_end: Optional[Dict[str, int]] = None
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
        # Truncate any stale file from a previous run in the same workspace.
        try:
            Path(self._out_file).parent.mkdir(parents=True, exist_ok=True)
            open(self._out_file, "w", encoding="utf8").close()
        except OSError as e:
            print(f"WARNING: Failed to init host metrics file [{self._out_file}]: {e}")
            self._available = False
            return self
        self._psi_start = self._read_psi()
        self._thread = threading.Thread(target=self._run, name="host-metrics", daemon=True)
        self._thread.start()
        print(f"NOTE: Host metrics collection started (fine {self._fine_interval}s, window {self._report_interval}s, psi {'on' if self._psi_available else 'off'}, file [{self._out_file}])")
        return self

    def stop(self) -> Optional[Dict]:
        """Stop sampling and return the compacted metrics, or None if disabled."""
        if not self._started or self._stopped:
            return None
        self._stopped = True
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._report_interval * 2 + 5)
        if not self._samples:
            return None
        return self._compact(self._samples)

    def _run(self):
        # The first /proc/stat read only establishes a baseline; CPU% needs a
        # delta between two reads, so no sample is emitted for it.
        prev_cpu = self._read_cpu_times()
        start = time.monotonic()
        window_start = start
        win_cpu: List[float] = []
        win_mem: List[float] = []
        win_disk: List[float] = []

        def flush_window(now: float):
            if not win_cpu:
                return
            sample = {
                "t": round(now - start, 1),
                "cpu": round(sum(win_cpu) / len(win_cpu), 1),
                "mem": round(sum(win_mem) / len(win_mem), 1),
                "disk": round(sum(win_disk) / len(win_disk), 1),
                "cpu_peak": round(max(win_cpu), 1),
                "mem_peak": round(max(win_mem), 1),
                "disk_peak": round(max(win_disk), 1),
            }
            self._samples.append(sample)
            self._append_to_fs(sample)
            win_cpu.clear()
            win_mem.clear()
            win_disk.clear()

        while not self._stop_event.is_set():
            # Sleep first so the first emitted sample already has a valid CPU
            # delta over one fine interval.
            self._stop_event.wait(self._fine_interval)
            try:
                cur_cpu = self._read_cpu_times()
                cpu_pct = self._cpu_percent(prev_cpu, cur_cpu)
                prev_cpu = cur_cpu
                mem_pct = self._mem_percent()
                disk_pct = self._read_disk_percent()
            except Exception as e:
                # A transient /proc read failure must not kill the collector.
                print(f"WARNING: Host metrics sample failed: {e}")
                traceback.print_exc()
                continue
            win_cpu.append(cpu_pct)
            win_mem.append(mem_pct)
            win_disk.append(disk_pct)
            self._n_fine += 1
            # Exact global peaks, immune to windowing/decimation.
            self._cpu_peak = max(self._cpu_peak, cpu_pct)
            self._mem_peak = max(self._mem_peak, mem_pct)
            self._disk_peak = max(self._disk_peak, disk_pct)
            now = time.monotonic()
            if now - window_start >= self._report_interval:
                flush_window(now)
                window_start = now

        # Emit the trailing partial window and capture PSI as close to the real
        # end of the job as possible.
        flush_window(time.monotonic())
        self._psi_end = self._read_psi()

    def _append_to_fs(self, sample: Dict[str, float]):
        try:
            with open(self._out_file, "a", encoding="utf8") as f:
                f.write(json.dumps(sample) + "\n")
        except OSError as e:
            print(f"WARNING: Failed to append host metrics sample: {e}")

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
        cpu_points = [(s["t"], s["cpu"], s["cpu_peak"]) for s in samples]
        mem_points = [(s["t"], s["mem"], s["mem_peak"]) for s in samples]
        disk_points = [(s["t"], s["disk"], s["disk_peak"]) for s in samples]
        mem_total_gb = round(self._mem_total_kb / 1024.0 / 1024.0, 2)
        disk_total_gb = round(self._disk_total_kb / 1024.0 / 1024.0, 2)
        result = {
            "interval": self._report_interval,
            "fine_interval": self._fine_interval,
            "duration": samples[-1]["t"] if samples else 0,
            "mem_total_gb": mem_total_gb,
            "disk_total_gb": disk_total_gb,
            "n_raw": self._n_fine,
            "n_windows": len(samples),
            "peaks": {
                "cpu": round(self._cpu_peak, 1),
                "mem": round(self._mem_peak, 1),
                "mem_gb": round(mem_total_gb * self._mem_peak / 100.0, 2),
                "disk": round(self._disk_peak, 1),
                "disk_gb": round(disk_total_gb * self._disk_peak / 100.0, 2),
            },
            "series": {
                "cpu": self._decimate(cpu_points, self._max_points),
                "mem": self._decimate(mem_points, self._max_points),
                "disk": self._decimate(disk_points, self._max_points),
            },
        }
        psi = self._psi_delta()
        if psi:
            result["psi"] = psi
        return result

    @staticmethod
    def _decimate(points: List[Tuple[float, float, float]], max_points: int) -> List[List[float]]:
        """Downsample (t, avg, peak) triples preserving the envelope.

        The first and last samples are always kept. The middle is split into
        buckets; from each bucket the sample with the highest ``peak`` and the
        one with the lowest ``avg`` are emitted in time order, so both spikes
        (via peak) and dips (via avg) survive.
        """
        n = len(points)
        if n <= max_points:
            return [[t, a, p] for t, a, p in points]

        first, last = points[0], points[-1]
        middle = points[1:-1]
        # Two points per bucket, plus the fixed first/last.
        n_buckets = max(1, (max_points - 2) // 2)
        bucket_size = len(middle) / n_buckets

        result: List[Tuple[float, float, float]] = [first]
        for b in range(n_buckets):
            lo = int(b * bucket_size)
            hi = int((b + 1) * bucket_size) if b < n_buckets - 1 else len(middle)
            chunk = middle[lo:hi]
            if not chunk:
                continue
            hi_peak = max(chunk, key=lambda p: p[2])
            lo_avg = min(chunk, key=lambda p: p[1])
            # Emit the two representatives in time order (dedup if identical).
            pair = sorted({hi_peak, lo_avg}, key=lambda p: p[0])
            result.extend(pair)
        result.append(last)
        return [[t, a, p] for t, a, p in result]
