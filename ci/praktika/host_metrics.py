import json
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .settings import Settings


class HostMetricsCollector:
    """Samples whole-VM CPU and RAM usage in a background thread.

    Dependency-free: reads ``/proc/stat`` and ``/proc/meminfo`` directly, so it
    reflects the load of the whole host (not just this process) regardless of
    whether the job itself runs inside Docker. Each sample is appended to a
    ``jsonl`` file so partial data survives if the runner is killed (e.g. on an
    OOM or a hard timeout). On ``stop`` the samples are decimated with a
    min/max-per-bucket pass that preserves peaks and troughs, and returned as a
    plain dict ready to be stored in ``Result.ext["metrics"]``.

    On a non-Linux host (no ``/proc``) the collector is a no-op and ``stop``
    returns ``None``.
    """

    _CPU_STAT = "/proc/stat"
    _MEMINFO = "/proc/meminfo"

    def __init__(
        self,
        out_file: str = Settings.HOST_METRICS_FILE,
        interval: float = Settings.HOST_METRICS_SAMPLE_INTERVAL_SEC,
        max_points: int = Settings.HOST_METRICS_MAX_POINTS,
    ):
        self._out_file = out_file
        self._interval = max(0.1, float(interval))
        self._max_points = max(2, int(max_points))
        self._available = Path(self._CPU_STAT).exists() and Path(self._MEMINFO).exists()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._samples: List[Dict[str, float]] = []
        self._mem_total_kb = 0
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
        self._thread = threading.Thread(target=self._run, name="host-metrics", daemon=True)
        self._thread.start()
        print(f"NOTE: Host metrics collection started (interval {self._interval}s, file [{self._out_file}])")
        return self

    def stop(self) -> Optional[Dict]:
        """Stop sampling and return the compacted metrics, or None if disabled."""
        if not self._started or self._stopped:
            return None
        self._stopped = True
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval * 2 + 5)
        if not self._samples:
            return None
        return self._compact(self._samples)

    def _run(self):
        # The first /proc/stat read only establishes a baseline; CPU% needs a
        # delta between two reads, so no sample is emitted for it.
        prev_cpu = self._read_cpu_times()
        start = time.monotonic()
        while not self._stop_event.is_set():
            # Sleep first so the first emitted sample already has a valid CPU
            # delta over one interval.
            self._stop_event.wait(self._interval)
            try:
                cur_cpu = self._read_cpu_times()
                cpu_pct = self._cpu_percent(prev_cpu, cur_cpu)
                prev_cpu = cur_cpu
                mem_pct = self._mem_percent()
                sample = {
                    "t": round(time.monotonic() - start, 1),
                    "cpu": cpu_pct,
                    "mem": mem_pct,
                }
            except Exception as e:
                # A transient /proc read failure must not kill the collector.
                print(f"WARNING: Host metrics sample failed: {e}")
                traceback.print_exc()
                continue
            self._samples.append(sample)
            self._append_to_fs(sample)

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

    def _compact(self, samples: List[Dict[str, float]]) -> Dict:
        cpu_points = [(s["t"], s["cpu"]) for s in samples]
        mem_points = [(s["t"], s["mem"]) for s in samples]
        return {
            "interval": self._interval,
            "duration": samples[-1]["t"] if samples else 0,
            "mem_total_gb": round(self._mem_total_kb / 1024.0 / 1024.0, 2),
            "n_raw": len(samples),
            "series": {
                "cpu": self._decimate(cpu_points, self._max_points),
                "mem": self._decimate(mem_points, self._max_points),
            },
        }

    @staticmethod
    def _decimate(points: List[Tuple[float, float]], max_points: int) -> List[List[float]]:
        """Downsample preserving the envelope (per-bucket min and max).

        The first and last samples are always kept. The middle is split into
        buckets; from each bucket the minimum and maximum values are emitted in
        time order, so spikes and dips are never smoothed away.
        """
        n = len(points)
        if n <= max_points:
            return [[t, v] for t, v in points]

        first, last = points[0], points[-1]
        middle = points[1:-1]
        # Two points (min, max) per bucket, plus the fixed first/last.
        n_buckets = max(1, (max_points - 2) // 2)
        bucket_size = len(middle) / n_buckets

        result: List[Tuple[float, float]] = [first]
        for b in range(n_buckets):
            lo = int(b * bucket_size)
            hi = int((b + 1) * bucket_size) if b < n_buckets - 1 else len(middle)
            chunk = middle[lo:hi]
            if not chunk:
                continue
            min_p = min(chunk, key=lambda p: p[1])
            max_p = max(chunk, key=lambda p: p[1])
            # Emit the two extrema in their original time order (dedup if equal).
            pair = sorted({min_p, max_p}, key=lambda p: p[0])
            result.extend(pair)
        result.append(last)
        return [[t, v] for t, v in result]
