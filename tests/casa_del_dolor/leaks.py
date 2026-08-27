#!/usr/bin/env python3
"""
ClickHouse Memory Leak Detection Script

Monitors ClickHouse memory usage over multiple iterations to detect potential leaks.
Uses system.asynchronous_metrics for accurate memory tracking.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from typing import Optional
from integration.helpers.client import Client, CommandRequest
from integration.helpers.cluster import ClickHouseCluster, ClickHouseInstance


@dataclass
class MemorySnapshot:
    """Represents a memory measurement at a point in time."""

    timestamp: datetime
    memory_resident: int
    phase: str


class ElOracloDeLeaks:
    """Detects memory leaks in ClickHouse by monitoring metrics over iterations."""

    def __init__(self):
        """Initialize the detector with ClickHouse connection."""
        self.snapshots: List[MemorySnapshot] = []
        self.logger = logging.getLogger(__name__)
        self.counter: int = 0

    def _get_memory_snapshot(
        self, phase: str, cluster: ClickHouseCluster
    ) -> Optional[MemorySnapshot]:
        """Capture current memory metrics from ClickHouse."""
        try:
            next_node: ClickHouseInstance = cluster.instances["node0"]
            client = Client(
                host=next_node.ip_address, port=9000, command=cluster.client_bin_path
            )
            metrics_str = client.query(
                """
                SELECT metric, value
                FROM system.asynchronous_metrics
                WHERE metric = 'MemoryResident';
                """
            )
            if not isinstance(metrics_str, str) or metrics_str == "":
                self.logger.warning(
                    f"No tables found to fetch on node {next_node.name}"
                )
                return None

            fetched_metrics: list[tuple[str, ...]] = [
                tuple(line.split("\t")) for line in metrics_str.split("\n") if line
            ]
            metrics = {row[0]: int(row[1]) for row in fetched_metrics}
            return MemorySnapshot(
                timestamp=datetime.now(),
                memory_resident=metrics.get("MemoryResident", 0),
                phase=phase,
            )
        except Exception as ex:
            self.logger.warning(
                f"Error occurred while fetching metrics from the server: {ex}"
            )
            return None

    def reset_and_capture_baseline(self, cluster: ClickHouseCluster):
        """Capture baseline memory before starting tests."""
        self.snapshots.clear()
        baseline = self._get_memory_snapshot("baseline", cluster)
        if baseline is None:
            self.logger.error(f"Could not get baseline capture")
            return
        self.snapshots.append(baseline)
        self._print_snapshot(baseline)

    def _print_snapshot(self, snapshot: MemorySnapshot):
        """Pretty print a memory snapshot."""
        self.logger.info(f"Iteration ({snapshot.phase}):")
        self.logger.info(
            f"MemoryResident: {snapshot.memory_resident / 1024 / 1024:.2f} MB"
        )

    def calculate_growth(self, snapshot: MemorySnapshot) -> Dict[str, float]:
        """Calculate memory growth percentage from baseline."""
        if len(self.snapshots) == 0:
            return {}

        prev_resident = self.snapshots[-1].memory_resident
        resident_growth = (
            (snapshot.memory_resident - prev_resident) / prev_resident * 100
            if prev_resident != 0
            else 0.0
        )
        return {
            "resident_growth": resident_growth,
            "resident_mb_delta": (snapshot.memory_resident - prev_resident)
            / 1024
            / 1024,
        }

    def _detect_leak(
        self, cluster: ClickHouseCluster, leak_threshold_percent: float = 10.0
    ):
        """
        Run leak detection test.

        Args:
            leak_threshold_percent: Alert if growth exceeds this percentage
        """
        # Check baseline
        if len(self.snapshots) == 0:
            self.logger.error(f"No previous captures exist")
            return
        leak_detected = False
        try:
            snapshot = self._get_memory_snapshot("Generator iteration", cluster)
            if snapshot is None:
                return
            growth = self.calculate_growth(snapshot)
            self.snapshots.append(snapshot)
            self._print_snapshot(snapshot)

            # Print progress
            self.logger.info(
                f"RSS: {growth['resident_growth']:+6.2f}% "
                f"({growth['resident_mb_delta']:+7.2f} MB)"
            )

            leak_detected = growth["resident_growth"] > leak_threshold_percent
        except Exception as ex:
            self.logger.warning(
                f"Error occurred while fetching metrics from the server: {ex}"
            )

        if leak_detected:
            message: str = (
                f"⚠️ WARNING: Potential leak detected: MemoryResident grew by {growth['resident_growth']:.2f}%"
            )
            self.logger.warning(message)
            raise ValueError(message)

    def _pause_or_resume_clickhouse_background_tasks(
        self, cmd: str, backupn: int, cluster: ClickHouseCluster, timeout: int = 30
    ) -> bool:
        """Stop all background operations that could affect memory."""
        try:
            active_merges = 1
            active_mutations = 1

            next_node: ClickHouseInstance = cluster.instances["node0"]
            client = Client(
                host=next_node.ip_address, port=9000, command=cluster.client_bin_path
            )
            for c in ["MERGES", "MUTATIONS", "FETCHES", "MOVES", "TTL MERGES"]:
                client.query(f"SYSTEM {cmd} {c};")
            if cmd == "START":
                active_merges = 0
                active_mutations = 0

                # Restore everything
                client.query(
                    f"RESTORE ALL FROM File('/var/lib/clickhouse/user_files/leaks{backupn}');"
                )
            else:
                start_time = time.time()
                while (
                    active_merges != 0 or active_mutations != 0
                ) and time.time() - start_time < timeout:
                    # Check for active merges and mutations
                    result1 = client.query("SELECT count() FROM system.merges;")
                    result2 = client.query(
                        "SELECT count() FROM system.mutations WHERE is_done = 0;"
                    )
                    if (
                        not isinstance(result1, str)
                        or result1 == ""
                        or not isinstance(result2, str)
                        or result2 == ""
                    ):
                        self.logger.warning(
                            f"Could not fetch number of merges or mutations"
                        )
                        return False

                    active_merges = int(result1)
                    active_mutations = int(result2)
                for c in ["MARK CACHE", "UNCOMPRESSED CACHE"]:
                    client.query(f"SYSTEM CLEAR {c};")

                # Backup then drop all databases
                client.query(
                    f"BACKUP ALL TO File('/var/lib/clickhouse/user_files/leaks{backupn}');"
                )
                dbs_str = client.query(
                    "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA');"
                )
                if not isinstance(dbs_str, str) or dbs_str == "":
                    self.logger.warning(f"Could not fetch databases")
                    return False
                fetched_dbs: list[str] = [line for line in dbs_str.split("\n") if line]
                for db_name in fetched_dbs:
                    client.query(f"DROP DATABASE IF EXISTS {db_name};")

            return active_merges == 0 and active_mutations == 0
        except Exception as ex:
            self.logger.warning(
                f"Error occurred while setting {cmd} operations in the server: {ex}"
            )
        return False

    def run_next_leak_detection(
        self,
        cluster: ClickHouseCluster,
        generator: CommandRequest,
        leak_threshold_percent: float = 10.0,
    ):
        backupn = self.counter

        self.counter += 1
        generator.pause_process()
        time.sleep(1)  # wait for process to stop?
        if self._pause_or_resume_clickhouse_background_tasks("STOP", backupn, cluster):
            self._detect_leak(cluster, leak_threshold_percent)
        self._pause_or_resume_clickhouse_background_tasks("START", backupn, cluster)
        generator.resume_process()
