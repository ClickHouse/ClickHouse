import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).parents[1]
    / "jobs"
    / "scripts"
    / "docs"
    / "autogenerate"
    / "async_metrics.py"
)
SPEC = importlib.util.spec_from_file_location("async_metrics", MODULE_PATH)
async_metrics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(async_metrics)


class TestAsyncMetricsDocs(unittest.TestCase):
    def test_normalize_metric_name(self):
        cases = [
            ("AsyncLogging*metric_first*QueueSize", "AsyncLogging*channel*QueueSize"),
            ("BlockReadBytes_*name*", "BlockReadBytes_*device*"),
            ("CPUFrequencyMHz_*core_id*", "CPUFrequencyMHz_*core*"),
            ("DiskAvailable_*name*", "DiskAvailable_*disk*"),
            ("EDAC*i*_Correctable", "EDAC*controller*_Correctable"),
            (
                "HTTPConnectionPool*group_name*TCPRcvBufTotalBytes",
                "HTTPConnectionPool*group*TCPRcvBufTotalBytes",
            ),
            (
                "NetworkReceiveBytes_*interface_name*",
                "NetworkReceiveBytes_*interface*",
            ),
            ("NetworkTCPSockets_*description*", "NetworkTCPSockets_*state*"),
            ("OSUserTime*cpu_suffix*", "OSUserTime*cpu*"),
            ("PSI_*type*_*stall_type*", "PSI_*resource*_*stall*"),
            ("Temperature*i*", "Temperature*zone*"),
            ("Temperature_*hwmon_name*", "Temperature_*hwmon*"),
            (
                "Temperature_*hwmon_name*_*sensor_name*",
                "Temperature_*hwmon*_*sensor*",
            ),
        ]
        for parsed_name, documented_name in cases:
            with self.subTest(parsed_name=parsed_name):
                self.assertEqual(
                    async_metrics.normalize_metric_name(parsed_name), documented_name
                )

    def test_unknown_dynamic_metric_family_is_rejected(self):
        with self.assertRaisesRegex(
            ValueError, "no stable documentation placeholder"
        ):
            async_metrics.normalize_metric_name("NewMetric_*implementation_detail*")


if __name__ == "__main__":
    unittest.main()
