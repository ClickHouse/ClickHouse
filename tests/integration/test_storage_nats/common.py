import asyncio
import logging
import shlex
import time

from helpers.cluster import check_nats_is_available


def wait_nats_to_start(cluster, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if asyncio.run(check_nats_is_available(cluster)):
                logging.debug("NATS is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to NATS " + str(ex))
            time.sleep(0.5)

    assert False, "NATS is unavailable"


def count_in_recent_log(instance, pattern, look_behind_lines=10000):
    result = instance.exec_in_container(
        [
            "bash",
            "-c",
            "tail -n{} /var/log/clickhouse-server/clickhouse-server.log | grep -Ec {} || true".format(
                look_behind_lines, shlex.quote(pattern)
            ),
        ]
    )
    return int(result.strip() or 0)
