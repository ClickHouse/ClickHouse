import atexit
import logging
import os
import random
import signal
import sys
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
from integration.helpers.cluster import is_port_free
from utils.httpserver import DolorHTTPServer
from catalogs.datalakes import SparkHandler


def get_unique_free_ports(total: int) -> list[int]:
    ports = []
    for port in range(30000, 55000):
        if is_port_free(port) and port not in ports:
            ports.append(port)

        if len(ports) == total:
            return ports

    raise Exception(f"Can't collect {total} ports. Collected: {len(ports)}")


def create_spark_http_server(
    cluster, with_unity: bool, test_env_variables
) -> DolorHTTPServer:

    def datalakehandler(path, data, headers, attachment):
        res = False
        state = random.getstate()
        saved_exception = None

        try:
            random.seed(data["seed"])
            if path == "/sparkdatabase":
                res = attachment.create_lake_database(cluster, data)
            elif path == "/sparktable":
                res = attachment.create_lake_table(cluster, data)
            elif path == "/sparkupdate":
                res = attachment.update_or_check_table(cluster, data)
        except Exception as e:
            saved_exception = e
        random.setstate(state)
        if saved_exception is not None:
            raise saved_exception
        return res

    catalog_server = DolorHTTPServer(
        port=get_unique_free_ports(1)[0],
        attachment=SparkHandler(cluster, with_unity, test_env_variables),
        handler_kwargs={
            "callback": datalakehandler,
        },
    )
    catalog_server.start()
    return catalog_server


def close_spark_http_server(catalog_server):
    if catalog_server.is_alive():
        catalog_server.stop()
    # Clean Spark sessions
    catalog_server.attachment.close_sessions()


if __name__ == "__main__":
    # Start the whole Spark integration with the HTTP server, so issues can be reproduced outside a fuzzer run
    from types import SimpleNamespace

    # FIXME these should be parameters for the script
    # So far I call `tests/docker_scripts/setup_minio.sh stateless` to start MinIO
    cluster_settings = {
        "minio_ip": "127.0.0.1",
        "minio_port": 11111,
        "minio_s3_port": 11111,
        "minio_bucket": "test",
        "minio_access_key": "clickhouse",
        "minio_secret_key": "clickhouse",
        "azurite_ip": "127.0.0.1",
        "azurite_port": 10000,
        "azurite_account": "devstoreaccount1",
        "azurite_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "azure_container_name": "cont",
        "instances_dir": "/var/lib/clickhouse/user_files",
        "client_bin_path": sys.argv[1],
        "with_kafka": False,
        "iceberg_rest_catalog_port": 8181,
        "glue_catalog_port": 3000,
        "hms_catalog_port": 9083,
        "get_instance_ip": lambda _: "127.0.0.1",
    }
    cluster = SimpleNamespace(**cluster_settings)
    os.makedirs(cluster.instances_dir, exist_ok=True)
    catalog_server = create_spark_http_server(cluster, False, {})

    def http_cleanup():
        close_spark_http_server(catalog_server)

    def my_signal_handler(sig, frame):
        http_cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, my_signal_handler)
    atexit.register(http_cleanup)

    all_running = True
    while all_running:
        start = time.time()
        finish = start + 1

        while all_running and start < finish:
            interval = 1
            if not catalog_server.is_alive():
                logger = logging.getLogger(__name__)
                logger.info("HTTP server is not running")
                all_running = False
            time.sleep(interval)
            start += interval

        if not all_running:
            break
