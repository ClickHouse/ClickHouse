import os
import time

ALL_HTTP_METHODS = {"POST", "PUT", "GET", "HEAD", "CONNECT"}


def check_proxy_logs(
    cluster, proxy_instances, protocol, bucket, requested_http_methods
):
    for i in range(10):
        # Check with retry that all possible interactions with Minio are present
        for http_method in ALL_HTTP_METHODS:
            for proxy_instance in proxy_instances:
                logs = cluster.get_container_logs(proxy_instance)
                if (
                    logs.find(
                        http_method + f" {protocol}://minio1:9001/root/data/{bucket}"
                    )
                    >= 0
                ):
                    if http_method not in requested_http_methods:
                        assert (
                            False
                        ), f"Found http method {http_method} for bucket {bucket} that should not be found in {proxy_instance} logs"
                    break
            else:
                if http_method in requested_http_methods:
                    assert (
                        False
                    ), f"{http_method} method not found in logs of {proxy_instance} for bucket {bucket}"

        time.sleep(1)


def wait_resolver(cluster):
    for i in range(10):
        response = cluster.exec_in_container(
            cluster.get_container_id("resolver"),
            [
                "curl",
                "-s",
                f"http://resolver:8080/hostname",
            ],
            nothrow=True,
        )
        if response == "proxy1" or response == "proxy2":
            return
        time.sleep(i)

    assert False, "Resolver is not up"


# Runs simple proxy resolver in python env container.
def run_resolver(cluster, current_dir):
    container_id = cluster.get_container_id("resolver")
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "proxy-resolver", "resolver.py"),
        "resolver.py",
    )
    cluster.exec_in_container(container_id, ["python", "resolver.py"], detach=True)

    wait_resolver(cluster)


def build_s3_endpoint(protocol, bucket):
    return f"{protocol}://minio1:9001/root/data/{bucket}/test.csv"


def perform_simple_queries(node, minio_endpoint):
    node.query(
        f"""
            INSERT INTO FUNCTION
            s3('{minio_endpoint}', 'minio', 'minio123', 'CSV', 'key String, value String')
            VALUES ('color','red'),('size','10')
            """
    )

    assert (
        node.query(
            f"SELECT * FROM s3('{minio_endpoint}', 'minio', 'minio123', 'CSV') FORMAT Values"
        )
        == "('color','red'),('size','10')"
    )

    assert (
        node.query(
            f"SELECT * FROM s3('{minio_endpoint}', 'minio', 'minio123', 'CSV') FORMAT Values"
        )
        == "('color','red'),('size','10')"
    )


def simple_test(cluster, proxies, protocol, bucket):
    minio_endpoint = build_s3_endpoint(protocol, bucket)
    node = cluster.instances[bucket]

    perform_simple_queries(node, minio_endpoint)

    check_proxy_logs(cluster, proxies, protocol, bucket, ["PUT", "GET", "HEAD"])


def simple_storage_test(cluster, node, proxies, policy):
    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(
            policy
        )
    )
    node.query("INSERT INTO s3_test VALUES (0,'data'),(1,'data')")
    assert (
        node.query("SELECT * FROM s3_test order by id FORMAT Values")
        == "(0,'data'),(1,'data')"
    )

    node.query("DROP TABLE IF EXISTS s3_test SYNC")

    # not checking for POST because it is in a different format
    check_proxy_logs(cluster, proxies, "http", policy, ["PUT", "GET"])


def simple_test_assert_no_proxy(cluster, proxies, protocol, bucket):
    minio_endpoint = build_s3_endpoint(protocol, bucket)
    node = cluster.instances[bucket]
    perform_simple_queries(node, minio_endpoint)

    # No HTTP method should be found in proxy logs if no proxy is active
    empty_method_list = []
    check_proxy_logs(cluster, proxies, protocol, bucket, empty_method_list)
