import importlib
import logging
import os
import time


# Starts simple HTTP servers written in Python.
# Parameters:
# `script_dir` contains a path to the directory containing server scripts.
# `mocks` is a list of tuples (server_script, container_name, port), where
#     `server_script` is a name of a python file inside `script_dir`,
#     `container_name` is usually "resolver" (see docker/test/integration/resolver)
def start_mock_servers(cluster, script_dir, mocks, timeout=100):
    server_names = [mock[0] for mock in mocks]
    server_names_with_desc = (
        f"{'server' if len(server_names) == 1 else 'servers'} {','.join(server_names)}"
    )
    logging.info(f"Starting mock {server_names_with_desc}")

    start_time = time.time()
    mocks_to_check = {}

    for server_name, container, port in mocks:
        filepath = os.path.join(script_dir, server_name)
        container_id = cluster.get_container_id(container)
        mocks_to_check[server_name] = (container_id, port)

        cluster.copy_file_to_container(
            container_id,
            filepath,
            server_name,
        )

        logs_dir = (
            "/var/log/resolver"
            if container == "resolver"
            else "/var/log/clickhouse-server"
        )
        log_file = os.path.join(logs_dir, os.path.splitext(server_name)[0] + ".log")
        err_log_file = os.path.join(
            logs_dir, os.path.splitext(server_name)[0] + ".err.log"
        )

        cluster.exec_in_container(
            container_id,
            [
                "bash",
                "-c",
                f"python3 {server_name} {port} >{log_file} 2>{err_log_file}",
            ],
            detach=True,
        )

    # Wait for the server to start.
    attempt = 1
    while mocks_to_check:
        for server_name in list(mocks_to_check.keys()):
            container_id, port = mocks_to_check[server_name]

            ping_response = cluster.exec_in_container(
                container_id,
                ["curl", "-s", f"http://localhost:{port}/"],
                nothrow=True,
            )

            if ping_response == "OK":
                logging.debug(
                    f"{server_name} answered {ping_response} on attempt {attempt}"
                )
                del mocks_to_check[server_name]
            elif time.time() - start_time > timeout:
                assert (
                    ping_response == "OK"
                ), 'Expected "OK", but got "{}" from {}'.format(
                    ping_response, server_name
                )

        if mocks_to_check:
            time.sleep(1)
            attempt += 1

    logging.info(f"Mock {server_names_with_desc} started")


# The same as start_mock_servers, but
# import servers from central directory tests/integration/helpers
# and return the control instance
def start_s3_mock(cluster, mock_name, port, timeout=100):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    registered_servers = [
        mock
        for mock in os.listdir(script_dir)
        if os.path.isfile(os.path.join(script_dir, mock))
    ]

    file_name = mock_name + ".py"
    if file_name not in registered_servers:
        raise KeyError(
            f"Can't run s3 mock `{mock_name}`. No file `{file_name}` in directory `{script_dir}`"
        )

    start_mock_servers(cluster, script_dir, [(file_name, "resolver", port)], timeout)

    fmt = importlib.import_module("." + mock_name, "helpers.s3_mocks")
    control = getattr(fmt, "MockControl")(cluster, "resolver", port)

    return control
