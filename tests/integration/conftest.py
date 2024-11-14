#!/usr/bin/env python3

import logging
import os

import pytest  # pylint:disable=import-error; for style check
from helpers.cluster import run_and_check
from helpers.network import _NetworkManager

# This is a workaround for a problem with logging in pytest [1].
#
#   [1]: https://github.com/pytest-dev/pytest/issues/5502
logging.raiseExceptions = False


@pytest.fixture(scope="session", autouse=True)
def pdb_history(request):
    """
    Fixture loads and saves pdb history to file, so it can be preserved between runs
    """
    if request.config.getoption("--pdb"):
        import readline  # pylint:disable=import-outside-toplevel
        import pdb  # pylint:disable=import-outside-toplevel

        def save_history():
            readline.write_history_file(".pdb_history")

        def load_history():
            try:
                readline.read_history_file(".pdb_history")
            except FileNotFoundError:
                pass

        load_history()
        pdb.Pdb.use_rawinput = True

        yield

        save_history()
    else:
        yield


@pytest.fixture(autouse=True, scope="session")
def tune_local_port_range():
    # Lots of services uses non privileged ports:
    # - hdfs -- 50020/50070/...
    # - minio
    #
    # NOTE: 5K is not enough, and sometimes leads to EADDRNOTAVAIL error.
    # NOTE: it is not inherited, so you may need to specify this in docker_compose_$SERVICE.yml
    try:
        run_and_check(["sysctl net.ipv4.ip_local_port_range='55000 65535'"], shell=True)
    except Exception as ex:
        logging.warning(
            "Failed to run sysctl, tests may fail with EADDRINUSE %s", str(ex)
        )


@pytest.fixture(autouse=True, scope="session")
def cleanup_environment():
    try:
        if int(os.environ.get("PYTEST_CLEANUP_CONTAINERS", 0)) == 1:
            logging.debug("Cleaning all iptables rules")
            _NetworkManager.clean_all_user_iptables_rules()
        result = run_and_check(["docker ps | wc -l"], shell=True)
        if int(result) > 1:
            if int(os.environ.get("PYTEST_CLEANUP_CONTAINERS", 0)) != 1:
                logging.warning(
                    "Docker containters(%s) are running before tests run. "
                    "They can be left from previous pytest run and cause test failures.\n"
                    "You can set env PYTEST_CLEANUP_CONTAINERS=1 or use runner with "
                    "--cleanup-containers argument to enable automatic containers cleanup.",
                    int(result),
                )
            else:
                logging.debug("Trying to kill unstopped containers...")
                run_and_check(
                    ["docker kill $(docker container list  --all  --quiet)"],
                    shell=True,
                    nothrow=True,
                )
                run_and_check(
                    ["docker rm $docker container list  --all  --quiet)"],
                    shell=True,
                    nothrow=True,
                )
                logging.debug("Unstopped containers killed")
                r = run_and_check(["docker-compose", "ps", "--services", "--all"])
                logging.debug("Docker ps before start:%s", r.stdout)
        else:
            logging.debug("No running containers")

        logging.debug("Pruning Docker networks")
        run_and_check(
            ["docker network prune --force"],
            shell=True,
            nothrow=True,
        )
    except Exception as e:
        logging.exception("cleanup_environment:%s", e)

    yield


def pytest_addoption(parser):
    parser.addoption(
        "--run-id",
        default="",
        help="run-id is used as postfix in _instances_{} directory",
    )


def pytest_configure(config):
    os.environ["INTEGRATION_TESTS_RUN_ID"] = config.option.run_id
