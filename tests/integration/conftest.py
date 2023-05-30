#!/usr/bin/env python3

import pytest
import logging
import os
from helpers.cluster import run_and_check
from helpers.test_tools import TSV
from helpers.network import _NetworkManager


# This is a workaround for a problem with logging in pytest [1].
#
#   [1]: https://github.com/pytest-dev/pytest/issues/5502
logging.raiseExceptions = False


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
            logging.debug(f"Cleaning all iptables rules")
            _NetworkManager.clean_all_user_iptables_rules()
        result = run_and_check(["docker ps | wc -l"], shell=True)
        if int(result) > 1:
            if int(os.environ.get("PYTEST_CLEANUP_CONTAINERS", 0)) != 1:
                logging.warning(
                    f"Docker containters({int(result)}) are running before tests run. They can be left from previous pytest run and cause test failures.\n"
                    "You can set env PYTEST_CLEANUP_CONTAINERS=1 or use runner with --cleanup-containers argument to enable automatic containers cleanup."
                )
            else:
                logging.debug("Trying to kill unstopped containers...")
                run_and_check(
                    [f"docker kill $(docker container list  --all  --quiet)"],
                    shell=True,
                    nothrow=True,
                )
                run_and_check(
                    [f"docker rm $docker container list  --all  --quiet)"],
                    shell=True,
                    nothrow=True,
                )
                logging.debug("Unstopped containers killed")
                r = run_and_check(["docker-compose", "ps", "--services", "--all"])
                logging.debug(f"Docker ps before start:{r.stdout}")
        else:
            logging.debug(f"No running containers")

        logging.debug("Pruning Docker networks")
        run_and_check(
            ["docker network prune --force"],
            shell=True,
            nothrow=True,
        )
    except Exception as e:
        logging.exception(f"cleanup_environment:{str(e)}")
        pass

    yield


def pytest_addoption(parser):
    parser.addoption(
        "--run-id",
        default="",
        help="run-id is used as postfix in _instances_{} directory",
    )


def pytest_configure(config):
    os.environ["INTEGRATION_TESTS_RUN_ID"] = config.option.run_id
