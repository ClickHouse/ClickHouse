import subprocess
import pytest
import logging
from helpers.test_tools import TSV
from helpers.network import _NetworkManager

@pytest.fixture(autouse=True, scope="session")
def cleanup_environment():
    _NetworkManager.clean_all_user_iptables_rules()
    try:
        result = subprocess.run(['docker', 'container', 'list', '-a', '|', 'wc', '-l'])
        if result.returncode != 0:
            logging.error(f"docker ps returned error:{str(result.stderr)}")
        else:
            if int(result.stdout) > 1:
                if env["PYTEST_CLEANUP_CONTAINERS"] != 1:
                    logging.warning(f"Docker containters({result.stdout}) are running before tests run. They can be left from previous pytest run and cause test failures.\n"\
                                    "You can set env PYTEST_CLEANUP_CONTAINERS=1 or use runner with --cleanup-containers argument to enable automatic containers cleanup.")
                else:
                    logging.debug("Trying to kill unstopped containers...")
                    subprocess.run(['docker', 'kill', f'`docker container list -a`'])
                    subprocess.run(['docker', 'rm', f'`docker container list -a`'])
                    logging.debug("Unstopped containers killed")
                    r = subprocess.run(['docker-compose', 'ps', '--services', '--all'])
                    logging.debug(f"Docker ps before start:{r.stdout}")
            else:
                logging.debug(f"No running containers")
    except Exception as e:
        logging.error(f"cleanup_environment:{str(e)}")
        pass

    yield