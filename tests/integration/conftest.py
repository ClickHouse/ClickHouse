import subprocess
import pytest
import logging
from helpers.test_tools import TSV
from helpers.network import _NetworkManager

@pytest.fixture(autouse=True, scope="session")
def cleanup_environment():
    _NetworkManager.clean_all_user_iptables_rules()
    try:
        result = subprocess.run(['docker', 'container', 'list', '-a'])
        if result.returncode==0 and int(result.stdout) > 1:
            logging.debug("Trying to kill unstopped containers...")
            subprocess.run(['docker', 'kill', f'`docker container list -a`'])
            subprocess.run(['docker', 'rm', f'`docker container list -a`'])
            logging.debug("Unstopped containers killed")
            r = subprocess.run(['docker-compose', 'ps', '--services', '--all'])
            logging.debug(f"Docker ps before start:{r.stdout}")
        else:
            logging.debug(f"No running containers")
    except:
        pass

    yield