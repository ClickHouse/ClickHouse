import pytest
import os
import pwd
import subprocess

import subprocess


## quite a silly wrapper which just do docker-compose up / run clickhouse-test / docker-compose down
## instead of that - it would be better just to 'teach' clickhouse-test to run docker-compose up / down commands for group of tests 
## and define needed evn vars 

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_NAME = pwd.getpwuid(os.getuid()).pw_name + os.path.basename(SCRIPT_DIR)
# '--verbose', 
COMPOSER_ARGUMENTS = [ 'docker-compose', '-f', SCRIPT_DIR + '/docker-compose.yml',  '--project-directory', SCRIPT_DIR, '--project-name', PROJECT_NAME ]

def subprocess_check_call(args):
    # Uncomment for debugging
    print('run:', ' ' . join(args))
    subprocess.check_call(args)

def call_composer(args):
    subprocess_check_call(COMPOSER_ARGUMENTS + args)

@pytest.fixture(scope="module")
def composer_running():
    try:
        call_composer(['up', '-d'])
        yield
    except Exception as ex:
        print(ex)
        raise ex
    finally:
        call_composer(['down'])

def test_run_clickhouse_test(composer_running):
    os.environ["DOCKER_COMPOSE"] = ' '.join(COMPOSER_ARGUMENTS);
    subprocess.check_call( 'cd ' + SCRIPT_DIR + '; ../../clickhouse-test -c "$DOCKER_COMPOSE exec clickhouse1 clickhouse client"', shell=True )

#    call_composer(['exec','clickhouse1','clickhouse client', '--query', '"SELECT version()"'])