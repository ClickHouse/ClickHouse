#!/usr/bin/env python3

import time
import logging
import os

import boto3
from github import Github

from env_helper import REPORTS_PATH, REPO_COPY, TEMP_PATH
from stopwatch import Stopwatch
from upload_result_helper import upload_results
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from compress_files import compress_fast
from commit_status_helper import post_commit_status
from docker_pull_helper import get_image_with_version
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from tee_popen import TeePopen
from ssh import SSHKey
from build_download_helper import get_build_name_for_check, get_build_urls

JEPSEN_GROUP_NAME = 'jepsen_group'
DESIRED_INSTANCE_COUNT = 3
IMAGE_NAME = 'clickhouse/keeper-jepsen-test'
CHECK_NAME = 'ClickHouse Keeper Jepsen (actions)'


SUCCESSFUL_TESTS_ANCHOR = "# Successful tests"
INTERMINATE_TESTS_ANCHOR = "# Indeterminate tests"
CRASHED_TESTS_ANCHOR = "# Crashed tests"
FAILED_TESTS_ANCHOR = "# Failed tests"

def _parse_jepsen_output(path):
    test_results = []
    current_type = ''
    with open(path, 'r') as f:
        for line in f:
            if SUCCESSFUL_TESTS_ANCHOR in line:
                current_type = 'OK'
            elif INTERMINATE_TESTS_ANCHOR in line or CRASHED_TESTS_ANCHOR in line:
                current_type = 'ERROR'
            elif FAILED_TESTS_ANCHOR in line:
                current_type = 'FAIL'

            if (line.startswith('store/clickhouse-keeper') or line.startswith('clickhouse-keeper')) and current_type:
                test_results.append((line.strip(), current_type))

    return test_results

def get_autoscaling_group_instances_ids(asg_client, group_name):
    group_description = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name])
    our_group = group_description['AutoScalingGroups'][0]
    instance_ids = []
    for instance in our_group['Instances']:
        if instance['LifecycleState'] == 'InService' and instance['HealthStatus'] == 'Healthy':
            instance_ids.append(instance['InstanceId'])

    return instance_ids

def get_instances_addresses(ec2_client, instance_ids):
    ec2_response = ec2_client.describe_instances(InstanceIds = instance_ids)
    instance_ips = []
    for instances in ec2_response['Reservations']:
        for ip in instances['Instances']:
            instance_ips.append(ip['PrivateIpAddress'])
    return instance_ips


def prepare_autoscaling_group_and_get_hostnames():
    asg_client = boto3.client('autoscaling')
    asg_client.set_desired_capacity(AutoScalingGroupName=JEPSEN_GROUP_NAME, DesiredCapacity=DESIRED_INSTANCE_COUNT)

    instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
    counter = 0
    while len(instances) < DESIRED_INSTANCE_COUNT:
        time.sleep(5)
        instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
        counter += 1
        if counter > 30:
            raise Exception("Cannot wait autoscaling group")

    ec2_client = boto3.client('ec2')
    return get_instances_addresses(ec2_client, instances)


def clear_autoscaling_group():
    asg_client = boto3.client('autoscaling')
    asg_client.set_desired_capacity(AutoScalingGroupName=JEPSEN_GROUP_NAME, DesiredCapacity=0)
    instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
    counter = 0
    while len(instances) > 0:
        time.sleep(5)
        instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
        counter += 1
        if counter > 30:
            raise Exception("Cannot wait autoscaling group")


def save_nodes_to_file(instances, temp_path):
    nodes_path = os.path.join(temp_path, "nodes.txt")
    with open(nodes_path, 'w') as f:
        f.write("\n".join(instances))
        f.flush()
    return nodes_path

def get_run_command(ssh_auth_sock, ssh_sock_dir, pr_info, nodes_path, repo_path, build_url, result_path, docker_image):
    return f"docker run --network=host -v '{ssh_sock_dir}:{ssh_sock_dir}' -e SSH_AUTH_SOCK={ssh_auth_sock} " \
           f"-e PR_TO_TEST={pr_info.number} -e SHA_TO_TEST={pr_info.sha} -v '{nodes_path}:/nodes.txt' -v {result_path}:/test_output " \
           f"-e 'CLICKHOUSE_PACKAGE={build_url}' -v '{repo_path}:/ch' -e 'CLICKHOUSE_REPO_PATH=/ch' -e NODES_USERNAME=ubuntu {docker_image}"

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    pr_info = PRInfo()

    gh = Github(get_best_robot_token())

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)

    result_path = os.path.join(TEMP_PATH, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    instances = prepare_autoscaling_group_and_get_hostnames()
    nodes_path = save_nodes_to_file(instances, TEMP_PATH)

    docker_image = get_image_with_version(REPORTS_PATH, IMAGE_NAME)

    build_name = get_build_name_for_check(CHECK_NAME)
    urls = get_build_urls(build_name, REPORTS_PATH)
    if not urls:
        raise Exception("No build URLs found")

    for url in urls:
        if url.endswith('/clickhouse'):
            build_url = url
            break
    else:
        raise Exception("Cannot binary clickhouse among build results")

    with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
        ssh_auth_sock = os.environ['SSH_AUTH_SOCK']
        auth_sock_dir = os.path.dirname(ssh_auth_sock)
        cmd = get_run_command(ssh_auth_sock, auth_sock_dir, pr_info, nodes_path, REPO_COPY, build_url, result_path, docker_image)
        logging.info("Going to run jepsen: %s", cmd)

        run_log_path = os.path.join(TEMP_PATH, "runlog.log")

        with TeePopen(cmd, run_log_path) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed")

    status = 'success'
    description = 'No invalid analysis found ヽ(‘ー`)ノ'
    jepsen_log_path = os.path.join(result_path, 'jepsen_run_all_tests.log')
    additional_data = []
    try:
        test_result = _parse_jepsen_output(jepsen_log_path)
        if any(r[1] == 'FAIL' for r in test_result):
            status = 'failure'
            description = 'Found invalid analysis (ﾉಥ益ಥ）ﾉ ┻━┻'
        compress_fast(os.path.join(result_path, 'store'), 'jepsen_store.tar.gz')
        additional_data.append(os.path.join(TEMP_PATH, 'jepsen_store.tar.gz'))
    except:
        status = 'failure'
        description = 'No Jepsen output log'
        test_result = [('No Jepsen output log', 'FAIL')]

    s3_helper = S3Helper('https://s3.amazonaws.com')
    report_url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_result, [run_log_path] + additional_data, CHECK_NAME)

    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, CHECK_NAME, description, status, report_url)

    ch_helper = ClickHouseHelper()
    prepared_events = prepare_tests_results_for_clickhouse(pr_info, test_result, status, stopwatch.duration_seconds, stopwatch.start_time_str, report_url, CHECK_NAME)
    ch_helper.insert_events_into(db="gh-data", table="checks", events=prepared_events)
    clear_autoscaling_group()
