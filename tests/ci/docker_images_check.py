#!/usr/bin/env python3
import subprocess
import logging
import json
import os
import time
import shutil
from github import Github
from report import create_test_html_report
from s3_helper import S3Helper
from pr_info import PRInfo
from get_robot_token import get_best_robot_token, get_parameter_from_ssm

NAME = "Push to Dockerhub (actions)"

def get_changed_docker_images(pr_info, repo_path, image_file_path):
    images_dict = {}
    path_to_images_file = os.path.join(repo_path, image_file_path)
    if os.path.exists(path_to_images_file):
        with open(path_to_images_file, 'r') as dict_file:
            images_dict = json.load(dict_file)
    else:
        logging.info("Image file %s doesnt exists in repo %s", image_file_path, repo_path)

    dockerhub_repo_name = 'yandex'
    if not images_dict:
        return [], dockerhub_repo_name

    files_changed = pr_info.changed_files

    logging.info("Changed files for PR %s @ %s: %s", pr_info.number, pr_info.sha, str(files_changed))

    changed_images = []

    for dockerfile_dir, image_description in images_dict.items():
        if image_description['name'].startswith('clickhouse/'):
            dockerhub_repo_name = 'clickhouse'

        for f in files_changed:
            if f.startswith(dockerfile_dir):
                logging.info(
                    "Found changed file '%s' which affects docker image '%s' with path '%s'",
                    f, image_description['name'], dockerfile_dir)
                changed_images.append(dockerfile_dir)
                break

    # The order is important: dependents should go later than bases, so that
    # they are built with updated base versions.
    index = 0
    while index < len(changed_images):
        image = changed_images[index]
        for dependent in images_dict[image]['dependent']:
            logging.info(
                "Marking docker image '%s' as changed because it depends on changed docker image '%s'",
                dependent, image)
            changed_images.append(dependent)
        index += 1
        if index > 100:
            # Sanity check to prevent infinite loop.
            raise RuntimeError("Too many changed docker images, this is a bug." + str(changed_images))

    # If a dependent image was already in the list because its own files
    # changed, but then it was added as a dependent of a changed base, we
    # must remove the earlier entry so that it doesn't go earlier than its
    # base. This way, the dependent will be rebuilt later than the base, and
    # will correctly use the updated version of the base.
    seen = set()
    no_dups_reversed = []
    for x in reversed(changed_images):
        if x not in seen:
            seen.add(x)
            no_dups_reversed.append(x)

    result = [(x, images_dict[x]['name']) for x in reversed(no_dups_reversed)]
    logging.info("Changed docker images for PR %s @ %s: '%s'", pr_info.number, pr_info.sha, result)
    return result, dockerhub_repo_name

def build_and_push_one_image(path_to_dockerfile_folder, image_name, version_string):
    logging.info("Building docker image %s with version %s from path %s", image_name, version_string, path_to_dockerfile_folder)
    build_log = None
    push_log = None
    with open('build_log_' + str(image_name).replace('/', '_') + "_" + version_string, 'w') as pl:
        cmd = "docker build --network=host -t {im}:{ver} {path}".format(im=image_name, ver=version_string, path=path_to_dockerfile_folder)
        retcode = subprocess.Popen(cmd, shell=True, stderr=pl, stdout=pl).wait()
        build_log = str(pl.name)
        if retcode != 0:
            return False, build_log, None

    with open('tag_log_' + str(image_name).replace('/', '_') + "_" + version_string, 'w') as pl:
        cmd = "docker build --network=host -t {im} {path}".format(im=image_name, path=path_to_dockerfile_folder)
        retcode = subprocess.Popen(cmd, shell=True, stderr=pl, stdout=pl).wait()
        build_log = str(pl.name)
        if retcode != 0:
            return False, build_log, None

    logging.info("Pushing image %s to dockerhub", image_name)

    with open('push_log_' + str(image_name).replace('/', '_') + "_" + version_string, 'w') as pl:
        cmd = "docker push {im}:{ver}".format(im=image_name, ver=version_string)
        retcode = subprocess.Popen(cmd, shell=True, stderr=pl, stdout=pl).wait()
        push_log = str(pl.name)
        if retcode != 0:
            return False, build_log, push_log

    logging.info("Processing of %s successfully finished", image_name)
    return True, build_log, push_log

def process_single_image(versions, path_to_dockerfile_folder, image_name):
    logging.info("Image will be pushed with versions %s", ', '.join(versions))
    result = []
    for ver in versions:
        for i in range(5):
            success, build_log, push_log = build_and_push_one_image(path_to_dockerfile_folder, image_name, ver)
            if success:
                result.append((image_name + ":" + ver, build_log, push_log, 'OK'))
                break
            logging.info("Got error will retry %s time and sleep for %s seconds", i, i * 5)
            time.sleep(i * 5)
        else:
            result.append((image_name + ":" + ver, build_log, push_log, 'FAIL'))

    logging.info("Processing finished")
    return result


def process_test_results(s3_client, test_results, s3_path_prefix):
    overall_status = 'success'
    processed_test_results = []
    for image, build_log, push_log, status in test_results:
        if status != 'OK':
            overall_status = 'failure'
        url_part = ''
        if build_log is not None and os.path.exists(build_log):
            build_url = s3_client.upload_test_report_to_s3(
                build_log,
                s3_path_prefix + "/" + os.path.basename(build_log))
            url_part += '<a href="{}">build_log</a>'.format(build_url)
        if push_log is not None and os.path.exists(push_log):
            push_url = s3_client.upload_test_report_to_s3(
                push_log,
                s3_path_prefix + "/" + os.path.basename(push_log))
            if url_part:
                url_part += ', '
            url_part += '<a href="{}">push_log</a>'.format(push_url)
        if url_part:
            test_name = image + ' (' + url_part + ')'
        else:
            test_name = image
        processed_test_results.append((test_name, status))
    return overall_status, processed_test_results

def upload_results(s3_client, pr_number, commit_sha, test_results):
    s3_path_prefix = f"{pr_number}/{commit_sha}/" + NAME.lower().replace(' ', '_')

    branch_url = "https://github.com/ClickHouse/ClickHouse/commits/master"
    branch_name = "master"
    if pr_number != 0:
        branch_name = "PR #{}".format(pr_number)
        branch_url = "https://github.com/ClickHouse/ClickHouse/pull/" + str(pr_number)
    commit_url = f"https://github.com/ClickHouse/ClickHouse/commit/{commit_sha}"

    task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"

    html_report = create_test_html_report(NAME, test_results, "https://hub.docker.com/u/clickhouse", task_url, branch_url, branch_name, commit_url)
    with open('report.html', 'w') as f:
        f.write(html_report)

    url = s3_client.upload_test_report_to_s3('report.html', s3_path_prefix + ".html")
    logging.info("Search result in url %s", url)
    return url

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo_path = os.getenv("GITHUB_WORKSPACE", os.path.abspath("../../"))
    temp_path = os.path.join(os.getenv("RUNNER_TEMP", os.path.abspath("./temp")), 'docker_images_check')
    dockerhub_password = get_parameter_from_ssm('dockerhub_robot_password')

    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event, False, True)
    changed_images, dockerhub_repo_name = get_changed_docker_images(pr_info, repo_path, "docker/images.json")
    logging.info("Has changed images %s", ', '.join([str(image[0]) for image in changed_images]))
    pr_commit_version = str(pr_info.number) + '-' + pr_info.sha
    versions = [str(pr_info.number), pr_commit_version]
    if pr_info.number == 0:
        versions.append("latest")

    subprocess.check_output("docker login --username 'robotclickhouse' --password '{}'".format(dockerhub_password), shell=True)

    result_images = {}
    images_processing_result = []
    for rel_path, image_name in changed_images:
        full_path = os.path.join(repo_path, rel_path)
        images_processing_result += process_single_image(versions, full_path, image_name)
        result_images[image_name] = pr_commit_version

    if changed_images:
        description = "Updated " + ','.join([im[1] for im in changed_images])
    else:
        description = "Nothing to update"


    if len(description) >= 140:
        description = description[:136] + "..."

    s3_helper = S3Helper('https://s3.amazonaws.com')

    s3_path_prefix = str(pr_info.number) + "/" + pr_info.sha + "/" + NAME.lower().replace(' ', '_')
    status, test_results = process_test_results(s3_helper, images_processing_result, s3_path_prefix)

    url = upload_results(s3_helper, pr_info.number, pr_info.sha, test_results)

    gh = Github(get_best_robot_token())
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(context=NAME, description=description, state=status, target_url=url)

    with open(os.path.join(temp_path, 'changed_images.json'), 'w') as images_file:
        json.dump(result_images, images_file)

    print("::notice ::Report url: {}".format(url))
    print("::set-output name=url_output::\"{}\"".format(url))
