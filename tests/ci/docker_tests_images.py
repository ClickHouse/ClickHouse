import argparse

from praktika import Docker
from praktika.info import Info
from praktika.mangle import _get_workflows
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import Shell, Utils

from docker_images_helper import get_images_oredered_list

# TODO: script is created for seamless transision to praktika ci, all docker images are supposed to be described and manged in praktika, this script is to be removed


def build_image(image, tag, path, parent_tag, push, log_file):
    if parent_tag:
        from_tag_arg = f"--build-arg FROM_TAG={parent_tag}"
    else:
        from_tag_arg = f" "

    cmd = f"docker buildx build --builder default {from_tag_arg} --tag {image}:{tag} --progress plain {path}"
    if push:
        cmd += " --push"
    return Shell.run(cmd, verbose=True, log_file=log_file)


def build_multiarch_manifest(image, tag, has_amd=True, has_arm=True):
    cmd = f"docker manifest create --amend {image}:{tag}"
    if has_amd:
        cmd += " {image}:{tag}-amd64"
    if has_arm:
        cmd += " {image}:{tag}-aarch64"
    return Shell.run(cmd, verbose=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--arch", type=str, required=True, help="arch suffix")
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push",
        action="store_false",
        dest="push",
        default=argparse.SUPPRESS,
        help="don't push images to docker hub",
    )
    return parser.parse_args()


def exist(docker_name, tag):
    code, out, err = Shell.get_res_stdout_stderr(
        f"docker manifest inspect {docker_name}:{tag}"
    )
    print(
        f"Docker inspect results for {docker_name}:{tag}: exit code [{code}], out [{out}], err [{err}]"
    )
    if "no such manifest" in err:
        return False
    else:
        return True


def main():
    args = parse_args()
    assert args.arch in ("arm", "amd", "multi")

    aarch_suffix = f"-{args.arch}" if args.arch in ("aarch64", "amd64") else ""
    images_build_list = get_images_oredered_list()
    docker_digests = Info().get_custom_data("digest_dockers")

    res = True
    if res:
        if args.push and not Docker.login(
            Settings.DOCKERHUB_USERNAME,
            user_password=_get_workflows(Info().workflow_name)[0]
            .get_secret(Settings.DOCKERHUB_SECRET)
            .get_value(),
        ):
            res = False

    results = []
    if res:
        if args.arch != "multi":
            for docker in images_build_list:
                stopwatch_ = Utils.Stopwatch()
                docker_name = docker.repo
                tag = f"{docker_digests[docker_name]}{aarch_suffix}"
                log_file = f"{Settings.OUTPUT_DIR}/docker_{Utils.normalize_string(docker_name)}.log"
                files = []
                if not exist(docker_name, tag):
                    parent_tag = (
                        None
                        if not docker.parent
                        else f"{docker_digests[docker.parent]}{aarch_suffix}"
                    )
                    ret_code = build_image(
                        image=docker_name,
                        tag=tag,
                        path=docker.path,
                        parent_tag=parent_tag,
                        push=args.push,
                        log_file=log_file,
                    )
                    status = Result.Status.SUCCESS
                    if not ret_code == 0:
                        files.append(log_file)
                        status = Result.Status.FAILED
                else:
                    status = Result.Status.SKIPPED

                results.append(
                    Result(
                        name=docker_name,
                        status=status,
                        duration=stopwatch_.duration,
                        start_time=stopwatch_.start_time,
                        files=files,
                    )
                )
                if not results[-1].is_ok():
                    break
        else:
            for docker in images_build_list:
                stopwatch_ = Utils.Stopwatch()
                docker_name = docker.repo
                tag = f"{docker_digests[docker_name]}"
                if not exist(docker_name, tag):
                    ret_code = build_multiarch_manifest(
                        image=docker_name, tag=tag, has_arm=not docker.only_amd64
                    )
                    status = Result.Status.SUCCESS
                    if not ret_code == 0:
                        status = Result.Status.FAILED
                else:
                    status = Result.Status.SKIPPED

                results.append(
                    Result(
                        name=docker_name,
                        status=status,
                        duration=stopwatch_.duration,
                        start_time=stopwatch_.start_time,
                    )
                )
                if not results[-1].is_ok():
                    break
    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
