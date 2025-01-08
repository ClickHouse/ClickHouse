import argparse
from typing import List

from praktika import Secret
from praktika.result import Result
from praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="A program to build clickhouse-server image, both alpine and "
        "ubuntu versions",
    )
    parser.add_argument(
        "--tag-type",
        type=str,
        choices=("head", "release", "release-latest"),
        default="head",
        help="defines required tags for resulting docker image. "
        "head - for master image (tag: head) "
        "release - for release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX) "
        "release-latest - for latest release image (tags: XX, XX.XX, XX.XX.XX, XX.XX.XX.XX, latest) ",
    )
    parser.add_argument("--push", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--os", default=["ubuntu", "alpine"], help=argparse.SUPPRESS)
    parser.add_argument("--from-binary", action="store_true")
    parser.add_argument("--from-deb", action="store_true")
    parser.add_argument("--no-arm", action="store_true")
    parser.add_argument("--no-amd", action="store_true")
    parser.add_argument("--no-alpine", action="store_true")
    return parser.parse_args()


def docker_login(relogin: bool = True) -> None:
    if relogin or not Shell.check(
        "docker system info | grep --quiet -E 'Username|Registry'"
    ):
        Shell.check(  # pylint: disable=unexpected-keyword-arg
            "docker login --username 'robotclickhouse' --password-stdin",
            strict=True,
            stdin_str=Secret.Config(
                "dockerhub_robot_password", type=Secret.Type.AWS_SSM_VAR
            ).get_value(),
            encoding="utf-8",
        )


# TODO: once tgz packaging is added:
#  1. add alpine
#  2. add keeper ubuntu
#  3. add keeper alpine
#  4. add appropriate docker tag
#  5. docker push


def main():

    stopwatch = Utils.Stopwatch()

    args = parse_args()
    image_path = "./ci/docker/clickhouse-server"
    image_repo = "clickhouse/clickhouse-server"
    platforms = "linux/arm64,linux/amd64"

    if args.no_arm:
        platforms = "linux/amd64"
    elif args.no_amd:
        platforms = "linux/arm64"

    assert args.from_binary ^ args.from_deb
    if args.from_binary:
        assert (
            args.no_arm or args.no_amd
        ), "Multiarch not supported with --from-binary, add --no-arm or --no-amd"
        docker_path = f"{image_path}/from_binary"
        Shell.check(
            f"cp {temp_dir}/clickhouse {image_path}/",
            strict=True,
            verbose=True,
        )
    elif args.from_deb:
        docker_path = f"{image_path}/from_deb"
        Shell.check(f"ls {temp_dir}", strict=True, verbose=True)
        Shell.check(f"mkdir -p {image_path}/debs", strict=True, verbose=True)
        Shell.check(
            f"cp {temp_dir}/*.deb {image_path}/debs",
            strict=True,
            verbose=True,
        )
    else:
        assert False

    push = args.push

    if push:
        docker_login()

    results = []  # type: List[Result]

    command = f"docker buildx build --platform {platforms} \
                    -t {image_repo}:tmp {image_path} -f {docker_path}/Dockerfile.ubuntu"

    results.append(
        Result.from_commands_run(
            name=f"{image_repo}:tmp", command=command, with_log=True
        )
    )

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
