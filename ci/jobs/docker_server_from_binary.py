import argparse
from typing import List

from praktika import Secret
from praktika.result import Result
from praktika.utils import Shell, Utils


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


def main():

    stopwatch = Utils.Stopwatch()

    args = parse_args()

    image_path = "./ci/docker/clickhouse-server"
    image_repo = "clickhouse/clickhouse-server"

    push = args.push

    Shell.check(
        f"cp /tmp/praktika/input/clickhouse {image_path}/", strict=True, verbose=True
    )

    if push:
        docker_login()

    # print("Following tags will be created: %s", ", ".join(tags))

    results = []  # type: List[Result]

    command = f"docker buildx build --platform linux/arm64 \
                    -t {image_repo}:tmp {image_path} -f {image_path}/from_binary/Dockerfile.ubuntu \
                    --cache-from=type=local,src=/tmp/build-cache \
                    --cache-to=type=local,dest=/tmp/build-cache"
    results.append(
        Result.from_commands_run(
            name=f"{image_repo}:tmp", command=command, with_log=True
        )
    )

    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
