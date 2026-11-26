import traceback

from ci.defs.defs import S3_BUCKET_NAME, BuildTypes
from ci.praktika.info import Info
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell

BUILD_TYPE_TO_STATIC_LOCATION = {
    # The static binary runs on glibc and musl systems alike. It is also published under the
    # `amd64musl` path of the former musl build, which the installer still selects on musl systems
    # because release branches built before the switch publish a glibc binary under `amd64`.
    BuildTypes.AMD_RELEASE: ("amd64", "amd64musl"),
    BuildTypes.ARM_RELEASE: ("aarch64",),
    BuildTypes.AMD_DARWIN: ("macos",),
    BuildTypes.ARM_DARWIN: ("macos-aarch64",),
    BuildTypes.ARM_V80COMPAT: ("aarch64v80compat",),
    BuildTypes.AMD_FREEBSD: ("freebsd",),
    BuildTypes.AMD_ILLUMOS: ("illumos",),
    BuildTypes.PPC64LE: ("powerpc64le",),
    BuildTypes.AMD_COMPAT: ("amd64compat",),
    BuildTypes.RISCV64: ("riscv64",),
    BuildTypes.S390X: ("s390x",),
    BuildTypes.LOONGARCH64: ("loongarch64",),
}


def check():
    info = Info()
    Shell.check("find ./ci/tmp/build/programs -type f", verbose=True)
    if not info.pr_number and info.repo_name == "ClickHouse/ClickHouse":
        for build_type, locations in BUILD_TYPE_TO_STATIC_LOCATION.items():
            if build_type in info.job_name:
                print("Upload builds to static location")
                try:
                    for location in locations:
                        S3.copy_file_to_s3(
                            local_path=f"./ci/tmp/build/programs/self-extracting/clickhouse",
                            s3_path=f"{S3_BUCKET_NAME}/{info.git_branch}/{location}/clickhouse-full",
                            with_rename=True,
                        )
                        S3.copy_file_to_s3(
                            local_path=f"./ci/tmp/build/programs/self-extracting/clickhouse-stripped",
                            s3_path=f"{S3_BUCKET_NAME}/{info.git_branch}/{location}/clickhouse",
                            with_rename=True,
                        )
                except AssertionError:
                    raise
                except Exception as e:
                    traceback.print_exc()
                return
        print(f"Not applicable for [{info.job_name}]")
    else:
        print(f"Not applicable")
    return True


if __name__ == "__main__":
    check()
