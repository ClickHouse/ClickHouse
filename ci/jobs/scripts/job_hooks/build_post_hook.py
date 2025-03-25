import traceback

from ci.defs.defs import S3_BUCKET_NAME, BuildTypes
from ci.praktika.info import Info
from ci.praktika.s3 import S3

BUILD_TYPE_TO_STATIC_LOCATION = {
    BuildTypes.AMD_RELEASE: "amd64",
    BuildTypes.ARM_RELEASE: "aarch64",
    BuildTypes.AMD_DARWIN: "macos",
    BuildTypes.ARM_DARWIN: "macos-aarch64",
    BuildTypes.ARM_V80COMPAT: "aarch64v80compat",
    BuildTypes.AMD_FREEBSD: "freebsd",
    BuildTypes.PPC64LE: "powerpc64le",
    BuildTypes.AMD_COMPAT: "amd64compat",
    BuildTypes.AMD_MUSL: "amd64musl",
    BuildTypes.RISCV64: "riscv64",
    BuildTypes.S390X: "s390x",
    BuildTypes.LOONGARCH64: "loongarch64",
}


def check():
    info = Info()
    if (
        not info.pr_number
        and info.repo_name == "ClickHouse/ClickHouse"
    ):
        for build_type, prefix in BUILD_TYPE_TO_STATIC_LOCATION.items():
            if build_type in info.job_name:
                try:
                    S3.copy_file_to_s3(
                        local_path=f"./ci/tmp/build/programs/clickhouse",
                        s3_path=f"{S3_BUCKET_NAME}/{info.git_branch}/{BUILD_TYPE_TO_STATIC_LOCATION[build_type]}/clickhouse-full",
                    )
                    S3.copy_file_to_s3(
                        local_path=f"./ci/tmp/build/programs/clickhouse-stripped",
                        s3_path=f"{S3_BUCKET_NAME}/{info.git_branch}/{BUILD_TYPE_TO_STATIC_LOCATION[build_type]}/clickhouse",
                    )
                except Exception as e:
                    traceback.print_exc()
        else:
            print(f"skip for [{info.job_name}]")
    else:
        print(f"skip for PRs")
    return True


if __name__ == "__main__":
    check()
