import traceback

from ci.defs.defs import S3_BUCKET_NAME, BuildTypes
from ci.praktika.info import Info
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell

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
    Shell.check("find ./ci/tmp/build/programs -type f", verbose=True)
    if not info.pr_number and info.repo_name == "ClickHouse/ClickHouse":
        for build_type, prefix in BUILD_TYPE_TO_STATIC_LOCATION.items():
            if build_type in info.job_name:
                print("Upload builds to static location")
                try:
                    S3.copy_file_to_s3(
                        local_path=f"./ci/tmp/build/programs/self-extracting/clickhouse",
                        s3_path=f"{S3_BUCKET_NAME}/{info.git_branch}/{BUILD_TYPE_TO_STATIC_LOCATION[build_type]}/clickhouse-full",
                        with_rename=True,
                    )
                    S3.copy_file_to_s3(
                        local_path=f"./ci/tmp/build/programs/self-extracting/clickhouse-stripped",
                        s3_path=f"{S3_BUCKET_NAME}/{info.git_branch}/{BUILD_TYPE_TO_STATIC_LOCATION[build_type]}/clickhouse",
                        with_rename=True,
                    )
                except Exception as e:
                    traceback.print_exc()
                return
        print(f"Not applicable for [{info.job_name}]")
    else:
        print(f"Not applicable")
    return True

    # TODO: Upload profile data
    # profile_result = None
    # if (
    #     res and JobStages.UPLOAD_PROFILE_DATA in stages
    #     and "release" in build_type
    #     and not Info().is_local_run
    # ):
    #     sw_ = Utils.Stopwatch()
    #     print("Prepare build profile data")
    #     profiles_dir = Path(temp_dir) / "profiles_source"
    #     profiles_dir.mkdir(parents=True, exist_ok=True)
    #     is_success = True
    #     try:
    #         Shell.check(
    #             "./utils/prepare-time-trace/prepare-time-trace.sh "
    #             f"{build_dir} {profiles_dir.absolute()}",
    #             strict=True,
    #             verbose=True,
    #         )
    #         profile_data_file = Path(temp_dir) / "profile.json"
    #         with open(profile_data_file, "wb") as profile_fd:
    #             for profile_source in profiles_dir.iterdir():
    #                 if profile_source.name not in (
    #                     "binary_sizes.txt",
    #                     "binary_symbols.txt",
    #                 ):
    #                     with open(profiles_dir / profile_source, "rb") as ps_fd:
    #                         profile_fd.write(ps_fd.read())
    #         LogClusterBuildProfileQueries().insert_profile_data(
    #             build_name=build_type,
    #             start_time=stop_watch.start_time,
    #             file=profile_data_file,
    #         )
    #         LogClusterBuildProfileQueries().insert_build_size_data(
    #             build_name=build_type,
    #             start_time=stop_watch.start_time,
    #             file=profiles_dir / "binary_sizes.txt",
    #         )
    #         LogClusterBuildProfileQueries().insert_binary_symbol_data(
    #             build_name=build_type,
    #             start_time=stop_watch.start_time,
    #             file=profiles_dir / "binary_symbols.txt",
    #         )
    #     except Exception as e:
    #         is_success = False
    #         traceback.print_exc()
    #         print(f"ERROR: Failed to upload build profile data. ex: [{e}]")
    #     profile_result = Result.create_from(
    #         name="Build Profile", status=is_success, stopwatch=sw_
    #     )


if __name__ == "__main__":
    check()
