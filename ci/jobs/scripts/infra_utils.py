from ci.praktika.s3 import S3
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell


class BinaryFinder:
    BUCKET = "clickhouse-builds"
    S3_PREFIX_TEMPLATE = "REFs/master/{commit}/build_{build_type}/{name}"
    MAX_COMMITS = 40

    @classmethod
    def get_commit_sha(cls, n):
        """Get the SHA for HEAD~n"""
        return Shell.get_output(f"git rev-parse HEAD~{n}")

    @classmethod
    def check_s3_object_exists(cls, prefix):
        """Check if the object exists using aws s3 ls"""
        return Shell.check(
            f"aws s3api head-object --bucket {cls.BUCKET} --key {prefix}"
        )

    @classmethod
    def find_first_existing_artifact(
        cls, build_type, binary_name="clickhouse", download=False
    ):
        for i in range(cls.MAX_COMMITS):
            commit = cls.get_commit_sha(i)
            if not commit:
                print("🔚 Reached end of commit history without finding the object.")
                break
            bucket = Settings.S3_ARTIFACT_PATH
            prefix = cls.S3_PREFIX_TEMPLATE.format(
                bucket=bucket, commit=commit, build_type=build_type, name=binary_name
            )
            print(f"🔍 Checking: {prefix}")
            if cls.check_s3_object_exists(prefix):
                print(f"✅ Found artifact for commit {commit}: {prefix}")
                if download:
                    s3_path = f"{bucket}/{prefix}"
                    assert S3.copy_file_from_s3(s3_path=s3_path, local_path="./ci/tmp")
                return commit
        print("❌ No artifact found in the last 100 commits.")
        return None


if __name__ == "__main__":
    assert BinaryFinder.find_first_existing_artifact(
        "arm_asan", binary_name="clickhouse", download=True
    )
