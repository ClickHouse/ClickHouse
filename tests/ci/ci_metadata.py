from pathlib import Path
from typing import Optional

from ci_utils import GH
from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_UPSTREAM_REPOSITORY,
    S3_BUILDS_BUCKET,
    S3_BUILDS_BUCKET_PUBLIC,
    TEMP_PATH,
)
from s3_helper import S3Helper
from synchronizer_utils import SYNC_BRANCH_PREFIX

# pylint: disable=too-many-lines


class CiMetadata:
    """
    CI Metadata class owns data like workflow run_id for a given pr, etc.
    Goal is to have everything we need to manage workflows on S3 and rely on GH api as little as possible
    """

    _S3_PREFIX = "CI_meta_v1"
    _LOCAL_PATH = Path(TEMP_PATH) / "ci_meta"
    _FILE_SUFFIX = ".cimd"
    _FILENAME_RUN_ID = "run_id" + _FILE_SUFFIX
    _FILENAME_SYNC_PR_RUN_ID = "sync_pr_run_id" + _FILE_SUFFIX

    def __init__(
        self,
        s3: S3Helper,
        pr_number: Optional[int] = None,
        git_ref: Optional[str] = None,
        sha: Optional[str] = None,
    ):
        assert pr_number or (sha and git_ref)

        self.sha = sha
        self.pr_number = pr_number
        self.git_ref = git_ref
        self.s3 = s3
        self.run_id = 0
        self.upstream_pr_number = 0
        self.sync_pr_run_id = 0

        if self.pr_number:
            self.s3_path = f"{self._S3_PREFIX}/PRs/{self.pr_number}/"
        else:
            self.s3_path = f"{self._S3_PREFIX}/{self.git_ref}/{self.sha}/"

        # Process upstream StatusNames.SYNC:
        # metadata path for upstream pr
        self.s3_path_upstream = ""
        if (
            self.git_ref
            and self.git_ref.startswith(f"{SYNC_BRANCH_PREFIX}/pr/")
            and GITHUB_REPOSITORY != GITHUB_UPSTREAM_REPOSITORY
        ):
            self.upstream_pr_number = int(self.git_ref.split("/pr/", maxsplit=1)[1])
            self.s3_path_upstream = f"{self._S3_PREFIX}/PRs/{self.upstream_pr_number}/"

        self._updated = False

        if not self._LOCAL_PATH.exists():
            self._LOCAL_PATH.mkdir(parents=True, exist_ok=True)

    def fetch_meta(self):
        """
        Fetches meta from s3
        """

        # clean up
        for file in self._LOCAL_PATH.glob("*" + self._FILE_SUFFIX):
            file.unlink()

        _ = self.s3.download_files(
            bucket=S3_BUILDS_BUCKET,
            s3_path=self.s3_path,
            file_suffix=self._FILE_SUFFIX,
            local_directory=self._LOCAL_PATH,
        )

        meta_files = Path(self._LOCAL_PATH).rglob("*" + self._FILE_SUFFIX)
        for file_name in meta_files:
            path_in_str = str(file_name)
            with open(path_in_str, "r", encoding="utf-8") as f:
                # Read all lines in the file
                lines = f.readlines()
                assert len(lines) == 1
            if file_name.name == self._FILENAME_RUN_ID:
                self.run_id = int(lines[0])
            elif file_name.name == self._FILENAME_SYNC_PR_RUN_ID:
                self.sync_pr_run_id = int(lines[0])

        self._updated = True
        return self

    def push_meta(
        self,
    ) -> None:
        """
        Uploads meta on s3
        """
        assert self.run_id
        assert self.git_ref, "Push meta only with full info"

        if not self.upstream_pr_number:
            log_title = f"Storing workflow metadata: PR [{self.pr_number}]"
        else:
            log_title = f"Storing workflow metadata: PR [{self.pr_number}], upstream PR [{self.upstream_pr_number}]"

        GH.print_in_group(
            log_title,
            [f"run_id: {self.run_id}"],
        )

        local_file = self._LOCAL_PATH / self._FILENAME_RUN_ID
        with open(local_file, "w", encoding="utf-8") as file:
            file.write(f"{self.run_id}\n")

        _ = self.s3.upload_file(
            bucket=S3_BUILDS_BUCKET,
            file_path=local_file,
            s3_path=self.s3_path + self._FILENAME_RUN_ID,
        )

        if self.upstream_pr_number:
            # store run id in upstream pr meta as well
            _ = self.s3.upload_file(
                bucket=S3_BUILDS_BUCKET_PUBLIC,
                file_path=local_file,
                s3_path=self.s3_path_upstream + self._FILENAME_SYNC_PR_RUN_ID,
            )


if __name__ == "__main__":
    # TEST:
    s3 = S3Helper()
    a = CiMetadata(s3, 12345, "deadbeaf", "test_branch")
    a.run_id = 111
    a.push_meta()
    b = CiMetadata(s3, 12345, "deadbeaf", "test_branch")
    assert b.fetch_meta().run_id == a.run_id

    a = CiMetadata(s3, 0, "deadbeaf", "test_branch")
    a.run_id = 112
    a.push_meta()
    b = CiMetadata(s3, 0, "deadbeaf", "test_branch")
    assert b.fetch_meta().run_id == a.run_id
