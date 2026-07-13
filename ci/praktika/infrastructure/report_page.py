from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


class ReportPage:

    @dataclass
    class Config:
        """An HTML report page uploaded to S3 on deploy.

        Reads the file at `path`, uploads it to Settings.S3_REPORT_BUCKET
        compressed with gzip. The uploaded filename matches the source filename.

        Idempotent: re-uploading overwrites the existing file.
        """

        path: str
        region: str = ""
        bucket_name: str = ""
        ext: Dict[str, Any] = field(default_factory=dict)

        def deploy(self, is_test: bool = False):
            from ..s3 import S3
            from ..settings import Settings
            from ..utils import Utils

            page_file = self.path
            if is_test:
                page_file = self.path.removesuffix(".html") + "_test.html"
                import shutil
                shutil.copy(self.path, page_file)

            with open(page_file, "r", encoding="utf-8") as f:
                html = f.read()

            compressed = Utils.compress_gz(page_file)

            bucket_name = self.bucket_name or Settings.S3_REPORT_BUCKET

            S3.copy_file_to_s3(
                s3_path=str(Path(bucket_name) / Path(page_file).name),
                local_path=compressed,
                content_type="text/html",
                content_encoding="gzip",
                with_rename=True,
            )
            print(f"Uploaded report page '{Path(page_file).name}' to s3://{bucket_name}")

            with open(page_file, "w", encoding="utf-8") as f:
                f.write(html)

            return self
