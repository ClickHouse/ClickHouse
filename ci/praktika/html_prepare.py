from pathlib import Path

from praktika.utils import Shell

from .s3 import S3
from .settings import Settings
from .utils import Utils


class Html:
    @classmethod
    def prepare(cls, is_test):
        if is_test:
            page_file = Settings.HTML_PAGE_FILE.removesuffix(".html") + "_test.html"
            Shell.check(f"cp {Settings.HTML_PAGE_FILE} {page_file}")
        else:
            page_file = Settings.HTML_PAGE_FILE

        with open(page_file, "r", encoding="utf-8") as f:
            html = f.read()

        with open(page_file, "w", encoding="utf-8") as f:
            f.write(html)

        compressed_file = Utils.compress_gz(page_file)

        S3.copy_file_to_s3(
            s3_path=str(Path(Settings.HTML_S3_PATH) / Path(page_file).name),
            local_path=compressed_file,
            content_type="text/html",
            content_encoding="gzip",
            with_rename=True,
        )

        with open(page_file, "w", encoding="utf-8") as f:
            f.write(html)
