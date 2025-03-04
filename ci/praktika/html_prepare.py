from praktika.utils import Shell

from .s3 import S3
from .settings import Settings


class Html:
    @classmethod
    def prepare(cls, is_test):
        if is_test:
            page_file = Settings.HTML_PAGE_FILE.removesuffix(".html") + "_test.html"
            Shell.check(f"cp {Settings.HTML_PAGE_FILE} {page_file}")
        else:
            page_file = Settings.HTML_PAGE_FILE
        S3.copy_file_to_s3(s3_path=Settings.HTML_S3_PATH, local_path=page_file)
