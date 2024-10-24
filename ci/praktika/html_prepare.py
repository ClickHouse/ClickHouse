from praktika.s3 import S3
from praktika.settings import Settings


class Html:
    @classmethod
    def prepare(cls):
        S3.copy_file_to_s3(
            s3_path=Settings.HTML_S3_PATH, local_path=Settings.HTML_PAGE_FILE
        )
