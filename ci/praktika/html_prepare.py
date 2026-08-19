from praktika.utils import Shell

from .s3 import S3
from .settings import Settings


class Html:
    @classmethod
    def prepare(cls, is_test):
        import htmlmin

        if is_test:
            page_file = Settings.HTML_PAGE_FILE.removesuffix(".html") + "_test.html"
            Shell.check(f"cp {Settings.HTML_PAGE_FILE} {page_file}")
        else:
            page_file = Settings.HTML_PAGE_FILE

        with open(page_file, "r", encoding="utf-8") as f:
            html = f.read()

        minified = htmlmin.minify(html, remove_comments=True, remove_empty_space=True)

        with open(page_file, "w", encoding="utf-8") as f:
            f.write(minified)

        S3.copy_file_to_s3(s3_path=Settings.HTML_S3_PATH, local_path=page_file)

        with open(page_file, "w", encoding="utf-8") as f:
            f.write(html)
