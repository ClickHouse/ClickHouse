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

    @classmethod
    def prepare_report(cls, is_test):
        """Deploy the React-based report UI to S3"""
        report_dir = Path("./ci/praktika/report-click-ui")
        dist_dir = report_dir / "dist"

        # Build the React app
        print("Building report UI...")
        Shell.check(f"cd {report_dir} && npm install && npm run build")

        # Get base S3 path
        base_s3_path = Settings.HTML_S3_PATH

        # Upload index.html
        index_file = dist_dir / "index.html"
        if is_test:
            s3_index_name = "report_test.html"
        else:
            s3_index_name = "report.html"

        compressed_index = Utils.compress_gz(str(index_file))
        S3.copy_file_to_s3(
            s3_path=f"{base_s3_path}/{s3_index_name}",
            local_path=compressed_index,
            content_type="text/html",
            content_encoding="gzip",
            with_rename=True,
        )
        print(f"Uploaded {s3_index_name}")

        # Upload assets directory
        assets_dir = dist_dir / "assets"
        if assets_dir.exists():
            for asset_file in assets_dir.iterdir():
                if asset_file.is_file():
                    compressed_asset = Utils.compress_gz(str(asset_file))

                    # Determine content type
                    if asset_file.suffix == ".js":
                        content_type = "application/javascript"
                    elif asset_file.suffix == ".css":
                        content_type = "text/css"
                    else:
                        content_type = "application/octet-stream"

                    S3.copy_file_to_s3(
                        s3_path=f"{base_s3_path}/assets/{asset_file.name}",
                        local_path=compressed_asset,
                        content_type=content_type,
                        content_encoding="gzip",
                        with_rename=True,
                    )
                    print(f"Uploaded assets/{asset_file.name}")
