import json
import sys
from pathlib import Path

from ci.defs.defs import S3_BUCKET_NAME
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Picks up assets from the latest GitHub release of ClickHouse/clickhousectl
# and uploads them to s3://clickhouse-builds/clickhousectl/. Skips assets that
# already exist in S3.

REPO = "ClickHouse/clickhousectl"
S3_PREFIX = f"{S3_BUCKET_NAME}/clickhousectl"
ASSET_TARGETS = [
    "aarch64-apple-darwin",
    "aarch64-unknown-linux-musl",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-musl",
]


def asset_matches(name):
    if not name.startswith("clickhousectl-") or not name.endswith(".tar.gz"):
        return False
    middle = name[len("clickhousectl-") : -len(".tar.gz")]
    for target in ASSET_TARGETS:
        if middle.startswith(target + "-v"):
            return True
    return False


def fetch_release():
    output = Shell.get_output(
        f"gh release view --repo {REPO} --json tagName,assets", verbose=True
    )
    if not output:
        raise RuntimeError(f"Failed to fetch latest release for {REPO}")
    data = json.loads(output)
    tag = data["tagName"]
    assets = [a["name"] for a in data.get("assets", []) if asset_matches(a["name"])]
    return tag, assets


def process_asset(tag, asset_name, uploaded_links):
    s3_path = f"{S3_PREFIX}/{asset_name}"
    if S3.head_object(s3_path):
        print(f"Skip: s3://{s3_path} already exists")
        return True

    download_dir = Path(Settings.TEMP_DIR) / "clickhousectl"
    download_dir.mkdir(parents=True, exist_ok=True)
    local_path = download_dir / asset_name
    if local_path.exists():
        local_path.unlink()

    if not Shell.check(
        f"gh release download {tag} --repo {REPO} --pattern {asset_name} "
        f"--dir {download_dir}",
        verbose=True,
    ):
        raise RuntimeError(f"Failed to download asset [{asset_name}]")

    assert local_path.exists(), f"Downloaded file [{local_path}] not found"

    link = S3.copy_file_to_s3(
        local_path=str(local_path), s3_path=S3_PREFIX, content_type="application/gzip"
    )
    uploaded_links.append(link)
    return True


def main():
    results = []
    state = {"tag": None, "assets": []}

    def discover():
        tag, assets = fetch_release()
        state["tag"] = tag
        state["assets"] = assets
        print(f"Latest release tag: {tag}")
        print(f"Matching assets: {assets}")
        if not assets:
            raise RuntimeError(
                f"No assets matched the expected patterns for release {tag}"
            )

    results.append(
        Result.from_commands_run(name="Discover release assets", command=discover)
    )
    if not results[-1].is_ok():
        Result.create_from(results=results).complete_job(with_job_summary_in_info=False)
        sys.exit(1)

    uploaded_links = []
    sub_results = []
    for asset_name in state["assets"]:
        sub_results.append(
            Result.from_commands_run(
                name=asset_name,
                command=process_asset,
                command_args=[state["tag"], asset_name, uploaded_links],
            )
        )

    results.append(
        Result(
            name="Upload assets",
            status=(
                Result.Status.OK
                if all(r.is_ok() for r in sub_results)
                else Result.Status.FAIL
            ),
            results=sub_results,
        )
    )

    Result.create_from(results=results, links=uploaded_links).complete_job(
        with_job_summary_in_info=False
    )


if __name__ == "__main__":
    main()
