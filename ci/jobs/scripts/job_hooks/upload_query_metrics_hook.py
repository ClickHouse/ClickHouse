import sys
import traceback
from datetime import datetime
from pathlib import Path

from ci.praktika.info import Info
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Post-hook for the "Collect Query Metrics" jobs: it uploads the per-test peak
# memory JSON produced by `ci/jobs/functional_tests.py` (the `collect metrics`
# option) to the shared statistics prefix in S3, gzip-compressed. Keeping the
# upload here, in a post-hook, avoids polluting `functional_tests.py` with
# statistics-publishing concerns. It mirrors `collect_test_duration_statistics.py`:
# a stable name (latest) plus a dated name (history).

# JSON written by ClickHouseProc.collect_test_memory_stats.
METRICS_FILE = "./ci/tmp/test_memory_stats.json"


def _slug(text):
    return "".join(c if c.isalnum() else "_" for c in text).strip("_")


def main():
    metrics_file = Path(METRICS_FILE)
    if not metrics_file.is_file() or metrics_file.stat().st_size == 0:
        # No metrics produced (e.g. the job failed before collection, or a
        # non-metrics job ran this hook). Nothing to upload - skip quietly.
        print(f"No query metrics file at [{METRICS_FILE}] - nothing to upload")
        return

    # One key per job variant (e.g. parallel vs sequential) so they do not
    # overwrite each other under the shared statistics prefix.
    slug = _slug(Info().job_name)
    archive = f"./ci/tmp/{slug}.json.gz"
    archive_dated = (
        f"./ci/tmp/{slug}_{datetime.now().strftime('%d_%m_%Y')}.json.gz"
    )

    Shell.check(
        f"rm -f {archive} {archive_dated} && gzip -kc {METRICS_FILE} > {archive} && cp {archive} {archive_dated}",
        strict=True,
        verbose=True,
    )

    for local_path in (archive, archive_dated):
        link = S3.copy_file_to_s3(
            local_path=local_path,
            s3_path=f"{Settings.S3_REPORT_BUCKET}/statistics",
            content_type="application/json",
            content_encoding="gzip",
        )
        print(f"Uploaded query metrics: {link}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
