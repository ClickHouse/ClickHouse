#!/usr/bin/env python3
"""Pre-hook: fetch the CI Logs cluster credentials on the host and write them into
the checkout, so a job whose container has no AWS access can still configure its
system-log export.

Pre-hooks run on the host (which has AWS access) before the job's container, and
the checkout is mounted into the container, so the job reads the credentials from
`log_export.CREDENTIALS_FILE` instead of calling SSM itself. Best effort: the
export is optional, so a failed fetch only warns and the job still runs (it finds
no file and skips the export with a warning of its own)."""

import json
import sys
from pathlib import Path

from ci.jobs.scripts import log_export
from ci.praktika.info import Info


def main():
    if Info().is_local_run:
        print("Local run - the CI Logs credentials are not fetched")
        return 0
    try:
        credentials = log_export.fetch_credentials_from_aws()
    except Exception as e:
        print(f"WARNING: could not fetch CI Logs credentials, log export will be skipped: {e}")
        return 0
    path = Path(log_export.CREDENTIALS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(credentials)))
    print(f"Wrote CI Logs credentials to [{path}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
