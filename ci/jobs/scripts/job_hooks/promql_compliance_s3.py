"""S3 baseline for PromQL compliance (same layout idea as LLVM coverage .info).

Master publishes to (HTTPS, public read):
  https://clickhouse-builds.s3.amazonaws.com/REFs/master/<sha>/promql_compliance/promql_compliance_result.json

PR jobs walk ``master_track_commits_sha`` (Config Workflow / store_data) and use the first
object that exists, mirroring ``generate_diff_coverage_report.sh`` for LLVM.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ci.defs.defs import S3_BUCKET_HTTP_ENDPOINT
from ci.praktika.info import Info
from ci.praktika.settings import Settings
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell

S3_KEY_DIR = "promql_compliance"
RESULT_NAME = "promql_compliance_result.json"
UPSTREAM_REPO = "ClickHouse/ClickHouse"
MASTER_BRANCH = "master"
URL_TIMEOUT_SEC = 30


def _baseline_payload_ok(data: dict[str, Any]) -> bool:
    """Reject malformed or non-finite S3 JSON (same spirit as strict baseline checks)."""
    for k in ("pct", "passed", "failed", "unsupported"):
        if k not in data:
            return False
    try:
        pct = float(data["pct"])
        if not math.isfinite(pct):
            return False
        for k in ("passed", "failed", "unsupported"):
            int(data[k])
    except (TypeError, ValueError):
        return False
    return True


def result_url_for_master_commit(sha: str) -> str:
    return (
        f"https://{S3_BUCKET_HTTP_ENDPOINT}/REFs/{MASTER_BRANCH}/{sha}/"
        f"{S3_KEY_DIR}/{RESULT_NAME}"
    )


def fetch_baseline_from_s3(commits: list[str]) -> Tuple[Optional[dict[str, Any]], Optional[str]]:
    """Return (parsed_json, commit_sha_used) or (None, None) if no hit."""
    for c in commits:
        c = (c or "").strip()
        if len(c) != 40:
            continue
        url = result_url_for_master_commit(c)
        try:
            req = Request(
                url,
                headers={"User-Agent": "ClickHouse-CI-promql-compliance"},
            )
            with urlopen(req, timeout=URL_TIMEOUT_SEC) as resp:
                if resp.status != 200:
                    continue
                raw = resp.read()
            data = json.loads(raw.decode("utf-8"))
            if isinstance(data, dict) and _baseline_payload_ok(data):
                return data, c
        except HTTPError as e:
            if e.code == 404:
                continue
        except (URLError, json.JSONDecodeError, OSError, ValueError):
            continue
    return None, None


def master_track_commits(info: Info) -> list[str]:
    """SHAs on master from store_data, or GH merge-base walk (same fallback as llvm_coverage_job)."""
    out = list(info.get_kv_data("master_track_commits_sha") or [])
    if out:
        return out
    if info.is_local_run or info.pr_number <= 0:
        return []
    sha = (info.sha or "").strip()
    if len(sha) != 40:
        return []
    try:
        merge_base = Shell.get_output(
            f"gh api repos/ClickHouse/ClickHouse/compare/master...{sha} -q .merge_base_commit.sha",
            strict=False,
            verbose=False,
        ).strip()
        if not merge_base:
            return []
        raw = Shell.get_output(
            f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={merge_base}&per_page=30' -q '.[].sha'",
            strict=False,
            verbose=False,
        )
        return [ln.strip() for ln in raw.splitlines() if ln.strip()]
    except Exception:
        return []


def upload_master_result(local_path: Path, commit_sha: str) -> Optional[str]:
    """Upload JSON to canonical S3 path; return public URL or None on failure."""
    if not local_path.is_file():
        return None
    prefix = Settings.S3_ARTIFACT_PATH or ""
    if not prefix:
        print("PromQL compliance S3: S3_ARTIFACT_PATH empty, skip upload")
        return None
    s3_dir = f"{prefix}/REFs/{MASTER_BRANCH}/{commit_sha}/{S3_KEY_DIR}"
    try:
        link = S3.copy_file_to_s3(s3_path=s3_dir, local_path=str(local_path))
        print(f"PromQL compliance S3: uploaded baseline to {link}")
        return link
    except Exception as e:
        print(f"PromQL compliance S3: upload failed: {e}")
        return None


def should_publish_master_baseline(info: Info) -> bool:
    return (
        info.pr_number <= 0
        and (info.git_branch or "").strip() == MASTER_BRANCH
        and (info.repo_name or "").strip() == UPSTREAM_REPO
    )
