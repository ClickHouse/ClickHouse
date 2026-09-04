"""Docs check (Nimbus): validates the docs and triggers the Workers Build preview.

Steps (each fails the job when it fails):
  1. Source checks that need no build: the Sätteri MDX compile/reference check
     and the lychee link check (the Astro build itself is the full validator
     and runs on Cloudflare Workers Builds).
  2. Scope: which locales the preview needs (ci/jobs/scripts/docs/nimbus_scope.py).
  3. Mirror: push the PR head plus `docs/.preview-scope.json` to
     `refs/heads/preview/pr-<N>` in the main repository, cancel a running build
     of that branch, and wait for the new Workers Build.
  4. Comment: post or update the preview link on the PR.

The Workers Builds integration needs the secrets `cloudflare-builds-api-token`
(user-scoped) and `cloudflare-account-id` plus the build trigger id; without
them the job runs the checks and reports the preview step as skipped, so it
can be enabled before the Cloudflare side is provisioned.
"""

import json
import os
import shlex
import time
import urllib.error
import urllib.request

from ci.jobs.scripts.docs.nimbus_scope import compute_scope, scope_to_env
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

WORKER_NAME = "clickhouse-docs"
MIRROR_BRANCH_PREFIX = "preview/pr-"
BUILD_POLL_SECONDS = 15
BUILD_TIMEOUT_SECONDS = 20 * 60  # Workers Builds hard-stops at 20 minutes.
PREVIEW_COMMENT_TAG = "docs-preview"


def _docs_dir():
    return f"{Utils.cwd()}/docs"


def _source_checks():
    """Checks that run against the sources; the build itself runs on Cloudflare."""
    docs = _docs_dir()
    return [
        Result.from_commands_run(
            name="Install docs toolchain",
            command="pnpm install --frozen-lockfile",
            workdir=docs,
        ),
        Result.from_commands_run(
            name="Generate navigation and compat wrappers",
            command="node bin/gen-compat-wrappers.ts && node bin/gen-import-index.ts && node bin/gen-sidebar.ts",
            workdir=docs,
        ),
        Result.from_commands_run(
            name="MDX compiles and every component resolves",
            command="node bin/measure/mdx-compile-check.ts --refs get-started concepts guides reference products clickstack integrations resources chdb snippets",
            workdir=docs,
        ),
        Result.from_commands_run(
            name="Check internal links and anchors",
            command="python3 ../ci/jobs/scripts/docs/lychee_check.py --mode links .",
            workdir=docs,
        ),
    ]


def _cloudflare_config():
    token = os.environ.get("CLOUDFLARE_BUILDS_API_TOKEN")
    account = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    trigger = os.environ.get("CLOUDFLARE_DOCS_PREVIEW_TRIGGER_UUID")
    if not (token and account and trigger):
        return None
    return {"token": token, "account": account, "trigger": trigger}


def _cf_request(cfg, method, path, body=None):
    url = f"https://api.cloudflare.com/client/v4/accounts/{cfg['account']}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {cfg['token']}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def _cancel_running_builds(cfg, branch):
    """Workers Builds supersedes queued builds only; running ones must be cancelled."""
    listing = _cf_request(cfg, "GET", f"/builds/workers/{WORKER_NAME}/builds?per_page=20")
    for build in listing.get("result", []):
        meta = build.get("build_trigger_metadata") or {}
        if meta.get("branch") != branch:
            continue
        if build.get("status") in ("queued", "initializing", "running"):
            _cf_request(cfg, "PUT", f"/builds/builds/{build['build_uuid']}/cancel")
            print(f"Cancelled superseded build {build['build_uuid']}")


def _wait_for_build(cfg, build_uuid):
    deadline = time.time() + BUILD_TIMEOUT_SECONDS
    while time.time() < deadline:
        build = _cf_request(cfg, "GET", f"/builds/builds/{build_uuid}")["result"]
        status = build.get("status")
        if status in ("success", "failed", "canceled", "cancelled", "terminated"):
            return build
        time.sleep(BUILD_POLL_SECONDS)
    return {"status": "timeout", "build_uuid": build_uuid}


def _preview():
    """Mirror the PR head, trigger the Workers Build, wait, and comment."""
    info = Info()
    if info.pr_number <= 0:
        print("Not a pull request; no preview.")
        return True

    cfg = _cloudflare_config()
    if cfg is None:
        print("Cloudflare Workers Builds is not configured; preview skipped.")
        return True

    scope = compute_scope(info.get_changed_files())
    branch = f"{MIRROR_BRANCH_PREFIX}{info.pr_number}"
    alias = f"pr-{info.pr_number}"

    # Mirror branch: the PR head plus one commit carrying the preview scope.
    # `git push` uses the GitHub App token (enable_gh_auth) like the nightly jobs.
    scope_file = f"{_docs_dir()}/.preview-scope.json"
    with open(scope_file, "w") as f:
        json.dump(scope, f, indent=2)
    commands = [
        "git config user.name robot-clickhouse",
        "git config user.email robot-clickhouse@users.noreply.github.com",
        f"git checkout -B {shlex.quote(branch)} {shlex.quote(info.sha)}",
        f"git add {shlex.quote(scope_file)}",
        f"git commit -m {shlex.quote(f'Docs preview scope for PR #{info.pr_number}')}",
    ]
    for cmd in commands:
        if not Shell.check(cmd, verbose=True):
            print(f"Error: {cmd}")
            return False

    _cancel_running_builds(cfg, branch)

    # Pushing the branch fires the preview trigger (branch_includes: preview/*).
    if not Shell.check(
        f"git push --force origin HEAD:refs/heads/{shlex.quote(branch)}", verbose=True
    ):
        print("Error: could not push the mirror branch")
        return False

    # Ask the Builds API for the build pinned to this commit so we can wait on it.
    started = _cf_request(
        cfg,
        "POST",
        f"/builds/triggers/{cfg['trigger']}/builds",
        {"branch": branch},
    )
    build_uuid = started["result"]["build_uuid"]
    build = _wait_for_build(cfg, build_uuid)
    status = build.get("status")

    workers_subdomain = os.environ.get("CLOUDFLARE_WORKERS_SUBDOMAIN", "<subdomain>")
    preview_url = f"https://{alias}-{WORKER_NAME}.{workers_subdomain}.workers.dev/docs/"
    body_lines = [
        f"**Docs preview:** {preview_url}" if status == "success" else f"**Docs preview build {status}** (`{build_uuid}`)",
        f"Scope: locales `{', '.join(scope['locales'])}`, reference pages {'included' if scope.get('reference', True) else 'omitted'}.",
    ]
    GH.post_updateable_comment({PREVIEW_COMMENT_TAG: "\n".join(body_lines)})
    return status == "success"


if __name__ == "__main__":
    results = _source_checks()
    results.append(Result.from_commands_run(name="Preview build (Workers Builds)", command=_preview))
    Result.create_from(results=results).complete_job()
