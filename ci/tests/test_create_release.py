"""
Regression / end-to-end guards for the Praktika CreateRelease workflow.

These tests pin the contract that the conversion of the legacy hand-written
``.github/workflows/create_release.yml`` into a Praktika-generated workflow
must keep:

  * every ``create_release.py`` CLI flag that ``release_job.py`` invokes is a
    real argparse option (catches the orchestrator drifting from the tool),
  * every workflow-dispatch input that ``release_job.py`` reads is declared by
    the workflow definition,
  * ``release_job.py`` points at the moved ``ci/jobs/scripts/create_release.py``
    and ``ci/jobs/scripts/artifactory.py``,
  * the generated workflow keeps the release-safety invariants (a ``release``
    concurrency group, the ``workflow_call`` reuse contract used by
    ``auto_releases.yml``, and boolean dispatch inputs),
  * the version arithmetic inlined into ``create_release.py`` round-trips.

The structural checks parse the sources with ``ast`` / read the generated YAML
as text, so they never import ``create_release.py`` (which pulls ``boto3`` via
``s3_helper``); the version-logic checks import it lazily behind
``importorskip`` so they are skipped where that dependency is absent.
"""

import ast
import json
import os
import re
import subprocess
import sys

import pytest

HERE = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(HERE, "../.."))
sys.path.insert(0, REPO_ROOT)

CREATE_RELEASE = os.path.join(REPO_ROOT, "ci/jobs/scripts/create_release.py")
RELEASE_JOB = os.path.join(REPO_ROOT, "ci/jobs/release_job.py")
WORKFLOW_DEF = os.path.join(REPO_ROOT, "ci/workflows/create_release.py")
WORKFLOW_YML = os.path.join(REPO_ROOT, ".github/workflows/create_release.yml")


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _head_sha(repo):
    """The current ``HEAD`` commit SHA of ``repo``. The version-file githash in
    these fixtures must point at a real commit so the (now strict) tweak =
    commit-count-since-githash is computable, so tests anchor it at a captured
    SHA instead of the ``0``*40 placeholder."""
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _argparse_long_flags(path):
    """Every ``--long-option`` registered via ``add_argument`` in ``path``."""
    flags = set()
    for node in ast.walk(ast.parse(_read(path))):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_argument"
        ):
            for arg in node.args:
                if (
                    isinstance(arg, ast.Constant)
                    and isinstance(arg.value, str)
                    and arg.value.startswith("--")
                ):
                    flags.add(arg.value)
    return flags


def _create_release_subcommands_used():
    """The ``--flag`` that immediately follows each ``create_release.py`` call."""
    return set(
        re.findall(r"create_release\.py\s+(--[a-z0-9-]+)", _read(RELEASE_JOB))
    )


def _workflow_input_names():
    """Names declared as ``Workflow.Config.InputConfig(name=...)`` in the def."""
    names = set()
    for node in ast.walk(ast.parse(_read(WORKFLOW_DEF))):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                    names.add(kw.value.value)
    return names


def _workflow_inputs_read_by_job():
    """Input names the job reads via ``Info.get_workflow_input_value`` / ``_wi``."""
    text = _read(RELEASE_JOB)
    return set(
        re.findall(r'_wi\(\s*["\']([a-z0-9-]+)["\']', text)
        + re.findall(r'get_workflow_input_value\(\s*["\']([a-z0-9-]+)["\']', text)
    )


def test_release_job_only_uses_existing_create_release_flags():
    defined = _argparse_long_flags(CREATE_RELEASE)
    used = _create_release_subcommands_used()
    assert used, "release_job.py should invoke create_release.py with flags"
    missing = used - defined
    assert not missing, (
        f"release_job.py invokes create_release.py flags that do not exist: "
        f"{sorted(missing)} (defined: {sorted(defined)})"
    )


def test_workflow_declares_every_input_the_job_reads():
    declared = _workflow_input_names()
    read = _workflow_inputs_read_by_job()
    assert read, "release_job.py should read workflow-dispatch inputs"
    missing = read - declared
    assert not missing, (
        f"release_job.py reads workflow inputs not declared in the workflow "
        f"definition: {sorted(missing)} (declared: {sorted(declared)})"
    )


def test_release_job_points_at_moved_paths():
    text = _read(RELEASE_JOB)
    assert "./ci/jobs/scripts/create_release.py" in text
    assert "./ci/jobs/scripts/artifactory.py" in text
    # Both files were moved out of tests/ci (and are scripts, not jobs); the
    # orchestrator must not call the old locations.
    assert "tests/ci/create_release.py" not in text
    assert "./ci/jobs/create_release.py" not in text
    assert "./ci/jobs/artifactory.py" not in text
    assert "tests/ci/artifactory.py" not in text


def test_enqueue_is_last_and_patch_bump_is_deferred():
    """The PR enqueue must be the last release step, and the patch bump deferred.

    Enqueue runs last so the release PR's `CH Inc sync` check gets the maximum
    time (the whole publish) to complete before we add the PR to the merge queue.
    The patch version bump is still deferred to near the end (after the changelog
    PR is created, not right after the tag push): that keeps the branch tip equal
    to the released commit through publishing, so a rerun after any failure sees
    an un-bumped branch and prepare recovers the existing release instead of
    minting a below-tip one. The "new" release bump stays early (it opens the
    master bump PR the enqueue later merges).
    """
    text = _read(RELEASE_JOB)
    # Match the actual create_release.py invocations, not prose in comments.
    bump_positions = [
        m.start()
        for m in re.finditer(r"create_release\.py --create-bump-version-pr", text)
    ]
    # The enqueue step is an in-process function call (merge_created_prs), anchored
    # by its unique step name rather than a CLI string.
    merge_pos = text.find('name="Update Release Info and Merge Created PRs"')
    changelog_pr_pos = text.find('name="Create ChangeLog PR"')
    assert len(bump_positions) >= 2, (
        "expected a separate 'new' and deferred 'patch' --create-bump-version-pr"
    )
    assert merge_pos != -1, "release_job.py should have the enqueue step"
    assert changelog_pr_pos != -1, "release_job.py should have the Create ChangeLog PR step"
    # Enqueue is the last release action: after both version bumps.
    assert all(p < merge_pos for p in bump_positions), (
        "the enqueue step must run after both version bumps (it is the last step)"
    )
    # The 'new' bump is early (before the changelog PR step); the 'patch' bump is
    # deferred to after it.
    assert min(bump_positions) < changelog_pr_pos, (
        "the 'new' version bump must run early (before the changelog PR step)"
    )
    assert max(bump_positions) > changelog_pr_pos, (
        "the 'patch' version bump must be deferred to after the changelog PR step"
    )


def test_generated_workflow_preserves_release_invariants():
    yml = _read(WORKFLOW_YML)
    assert yml.startswith("# generated by praktika"), "stale / hand-edited YAML"
    # Releases must never overlap. Dispatch workflows always emit a concurrency
    # group defaulting to the workflow name, which serializes CreateRelease runs
    # (legacy used a fixed `group: release`).
    assert "concurrency:" in yml and "group: ${{ github.workflow }}" in yml
    # auto_releases.yml reuses this workflow via `uses:`, which needs both
    # triggers and the inherited secret.
    assert "workflow_dispatch:" in yml
    assert "workflow_call:" in yml
    # The release pushes with the robot PAT (the App token lacks the `workflow`
    # scope), so the commit-token secret must be declared on the workflow.
    assert "ROBOT_CLICKHOUSE_COMMIT_TOKEN" in yml
    # The env setup must read the `inputs` context too, otherwise workflow_call
    # (auto_releases) runs get an empty github.event.inputs and lose `ref`.
    assert "toJson(inputs)" in yml
    # Boolean dispatch inputs render as checkboxes.
    assert "type: boolean" in yml


# --- version arithmetic in clickhouse_version.py -----------------------------


def _create_release_module():
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3
    import ci.jobs.scripts.create_release as cr  # noqa: E402

    return cr


def test_version_file_roundtrips(tmp_path, monkeypatch):
    import ci.jobs.scripts.clickhouse_version as chv

    version_file = tmp_path / "autogenerated_versions.txt"
    monkeypatch.setattr(chv, "FILE_WITH_VERSION_PATH", str(version_file))

    version = chv.CHVersion(26, 6, 1, 54511, 42).with_description("stable")
    version.githash = "0" * 40
    version.write()
    read = chv._read_versions()
    # tweak is not stored as its own SET() line — it is encoded in the string
    # (major.minor.patch.tweak) and the describe.
    assert (read["major"], read["minor"], read["patch"], read["revision"]) == (
        26,
        6,
        1,
        54511,
    )
    assert read["string"] == "26.6.1.42"
    assert read["describe"] == "v26.6.1.42-stable"

    reloaded = chv.CHVersion.get_release_version()
    assert (reloaded.major, reloaded.minor, reloaded.patch) == (26, 6, 1)
    assert reloaded.tweak == 42


def test_version_bump():
    import ci.jobs.scripts.clickhouse_version as chv

    patch = chv.CHVersion(26, 6, 5, 54511, 7)
    patch.bump_patch()
    assert patch.patch == 6 and patch.tweak == 1

    rollover = chv.CHVersion(26, 12, 1, 100)
    rollover.bump_release()
    assert (rollover.major, rollover.minor, rollover.patch) == (27, 1, 1)


def test_new_is_a_valid_version_type():
    import ci.jobs.scripts.clickhouse_version as chv

    # `new` must be a valid version type so `with_description` accepts it when
    # CreateRelease cuts a fresh branch (the `vX.Y.1.1-new` marker).
    assert "new" in chv.VersionType.VALID
    version = chv.CHVersion(26, 6, 1, -1, 1).with_description("new")
    assert version.version_type == "new"
    assert version.describe == "v26.6.1.1-new"


# --- full dry-run patch release, start to finish -----------------------------

_VERSIONS_FILE = "cmake/autogenerated_versions.txt"
_CONTRIBUTORS_FILE = "src/Storages/System/StorageSystemContributors.generated.cpp"
_VERSIONS_CONTENT = """\
# This variables autochanged by ci/jobs/scripts/create_release.py:

SET(VERSION_REVISION 54500)
SET(VERSION_MAJOR 26)
SET(VERSION_MINOR 6)
SET(VERSION_PATCH 2)
SET(VERSION_GITHASH 0000000000000000000000000000000000000000)
SET(VERSION_DESCRIBE v26.6.2.1-stable)
SET(VERSION_STRING 26.6.2.1)
# end of autochange
"""


def test_dry_run_patch_release_end_to_end(tmp_path):
    """Drive create_release.py through a whole patch release in --dry-run.

    Builds a synthetic ClickHouse release branch (``26.6`` with a previous
    ``v26.6.1.1-stable`` tag) and runs the release steps that are hermetic in
    dry-run — preparing the release info, creating the tag, bumping the version
    + contributors, and walking the progress state machine to a completed
    status. The publish steps (download-packages / create-gh-release /
    artifactory / docker) require real S3, the GitHub API and a registry, so
    they are out of scope for an offline test. The only network call on this
    path, ``is_latest_release_branch``, is served by a `gh` stub on PATH.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    # The release tool and this setup create commits/tags; never sign them
    # (the environment may have commit.gpgsign / tag.gpgsign enabled globally).
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(_VERSIONS_CONTENT, encoding="utf-8")
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Base release commit")
    # The previous release on this branch.
    git("tag", "-a", "v26.6.1.1-stable", "-m", "Release v26.6.1.1-stable")
    prev = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    # Point the version-file githash at the previous release so the tweak counts
    # real commits on top of it (two here) — a non-empty patch release (26.6.2.2),
    # not the empty tweak==1 case that prepare refuses.
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, prev), encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Point version githash at previous release")
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Post-release commit")
    # Populate origin/26.6 and tags (the tool reads origin/<branch>).
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    # create_release.py resolves `s3_helper`/`ssh` relative to its own location
    # and computes the contributors "executer" as its path relative to cwd, so
    # it must live inside the repo it operates on (as it does in production).
    # Symlink the real ci/ and tests/ trees in (left untracked) and run that
    # in-repo copy.
    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    commit_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    # `gh` stub: is_latest_release_branch (now in create_release) is the only
    # network call on this path. It prints an empty JSON array (what a real
    # `gh pr list --json` prints when nothing matches), so the strict retried
    # read succeeds and reports "not the latest branch", which is enough.
    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)

    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    def step(*flags):
        result = subprocess.run(
            [sys.executable, script, *flags],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"step {flags} failed (rc={result.returncode})\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )
        return result

    step(
        "--prepare-release-info",
        "--ref",
        "26.6",  # new release from the branch tip
        "--release-type",
        "patch",
        "--dry-run",
    )

    with open("/tmp/release_info.json", encoding="utf-8") as f:
        info = json.load(f)
    assert info["release_type"] == "patch"
    assert info["release_branch"] == "26.6"
    assert info["release_tag"] == "v26.6.2.2-stable"
    assert info["version"] == "26.6.2.2"
    assert info["commit_sha"] == commit_sha
    assert info["create_new_release"] is True

    step("--push-release-tag", "--dry-run")
    step("--create-bump-version-pr", "--dry-run")
    final = step("--post-status", "--dry-run")
    assert "New release" in final.stdout


def test_prepare_recovers_from_tag(tmp_path):
    """Dispatching an existing release tag recovers (re-publishes) that release.

    Recovery is expressed by passing the version tag: ``prepare`` must set
    ``create_new_release=false`` and not attempt to create it again.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    # Anchor commit the release version-file githash points at, so the strict
    # tweak (commits since githash) is computable — here one commit -> tweak 1.
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Anchor commit (previous release)")
    anchor = _head_sha(repo)

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, anchor), encoding="utf-8"
    )
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Base release commit")
    # The release for this commit was already created on a previous attempt.
    git("tag", "-a", "v26.6.2.1-stable", "-m", "Release v26.6.2.1-stable")
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)
    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    result = subprocess.run(
        [
            sys.executable,
            script,
            "--prepare-release-info",
            "--ref",
            "v26.6.2.1-stable",  # recovery via the release tag
            "--release-type",
            "patch",
            "--dry-run",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"recovery prepare failed (rc={result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    with open("/tmp/release_info.json", encoding="utf-8") as f:
        info = json.load(f)
    assert info["release_tag"] == "v26.6.2.1-stable"
    assert info["create_new_release"] is False


def test_prepare_recovers_already_released_commit(tmp_path):
    """A rerun that keeps the original commit SHA degrades to recovery.

    ``auto_releases.yml`` dispatches ``ref=<commit_sha>``, and GitHub's "Re-run
    failed jobs" replays the release matrix with that same SHA (AutoReleaseInfo
    is not recomputed) even after the first attempt already pushed the release
    tag. With no *newer* release tag on the branch this is not out-of-order:
    the tag at this commit is this run's own tag, so ``prepare`` must recover
    (``create_new_release=false``) rather than re-enter the creation/merge path.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    # Anchor commit the release version-file githash points at, so the strict
    # tweak (commits since githash) is computable — here one commit -> tweak 1.
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Anchor commit (previous release)")
    anchor = _head_sha(repo)

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, anchor), encoding="utf-8"
    )
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Base release commit")
    # The release for this commit was already created (tagged) on a previous
    # attempt; the rerun dispatches the SAME raw SHA, not the tag name.
    commit_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    git("tag", "-a", "v26.6.2.1-stable", "-m", "Release v26.6.2.1-stable")
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)
    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    result = subprocess.run(
        [
            sys.executable,
            script,
            "--prepare-release-info",
            "--ref",
            commit_sha,  # raw SHA of an already-released commit (the rerun case)
            "--release-type",
            "patch",
            "--dry-run",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"rerun recovery prepare failed (rc={result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    with open("/tmp/release_info.json", encoding="utf-8") as f:
        info = json.load(f)
    assert info["release_tag"] == "v26.6.2.1-stable"
    assert info["create_new_release"] is False


def test_prepare_refuses_out_of_order_commit(tmp_path):
    """A commit ref that is behind the branch tip's release must fail.

    The branch tip's version file already describes a newer release
    (``26.6.3`` here) than the dispatched commit (``26.6.2``), so ``prepare``
    must refuse it rather than create a release from a stale commit. The
    decision reads the branch-tip version file, not release tags. Re-publishing
    an existing release is done by passing its tag.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    # Anchor commit the release version-file githash points at, so the strict
    # tweak (commits since githash) is computable — here one commit -> tweak 1.
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Anchor commit (previous release)")
    anchor = _head_sha(repo)

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, anchor), encoding="utf-8"
    )
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Base release commit")
    # The stale commit we will dispatch — it predates the latest release tag.
    commit_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    # Advance the branch and bump the version file so the branch tip is a newer
    # release (26.6.3) than the dispatched commit (26.6.2). The githash points at
    # the anchor too, so reading the branch-tip version computes a real tweak.
    later_versions = (
        _VERSIONS_CONTENT.replace("VERSION_PATCH 2", "VERSION_PATCH 3")
        .replace("26.6.2.1", "26.6.3.1")
        .replace("0" * 40, anchor)
    )
    (repo / _VERSIONS_FILE).write_text(later_versions, encoding="utf-8")
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Later commit (bump to 26.6.3)")
    git("tag", "-a", "v26.6.3.1-stable", "-m", "Release v26.6.3.1-stable")
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)
    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    result = subprocess.run(
        [
            sys.executable,
            script,
            "--prepare-release-info",
            "--ref",
            commit_sha,
            "--release-type",
            "patch",
            "--dry-run",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, "out-of-order release should have failed"
    assert "out-of-order release" in (result.stdout + result.stderr)


def test_prepare_refuses_stale_commit_even_when_it_is_a_tagged_release(tmp_path):
    """A bare SHA of an older *tagged* release is still out-of-order, not recovery.

    Recovery is expressed by the ref being a release *tag name*; passing the raw
    commit that an older release tag points at must not be mistaken for recovery
    of that release. The branch tip is a newer release (``26.6.3``) than the
    dispatched commit (``26.6.2``), so ``prepare`` must refuse it as out-of-order
    rather than re-publish the stale ``v26.6.2.1-stable`` sitting at that commit.
    This mirrors dispatching e.g. the commit behind an existing ``v25.8.24.21-lts``.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    # Anchor commit the release version-file githash points at, so the strict
    # tweak (commits since githash) is computable — here one commit -> tweak 1.
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Anchor commit (previous release)")
    anchor = _head_sha(repo)

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, anchor), encoding="utf-8"
    )
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Base release commit")
    # The stale commit already carries its own (older) release tag; we will
    # dispatch it by raw SHA, which must NOT be read as recovery of that tag.
    commit_sha = _head_sha(repo)
    git("tag", "-a", "v26.6.2.1-stable", "-m", "Release v26.6.2.1-stable")
    # Advance the branch and bump the version file so the tip is a newer release
    # (26.6.3) than the dispatched commit (26.6.2), plus a tag for realism. The
    # githash points at the anchor too, so the branch-tip tweak is computable.
    later_versions = (
        _VERSIONS_CONTENT.replace("VERSION_PATCH 2", "VERSION_PATCH 3")
        .replace("26.6.2.1", "26.6.3.1")
        .replace("0" * 40, anchor)
    )
    (repo / _VERSIONS_FILE).write_text(later_versions, encoding="utf-8")
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Later commit (bump to 26.6.3)")
    git("tag", "-a", "v26.6.3.1-stable", "-m", "Release v26.6.3.1-stable")
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)
    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    result = subprocess.run(
        [
            sys.executable,
            script,
            "--prepare-release-info",
            "--ref",
            commit_sha,  # raw SHA of an older tagged release, not the tag name
            "--release-type",
            "patch",
            "--dry-run",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, "stale tagged commit should have failed"
    assert "out-of-order release" in (result.stdout + result.stderr)


def test_prepare_creates_from_branch_ref(tmp_path):
    """A branch ref whose tip is after the latest release tag creates the next
    release — it is never treated as out-of-order, even if a version file lags.

    The branch tip is a commit past ``v26.6.1.1-stable``; dispatching the branch
    (not a tag/SHA) must set ``create_new_release=true``.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(_VERSIONS_CONTENT, encoding="utf-8")
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Previous release commit")
    git("tag", "-a", "v26.6.1.1-stable", "-m", "Release v26.6.1.1-stable")
    prev = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    # Point the version-file githash at the previous release so the tweak counts
    # real commits on top of it (two here) — a non-empty patch release (26.6.2.2),
    # not the empty tweak==1 case that prepare refuses.
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, prev), encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Point version githash at previous release")
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "New commit to release")
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)
    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    result = subprocess.run(
        [
            sys.executable,
            script,
            "--prepare-release-info",
            "--ref",
            "26.6",
            "--release-type",
            "patch",
            "--dry-run",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"branch release prepare failed (rc={result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    with open("/tmp/release_info.json", encoding="utf-8") as f:
        info = json.load(f)
    assert info["release_tag"] == "v26.6.2.2-stable"
    assert info["create_new_release"] is True


def test_prepare_fails_closed_on_stale_branch_version_file(tmp_path):
    """A branch ref whose tip version file still describes an already-published
    release must fail closed, not mint a colliding tag.

    The post-release version bump never landed on the branch, so the tip still
    describes ``v26.6.2.1-stable`` — a tag that already exists at an earlier
    commit. ``prepare`` must refuse with a clear "version file is stale" error
    rather than assert or re-create the existing release at a different commit.
    Detecting the wider "computed release is below the branch's latest" case
    needs a release-tag scan, which the release job deliberately avoids; this
    guards the collision case that a targeted tag check can see.
    """
    pytest.importorskip("boto3")  # create_release.py imports s3_helper -> boto3

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=repo, check=True, capture_output=True, text=True
        )

    git("init", "-q", "-b", "26.6")
    git("config", "user.email", "robot@clickhouse.com")
    git("config", "user.name", "robot-clickhouse")
    git("config", "commit.gpgsign", "false")
    git("config", "tag.gpgsign", "false")

    (repo / "cmake").mkdir()
    (repo / _VERSIONS_FILE).write_text(_VERSIONS_CONTENT, encoding="utf-8")
    (repo / "src" / "Storages" / "System").mkdir(parents=True)
    (repo / _CONTRIBUTORS_FILE).write_text(
        "const char * auto_contributors[] {\n    nullptr};\n", encoding="utf-8"
    )
    git("add", "-A")
    git("commit", "-q", "-m", "Base release commit")
    # This commit's release was already published as v26.6.2.1-stable.
    git("tag", "-a", "v26.6.2.1-stable", "-m", "Release v26.6.2.1-stable")
    base_sha = _head_sha(repo)
    # A later commit lands, but the post-release version bump did NOT: the tip's
    # version file still says 26.6.2.1 (its githash points at the base commit, so
    # the tweak is a computable 1), so prepare computes the already-used tag
    # v26.6.2.1-stable at a different (tip) commit.
    (repo / _VERSIONS_FILE).write_text(
        _VERSIONS_CONTENT.replace("0" * 40, base_sha), encoding="utf-8"
    )
    (repo / "README.md").write_text("clickhouse\n", encoding="utf-8")
    git("add", "-A")
    git("commit", "-q", "-m", "Later commit; version bump not applied")
    git("remote", "add", "origin", str(repo))
    git("fetch", "-q", "origin")

    os.symlink(os.path.join(REPO_ROOT, "ci"), repo / "ci")
    os.symlink(os.path.join(REPO_ROOT, "tests"), repo / "tests")
    script = str(repo / "ci" / "jobs" / "scripts" / "create_release.py")

    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh_stub = bindir / "gh"
    gh_stub.write_text("#!/bin/sh\necho '[]'\n", encoding="utf-8")
    gh_stub.chmod(0o755)
    env = {
        **os.environ,
        "PYTHONPATH": REPO_ROOT,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_REPOSITORY": "test/clickhouse",
    }

    result = subprocess.run(
        [
            sys.executable,
            script,
            "--prepare-release-info",
            "--ref",
            "26.6",
            "--release-type",
            "patch",
            "--dry-run",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, "stale branch version file should have failed"
    assert "is stale" in (result.stdout + result.stderr)


# --- ReleaseInfo._enqueue_release_pr (merge_prs helper) ----------------------


def _make_release_info(cr, **overrides):
    kwargs = dict(
        version="26.5.6.64",
        release_type="patch",
        release_tag="v26.5.6.64-stable",
        release_branch="26.5",
        commit_sha="deadbeef",
        latest=False,
        codename="stable",
    )
    kwargs.update(overrides)
    return cr.ReleaseInfo(**kwargs)


def test_enqueue_release_pr_transient_gh_failure_is_best_effort(monkeypatch):
    """A transient `gh pr view` failure must not raise (the release is already
    published) — return False so merge_prs only warns, and never enqueue."""
    cr = _create_release_module()
    ri = _make_release_info(cr)
    monkeypatch.setattr(cr.Shell, "get_output", staticmethod(lambda *a, **k: ""))
    enqueued = []
    monkeypatch.setattr(
        cr.Git,
        "enqueue_pull_request",
        staticmethod(lambda *a, **k: enqueued.append(1) or True),
    )
    assert ri._enqueue_release_pr("https://x/pull/111", "ChangeLog", False) is False
    assert not enqueued


def test_enqueue_release_pr_skips_already_merged(monkeypatch):
    """A non-OPEN (e.g. already merged) PR is a no-op, not an enqueue."""
    cr = _create_release_module()
    ri = _make_release_info(cr)
    monkeypatch.setattr(cr.Shell, "get_output", staticmethod(lambda *a, **k: "MERGED"))
    enqueued = []
    monkeypatch.setattr(
        cr.Git,
        "enqueue_pull_request",
        staticmethod(lambda *a, **k: enqueued.append(1) or True),
    )
    assert ri._enqueue_release_pr("https://x/pull/111", "ChangeLog", False) is True
    assert not enqueued


def test_enqueue_release_pr_open_enqueues(monkeypatch):
    """An OPEN PR is enqueued (no status is force-set — CH Inc sync runs on its
    own; the PR is opened early enough to complete by enqueue time)."""
    cr = _create_release_module()
    ri = _make_release_info(cr)
    monkeypatch.setattr(cr.Shell, "get_output", staticmethod(lambda *a, **k: "OPEN"))
    enq = {}
    monkeypatch.setattr(
        cr.Git,
        "enqueue_pull_request",
        staticmethod(lambda pr, repo, **k: enq.update(pr=pr) or True),
    )
    assert ri._enqueue_release_pr("https://x/pull/111", "ChangeLog", False) is True
    assert enq["pr"] == 111
