"""Single source of truth for the release build artifacts `CreateRelease`
publishes to and downloads from S3 for a given release branch and version.

Kept to the stdlib apart from one lazy praktika import (see `s3_commit_prefix`),
so it stays importable from both consumers under their own minimal runtime
`PYTHONPATH`: `create_release.py`'s `PackageDownloader`, which downloads the
artifacts, and `auto_release_job.py`'s `AutoReleases` gate, which refuses to
select a commit whose artifacts are not yet in S3. Keeping the prefix /
build-job / filename contract in one place stops the producer and the checker
from drifting apart — a drift would only surface at the next scheduled
`AutoReleases` run, never in regular PR CI.
"""

import subprocess

# The six packages built for every release, per arch.
PACKAGES = (
    "clickhouse-client",
    "clickhouse-common-static",
    "clickhouse-common-static-dbg",
    "clickhouse-keeper",
    "clickhouse-keeper-dbg",
    "clickhouse-server",
)

PACKAGE_ARCHS = ("amd", "arm")

# The macOS binary is stored under the darwin build-job dir as this fixed,
# version-less object name.
MACOS_S3_OBJECT = "clickhouse"

MACOS_SIGNED_S3_OBJECT = "clickhouse-macos.zip"

SIGN_MACOS_JOB_SCRIPT = "ci/jobs/sign_macos_binary.py"

# The workflow that builds a release branch's artifacts.
RELEASE_CI_WORKFLOW = "ReleaseBranchCI"

PRAKTIKA_ENVIRONMENT_SCRIPT = "ci/praktika/_environment.py"

# Marker for the line in praktika's `_Environment.get_s3_prefix_static` that
# appends the workflow-name segment to the artifact prefix - on `master` since
# 2026-08-31 (#110081). It is also present in the earlier praktika variant that
# guarded the append behind a predicate exempting only `pr`/`main`/`master`
# workflow names, which `ReleaseBranchCI` never matched, and absent from every
# variant that does not segment at all. So it predicts the release-branch
# layout in every praktika version.
WORKFLOW_NAME_IN_S3_PREFIX_MARKER = "normalize_string(workflow_name)"


def commit_prefixes_artifacts_by_workflow(commit_sha: str) -> bool:
    """Whether this commit's `ReleaseBranchCI` inserts a workflow-name segment
    into the S3 artifact prefix, i.e. uploads to
    `REFs/<branch>/<sha>/releasebranchci/<job>/` rather than
    `REFs/<branch>/<sha>/<job>/`.

    Same reasoning as `commit_has_macos_signing`: `ReleaseBranchCI` is a `push`
    workflow, so a release branch uploads with its **own** praktika, while
    `AutoReleases` and `CreateRelease` read from `master`. Branches cut before
    praktika started prefixing artifacts by workflow name upload without the
    segment and branches cut after it upload with it, so the layout is a
    property of the release commit - read it off that commit's tree.

    Raises when the commit or its praktika is not present locally rather than
    assuming either layout: guessing picks the wrong prefix and turns a
    releasable commit into a 404 (or, in the `AutoReleases` gate, silently
    rejects every commit on the branch)."""
    resolved = subprocess.run(
        ["git", "cat-file", "-e", f"{commit_sha}^{{commit}}"],
        capture_output=True,
    )
    if resolved.returncode != 0:
        raise RuntimeError(
            f"Commit [{commit_sha}] is not present in the local repository, cannot"
            f" tell which S3 artifact layout it uploads with"
        )
    blob = subprocess.run(
        ["git", "show", f"{commit_sha}:{PRAKTIKA_ENVIRONMENT_SCRIPT}"],
        capture_output=True,
    )
    if blob.returncode != 0:
        raise RuntimeError(
            f"Commit [{commit_sha}] has no [{PRAKTIKA_ENVIRONMENT_SCRIPT}], cannot"
            f" tell which S3 artifact layout it uploads with"
        )
    return WORKFLOW_NAME_IN_S3_PREFIX_MARKER in blob.stdout.decode(
        "utf-8", errors="replace"
    )


def s3_commit_prefix(release: str, commit_sha: str) -> str:
    """The S3 key prefix this release commit's build artifacts live under, i.e.
    everything up to (not including) the per-job dir.

    For a commit that segments by workflow, ask praktika where `ReleaseBranchCI`
    publishes instead of spelling the prefix out, so a later change of the
    layout is picked up here for free - the same reasoning as
    `performance_tests.master_build_links`. That answer is `master`'s praktika,
    which matches a branch cut after the layout last changed; a branch that is
    still supported across a *further* layout change would need its own
    praktika, and would surface as the `AutoReleases` artifact gate going red
    rather than as a wrong publish.

    Imported lazily so the module keeps working for callers that do not have
    praktika importable, mirroring praktika's own `get_s3_prefix_static`."""
    if not commit_prefixes_artifacts_by_workflow(commit_sha):
        # Pre-#110081 layout: the per-job dirs sit directly under the commit.
        return f"REFs/{release}/{commit_sha}"
    from ci.praktika._environment import _Environment

    return _Environment.get_s3_prefix_static(
        pr_number=0,
        branch=release,
        sha=commit_sha,
        workflow_name=RELEASE_CI_WORKFLOW,
    )


def deb_tgz_arch(package_arch: str) -> str:
    """Arch suffix used in `.deb`/`.tgz` filenames."""
    return "amd64" if package_arch == "amd" else "arm64"


def rpm_arch(package_arch: str) -> str:
    """Arch suffix used in `.rpm` filenames."""
    return "x86_64" if package_arch == "amd" else "aarch64"


def build_job_name(package_arch: str) -> str:
    """CI job dir that holds the deb/rpm/tgz packages for this arch."""
    return f"build_{package_arch}_release"


def darwin_job_name(package_arch: str) -> str:
    """CI job dir that holds the macOS binary for this arch."""
    return f"build_{package_arch}_darwin"


def iter_package_objects(version: str):
    """Yield `(repo_type, filename, job_name)` for every deb/rpm/tgz/tgz.sha512
    object across amd+arm. `repo_type` is one of `"deb"`, `"rpm"`, `"tgz"`."""
    for package_arch in PACKAGE_ARCHS:
        job = build_job_name(package_arch)
        deb = deb_tgz_arch(package_arch)
        rpm = rpm_arch(package_arch)
        for package in PACKAGES:
            yield "deb", f"{package}_{version}_{deb}.deb", job
            yield "rpm", f"{package}-{version}.{rpm}.rpm", job
            tgz = f"{package}-{version}-{deb}.tgz"
            yield "tgz", tgz, job
            yield "tgz", f"{tgz}.sha512", job


def sign_macos_job_name(package_arch: str) -> str:
    """CI job dir that holds the signed macOS zip for this arch."""
    return f"sign_macos_binary_{package_arch}_darwin"


def iter_macos_objects():
    """Yield `(package_arch, job_name)` for each per-arch macOS build. The S3
    object basename is always `MACOS_S3_OBJECT`."""
    for package_arch in PACKAGE_ARCHS:
        yield package_arch, darwin_job_name(package_arch)


def iter_macos_signed_objects():
    """Yield `(package_arch, job_name)` for each per-arch signed macOS zip. The
    S3 object basename is always `MACOS_SIGNED_S3_OBJECT`."""
    for package_arch in PACKAGE_ARCHS:
        yield package_arch, sign_macos_job_name(package_arch)


def commit_has_macos_signing(commit_sha: str) -> bool:
    """Whether this commit's tree carries the macOS signing job, i.e. whether its
    `ReleaseBranchCI` produces the signed zips.

    `ReleaseBranchCI` is a `push` workflow, so a release branch runs its **own**
    workflow definition, while `AutoReleases` and `CreateRelease` run from
    `master`. A release branch cut before signing existed therefore never uploads
    the zips, and requiring them unconditionally would reject every commit on it.
    Read the requirement off the release commit itself instead.

    Raises when `commit_sha` is not present locally rather than assuming the
    absence of signing - guessing would silently publish a release without its
    signed assets."""
    resolved = subprocess.run(
        ["git", "cat-file", "-e", f"{commit_sha}^{{commit}}"],
        capture_output=True,
    )
    if resolved.returncode != 0:
        raise RuntimeError(
            f"Commit [{commit_sha}] is not present in the local repository, cannot"
            f" tell whether it produces signed macOS artifacts"
        )
    present = subprocess.run(
        ["git", "cat-file", "-e", f"{commit_sha}:{SIGN_MACOS_JOB_SCRIPT}"],
        capture_output=True,
    )
    return present.returncode == 0


def expected_s3_objects(version: str, with_signed_macos: bool):
    """`{job_name: set(object_basenames)}` — every object `CreateRelease`
    downloads from `<s3_commit_prefix>/<job_name>/` for this version."""
    by_job = {}  # type: dict[str, set]
    for _repo_type, filename, job in iter_package_objects(version):
        by_job.setdefault(job, set()).add(filename)
    for _package_arch, job in iter_macos_objects():
        by_job.setdefault(job, set()).add(MACOS_S3_OBJECT)
    if with_signed_macos:
        for _package_arch, job in iter_macos_signed_objects():
            by_job.setdefault(job, set()).add(MACOS_SIGNED_S3_OBJECT)
    return by_job


def release_build_artifacts_ready(
    s3, release: str, commit_sha: str, version: str, with_signed_macos: bool
) -> bool:
    """Whether every object `CreateRelease` will download for this commit is
    already present in S3.

    A commit can pass the `AutoReleases` CI checks (`check_wf_completed` + no
    failed statuses) while its release build was deduplicated by the CI cache —
    reported as `skipped`, which is not a *failed* status — so nothing (or only
    part) was uploaded under this commit's own SHA. `CreateRelease` downloads
    each object strictly from `<s3_commit_prefix>/<job>/<file>`, so it would 404
    on such a commit.

    Enumerate the *exact* object keys and require every one to exist. A
    directory-level check is too weak: a partial upload, or a build-vs-release
    version mismatch, leaves the dir non-empty yet the exact file absent. Fail
    closed — a single missing object rejects the commit.

    `s3` must expose `list_prefix(prefix) -> iterable of keys` (e.g. the
    `ci/tools` `S3Helper`); it is passed in so this module stays dependency-free.
    """
    prefix = s3_commit_prefix(release, commit_sha)
    for job, expected_files in expected_s3_objects(version, with_signed_macos).items():
        job_prefix = f"{prefix}/{job}/"
        present = {key.rsplit("/", 1)[-1] for key in s3.list_prefix(job_prefix)}
        missing = expected_files - present
        if missing:
            print(
                f"Missing release artifacts for [{version}] under "
                f"[s3://.../{job_prefix}]: {sorted(missing)} — the release build "
                f"for this commit was skipped/cached or uploaded partially; not "
                f"releasable"
            )
            return False
    return True
