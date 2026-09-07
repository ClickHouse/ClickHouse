"""Single source of truth for the release build artifacts `CreateRelease`
publishes to and downloads from S3 for a given release branch and version.

Deliberately stdlib-only so it can be imported both from the praktika world
(`create_release.py`'s `PackageDownloader`, which produces/downloads the
artifacts) and from the praktika-free `tests/ci` world (`auto_release.py`'s
`AutoReleases` gate, which refuses to select a commit whose artifacts are not
yet in S3). Keeping the package / build-job / filename contract in one place
stops the producer and the checker from drifting apart — a drift would only
surface at the next scheduled `AutoReleases` run, never in regular PR CI.
"""

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


def s3_release_prefix(release: str) -> str:
    """The S3 key prefix release artifacts live under for this branch."""
    return f"REFs/{release}"


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


def iter_macos_objects():
    """Yield `(package_arch, job_name)` for each per-arch macOS build. The S3
    object basename is always `MACOS_S3_OBJECT`."""
    for package_arch in PACKAGE_ARCHS:
        yield package_arch, darwin_job_name(package_arch)


def expected_s3_objects(version: str):
    """`{job_name: set(object_basenames)}` — every object `CreateRelease`
    downloads from `<s3_release_prefix>/<commit_sha>/<job_name>/` for this
    version."""
    by_job = {}  # type: dict[str, set]
    for _repo_type, filename, job in iter_package_objects(version):
        by_job.setdefault(job, set()).add(filename)
    for _package_arch, job in iter_macos_objects():
        by_job.setdefault(job, set()).add(MACOS_S3_OBJECT)
    return by_job


def release_build_artifacts_ready(
    s3, release: str, commit_sha: str, version: str
) -> bool:
    """Whether every object `CreateRelease` will download for this commit is
    already present in S3.

    A commit can pass the `AutoReleases` CI checks (`check_wf_completed` + no
    failed statuses) while its release build was deduplicated by the CI cache —
    reported as `skipped`, which is not a *failed* status — so nothing (or only
    part) was uploaded under this commit's own SHA. `CreateRelease` downloads
    each object strictly from `<s3_release_prefix>/<commit_sha>/<job>/<file>`, so
    it would 404 on such a commit.

    Enumerate the *exact* object keys and require every one to exist. A
    directory-level check is too weak: a partial upload, or a build-vs-release
    version mismatch, leaves the dir non-empty yet the exact file absent. Fail
    closed — a single missing object rejects the commit.

    `s3` must expose `list_prefix(prefix) -> iterable of keys` (e.g. the
    `tests/ci` `S3Helper`); it is passed in so this module stays dependency-free.
    """
    prefix = s3_release_prefix(release)
    for job, expected_files in expected_s3_objects(version).items():
        job_prefix = f"{prefix}/{commit_sha}/{job}/"
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
