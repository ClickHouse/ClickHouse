#!/usr/bin/env python3
"""Check that the URLs added by a change are reachable.

In CI (`broken_links` in ci/jobs/check_style.py) the added lines are taken from the PR diff via
the GitHub API. Locally, pass a unified diff on stdin:

    git diff master... | python3 ci/jobs/scripts/check_style/check_broken_links.py

Only the links that point to a missing resource are reported: a final status of 404 or 410, a
persistent 5xx, or a host that cannot be resolved or connected to. Other 4xx statuses, which
mostly mean that the server refuses robots or anonymous clients (401, 403, ...), are not evidence
of a dead link. Every failure is retried a few times with a backoff before it is reported, and so
is a rate limited (429) request. The links of a host that keeps rate limiting are reported as
unchecked in the log, unless they return 404 or 410.
"""

import ipaddress
import json
import re
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

IGNORE_FILE = Path(__file__).parent / "broken_links_ignore.txt"

ATTEMPTS = 4
RETRY_BACKOFF_SECONDS = 2
MAX_RETRY_AFTER_SECONDS = 60
TIMEOUT_SECONDS = 20
THREADS = 16
# Sites like GitHub and the Web Archive throttle or refuse bursts of requests from one client.
REQUESTS_PER_HOST = 2

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

# Test data and third-party code are full of fake or placeholder URLs.
EXCLUDED_PATH_PREFIXES = ("contrib/", "tests/")
EXCLUDED_PATH_PATTERNS = ("/tests/", "/gtest", "/examples/")

# In documentation, bare URLs are mostly placeholders in code blocks; check only the actual links.
DOCS_PATH_PREFIX = "docs/"
DOCS_LINK_PREFIXES = ("](", 'href="', "href='", "<")

# The base URL of an apt repository is not a page: apt only requests the paths below it.
APT_SOURCE_RE = re.compile(r"\bdeb(?:-src)?\s+(?:\[[^\]]*\]\s+)?$")

BROKEN_STATUSES = {404, 410}
DEFINITE_ERRORS = {f"HTTP {status}" for status in BROKEN_STATUSES}

URL_RE = re.compile(r"https?://[^\s\"'<>`\\^{}|\[\]]+")
TEMPLATE_MARKERS = ("$", "*", "%s", "%d", "...", "{", "}")
TRAILING_PUNCTUATION = ".,;:!?'\""
RESERVED_SUFFIXES = (
    ".example",
    ".invalid",
    ".local",
    ".localhost",
    ".test",
    ".internal",
)
RESERVED_HOSTS = ("localhost", "example.com", "example.org", "example.net")
SAME_REPO_FILE_RE = re.compile(
    r"^https://github\.com/ClickHouse/ClickHouse/(?:blob|tree|raw)/master/([^?#]+)"
)


def trim_url(url):
    while url and url[-1] in TRAILING_PUNCTUATION + ")":
        if url[-1] == ")" and url.count("(") >= url.count(")"):
            break
        url = url[:-1]
    return url


def extract_urls(path, line):
    is_docs = path.startswith(DOCS_PATH_PREFIX)
    urls = []
    for match in URL_RE.finditer(line):
        prefix = line[: match.start()]
        if is_docs and not prefix.endswith(DOCS_LINK_PREFIXES):
            continue
        if APT_SOURCE_RE.search(prefix):
            continue
        url = trim_url(match.group(0))
        if url:
            urls.append(url)
    return urls


def is_checkable(url):
    if any(marker in url for marker in TEMPLATE_MARKERS):
        return False
    parsed = urllib.parse.urlsplit(url)
    if "@" in parsed.netloc:
        return False
    host = (parsed.hostname or "").lower()
    if "." not in host:
        return False
    try:
        ipaddress.ip_address(host)
        return False
    except ValueError:
        pass
    if host.endswith(RESERVED_SUFFIXES):
        return False
    if any(host == h or host.endswith("." + h) for h in RESERVED_HOSTS):
        return False
    return True


def is_excluded_path(path):
    return path.startswith(EXCLUDED_PATH_PREFIXES) or any(
        pattern in "/" + path for pattern in EXCLUDED_PATH_PATTERNS
    )


def load_ignore_list():
    ignored = []
    with open(IGNORE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                ignored.append(line)
    return ignored


def added_lines_from_unified_diff(text):
    """Yield (path, line) for every added line of a `git diff` output."""
    path = None
    for line in text.splitlines():
        if line.startswith("+++ "):
            target = line[4:]
            path = target[2:] if target.startswith("b/") else None
        elif line.startswith("+") and path is not None:
            yield path, line[1:]


def added_lines_from_patch(path, patch):
    """Yield (path, line) for every added line of a per-file patch from the GitHub API."""
    for line in patch.splitlines():
        if line.startswith("+"):
            yield path, line[1:]


def collect_urls(added_lines):
    """{url: first location `path: line`} of the URLs to check among `added_lines`."""
    ignored = load_ignore_list()
    urls = {}
    for path, line in added_lines:
        if is_excluded_path(path):
            continue
        for url in extract_urls(path, line):
            if url in urls or not is_checkable(url):
                continue
            if any(url.startswith(prefix) for prefix in ignored):
                continue
            # A link to a file of this repository that the change itself adds.
            same_repo = SAME_REPO_FILE_RE.match(url)
            if same_repo and Path(urllib.parse.unquote(same_repo.group(1))).exists():
                continue
            urls[url] = path
    return urls


class RateLimited(Exception):
    def __init__(self, retry_after):
        self.retry_after = retry_after


def probe(url):
    """One attempt. Returns None if the link is fine, otherwise the error."""
    request = urllib.request.Request(
        urllib.parse.urldefrag(url).url,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS):
            return None
    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = e.headers.get("Retry-After", "")
            raise RateLimited(int(retry_after) if retry_after.isdigit() else 0)
        # Other 4xx statuses are about the request (e.g. a server refusing robots or
        # anonymous clients), not about the page being gone.
        if e.code in BROKEN_STATUSES or e.code >= 500:
            return f"HTTP {e.code}"
        return None
    except urllib.error.URLError as e:
        if isinstance(e.reason, socket.gaierror):
            return f"cannot resolve host ({e.reason})"
        return f"cannot connect ({e.reason})"
    except (TimeoutError, socket.timeout):
        return "timeout"
    except Exception as e:  # e.g. `http.client.RemoteDisconnected`, SSL errors
        return f"{type(e).__name__}: {e}"


host_semaphores = {}
# Hosts that answered 429: some of them (e.g. the Web Archive) then refuse connections for a while.
rate_limiting_hosts = set()
hosts_lock = threading.Lock()


def host_semaphore(host):
    with hosts_lock:
        return host_semaphores.setdefault(host, threading.Semaphore(REQUESTS_PER_HOST))


def check_url(url):
    """Returns None if the link is fine, otherwise the error of the last attempt."""
    host = urllib.parse.urlsplit(url).hostname
    with host_semaphore(host):
        error = check_url_with_retries(url, host)
    if (
        error is not None
        and host in rate_limiting_hosts
        and error not in DEFINITE_ERRORS
    ):
        # The status of the link is unknown, not bad.
        print(f"Not checked, {host} rate limits requests: {url} - {error}")
        return None
    return error


def check_url_with_retries(url, host):
    error = None
    delay = 0
    for attempt in range(ATTEMPTS):
        if attempt:
            time.sleep(max(delay, RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1)))
        try:
            error = probe(url)
        except RateLimited as e:
            with hosts_lock:
                rate_limiting_hosts.add(host)
            error = "HTTP 429"
            delay = min(e.retry_after, MAX_RETRY_AFTER_SECONDS)
            continue
        if error is None:
            return None
        delay = 0
    return error


def check_urls(urls):
    """Returns a list of `path: url - error` for the broken links among `urls`."""
    url_list = list(urls)
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        errors = list(executor.map(check_url, url_list))
    return [
        f"{urls[url]}: {url} - {error}"
        for url, error in zip(url_list, errors)
        if error is not None
    ]


def report(broken):
    if not broken:
        return ""
    return (
        "Broken links in the added lines:\n"
        + "\n".join(broken)
        + "\nReplace them with the new location of the page or with a Web Archive snapshot "
        "(https://web.archive.org/). If a link is a false positive, add it to "
        f"{IGNORE_FILE.relative_to(Path(__file__).parents[4])}."
    )


def check_pr(repo_name, pr_number):
    """Check the links added by the PR. Returns "" on success or an error message."""

    from ci.praktika.gh import GH

    out = GH.get_output_with_retries(
        f"gh api repos/{repo_name}/pulls/{pr_number}/files --paginate "
        "--jq '.[] | {filename, patch} | @json'",
        verbose=True,
        strict=True,
    )
    added_lines = []
    for line in out.splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("patch") is None:
            # GitHub omits the patch of binary files and of very large diffs.
            print(f"No patch for {entry['filename']}, its links are not checked")
            continue
        added_lines.extend(added_lines_from_patch(entry["filename"], entry["patch"]))
    urls = collect_urls(added_lines)
    print(f"Checking {len(urls)} added links")
    return report(check_urls(urls))


def main():
    urls = collect_urls(added_lines_from_unified_diff(sys.stdin.read()))
    print(f"Checking {len(urls)} added links")
    message = report(check_urls(urls))
    if message:
        print(message)
        sys.exit(1)


if __name__ == "__main__":
    main()
