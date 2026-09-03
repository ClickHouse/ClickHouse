#!/usr/bin/env python3
"""
Validate the ClickStack SDK docs conventions that in-app onboarding depends on.

The HyperDX in-app onboarding mirrors the SDK guides under
``docs/clickstack/ingesting-data/sdks/`` from this single source of truth, so it
parses them by convention: a machine-readable ``clickstack_integrations``
frontmatter key, a fixed set of replaceable environment-variable placeholders,
and exact deployment tab titles. This checker enforces those conventions so a
drifting page cannot silently break the app's parsing.

Scope is deliberately narrow -- only the SDK folder -- and the Mintlify docs job
gates it to run only when an SDK page or this script changes (see
``ci/jobs/docs_job_mintlify.py``), so it stays fast and low maintenance.

Checks (one aggregated error list, non-zero exit on any failure):
  1. ``clickstack_integrations`` frontmatter is present and non-empty, and every
     value is one of logs/traces/metrics. ``index.mdx`` is exempt -- it is the
     overview, not an SDK guide.
  2. No forbidden environment-variable placeholder variants appear (the drifted
     spellings the standardization replaced); the canonical placeholders are the
     only accepted forms.
  3. Deployment ``<Tabs>`` use only the canonical exact tab titles.
  4. The required frontmatter fields are present on every page.

Run from the docs root: ``python3 ../ci/jobs/scripts/docs/sdk_docs_check.py .``
"""

import re
import sys
from pathlib import Path

# The folder this checker owns, relative to the docs root.
SDK_DIR = Path("clickstack/ingesting-data/sdks")

# The overview page is not an SDK guide: it documents no single language and
# carries no deployment tabs or per-guide integration set, so it is exempt from
# the integrations and deployment-tab checks (but not the placeholder check --
# it still shows canonical env-var examples).
OVERVIEW_PAGE = "index.mdx"

# The machine-readable integration signals in-app onboarding reads. Omitting a
# value means the guide does not support that signal.
ALLOWED_INTEGRATIONS = ["logs", "traces", "metrics"]

# Required frontmatter keys on every SDK page (matches the root AGENTS.md
# convention for new docs pages).
REQUIRED_FRONTMATTER = ["title", "description", "slug", "doc_type", "sidebarTitle"]

# The exact deployment tab titles. In-app onboarding selects the deployment tab
# by exact title, so these must not drift. "ClickStack Cloud" is intentionally
# not part of the set yet.
CANONICAL_DEPLOYMENT_TAB_TITLES = ["Managed ClickStack", "ClickStack Open Source"]

# The titles that mark a <Tabs> block as the deployment selector: a block is a
# deployment block if any of its tab titles is a canonical deployment title. Any
# OTHER title inside such a block is then a drift error. Non-deployment tab
# groups (NPM/Yarn, Gunicorn/uWSGI, ...) share no title with this set and are
# left alone.
DEPLOYMENT_TAB_MARKERS = set(CANONICAL_DEPLOYMENT_TAB_TITLES)

# Forbidden placeholder spellings -> the canonical placeholder that replaces
# them. Each entry is (compiled regex, human-readable canonical form). These are
# the exact drift variants the standardization removed; keeping them out is what
# guarantees the app sees one spelling.
FORBIDDEN_PLACEHOLDERS = [
    # Ingestion API key: only <YOUR_INGESTION_API_KEY> is allowed.
    (re.compile(r"\*\*\*YOUR_INGESTION_API_KEY\*\*\*"), "<YOUR_INGESTION_API_KEY>"),
    (re.compile(r"<YOUR_INGESTION_KEY>"), "<YOUR_INGESTION_API_KEY>"),
    # Bare, unbracketed key (but not the bracketed <YOUR_INGESTION_API_KEY> or
    # the header env-var name OTEL_EXPORTER_OTLP_HEADERS). Negative lookbehind
    # for `<` / `_` and lookahead for `>` keep the canonical form and the
    # env-var name from matching.
    (
        re.compile(r"(?<![<A-Z_])YOUR_INGESTION_API_KEY(?![>A-Z_])"),
        "<YOUR_INGESTION_API_KEY>",
    ),
    # OTLP endpoint typo (missing the R in YOUR).
    (
        re.compile(r"<YOU_OTEL_COLLECTOR_HTTP_ENDPOINT>"),
        "<YOUR_OTLP_HTTP_ENDPOINT>",
    ),
]

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)
TAB_TITLE_RE = re.compile(r'<Tab\s+title="([^"]*)"')
TABS_BLOCK_RE = re.compile(r"<Tabs>(.*?)</Tabs>", re.DOTALL)


def sdk_pages(docs_root: Path):
    """Yield every SDK page (.mdx) under the SDK folder, sorted."""
    yield from sorted((docs_root / SDK_DIR).glob("*.mdx"))


def parse_integrations(frontmatter: str):
    """Return the clickstack_integrations values, or None if the key is absent.

    Accepts both the inline form ``clickstack_integrations: [logs, traces]`` and
    the YAML block form::

        clickstack_integrations:
          - logs
          - traces
    """
    inline = re.search(
        r"^clickstack_integrations:\s*\[(.*?)\]\s*$", frontmatter, re.M
    )
    if inline:
        return [v.strip().strip("'\"") for v in inline.group(1).split(",") if v.strip()]

    block = re.search(
        r"^clickstack_integrations:\s*\n((?:[ \t]+-[ \t]*.+\n?)+)",
        frontmatter,
        re.M,
    )
    if block:
        return [
            line.split("-", 1)[1].strip().strip("'\"")
            for line in block.group(1).splitlines()
            if line.strip().startswith("-")
        ]
    return None


def check_frontmatter(name, frontmatter: str) -> list:
    errors = []
    for key in REQUIRED_FRONTMATTER:
        if not re.search(rf"^{key}:\s*\S", frontmatter, re.M):
            errors.append(f"{name}: missing `{key}` frontmatter")
    return errors


def check_integrations(name, frontmatter: str) -> list:
    integrations = parse_integrations(frontmatter)
    if integrations is None:
        return [
            f"{name}: missing `clickstack_integrations` frontmatter "
            f"(a list of {'/'.join(ALLOWED_INTEGRATIONS)}; omit a value it does "
            "not support). In-app onboarding reads this to render the "
            "'This guide integrates' section."
        ]
    if not integrations:
        return [f"{name}: `clickstack_integrations` is empty; list at least one of "
                f"{'/'.join(ALLOWED_INTEGRATIONS)}"]
    errors = []
    for value in integrations:
        if value not in ALLOWED_INTEGRATIONS:
            errors.append(
                f"{name}: unknown integration {value!r} "
                f"(allowed: {', '.join(ALLOWED_INTEGRATIONS)})"
            )
    return errors


def check_placeholders(name, body: str) -> list:
    errors = []
    for pattern, canonical in FORBIDDEN_PLACEHOLDERS:
        if pattern.search(body):
            errors.append(
                f"{name}: found non-standard placeholder matching "
                f"`{pattern.pattern}`; use `{canonical}` instead"
            )
    return errors


def check_deployment_tabs(name, body: str) -> list:
    errors = []
    for block in TABS_BLOCK_RE.finditer(body):
        titles = TAB_TITLE_RE.findall(block.group(1))
        if not any(t in DEPLOYMENT_TAB_MARKERS for t in titles):
            # Not a deployment tab group; leave it alone.
            continue
        for title in titles:
            if title not in DEPLOYMENT_TAB_MARKERS:
                errors.append(
                    f"{name}: deployment `<Tabs>` block has unexpected tab title "
                    f"{title!r}; deployment tabs must use exactly "
                    f"{CANONICAL_DEPLOYMENT_TAB_TITLES}"
                )
    return errors


def main() -> int:
    docs_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    if not (docs_root / "docs.json").is_file():
        print(f"Error: no docs.json in {docs_root}; pass the docs root.")
        return 2

    sdk_root = docs_root / SDK_DIR
    if not sdk_root.is_dir():
        print(f"Error: SDK docs folder not found at {sdk_root}.")
        return 2

    errors = []
    pages = list(sdk_pages(docs_root))
    if not pages:
        print(f"Error: no SDK pages found under {sdk_root}.")
        return 2

    for page in pages:
        name = page.relative_to(docs_root)
        src = page.read_text(encoding="utf-8")
        m = FRONTMATTER_RE.match(src)
        if not m:
            errors.append(f"{name}: no frontmatter block")
            continue
        frontmatter = m.group(1)
        body = src[m.end():]

        errors += check_frontmatter(name, frontmatter)
        errors += check_placeholders(name, body)
        if page.name == OVERVIEW_PAGE:
            continue
        errors += check_integrations(name, frontmatter)
        errors += check_deployment_tabs(name, body)

    if errors:
        print(f"FAIL: {len(errors)} ClickStack SDK docs problem(s):")
        for e in errors:
            print(f"- {e}")
        return 1
    print(f"OK: ClickStack SDK docs checks passed ({len(pages)} pages)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
