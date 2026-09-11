#!/usr/bin/env python3
"""
Validate the ClickStack SDK docs conventions that in-app onboarding depends on.

The HyperDX in-app onboarding mirrors the SDK guides under
``docs/clickstack/ingesting-data/sdks/`` from this single source of truth, so it
parses them by convention: a ``<ClickStackIntegrates signals={[...]} />``
component whose ``signals`` prop is the source of truth for which signals a
guide supports, a fixed set of replaceable environment-variable placeholders,
and exact deployment tab titles. This checker enforces those conventions so a
drifting page cannot silently break the app's parsing.

Scope is deliberately narrow -- only the SDK folder -- and the Mintlify docs job
gates it to run only when an SDK page or this script changes (see
``ci/jobs/docs_job_mintlify.py``), so it stays fast and low maintenance.

Checks (one aggregated error list, non-zero exit on any failure):
  1. Each guide has exactly one ``<ClickStackIntegrates signals={[...]} />`` with
     a valid, non-empty ``signals`` prop (values in logs/traces/metrics) and the
     matching import. ``index.mdx`` is exempt -- it is the overview, not a guide.
  2. Every `<UPPER_SNAKE>` placeholder is on the allowlist (ALLOWED_PLACEHOLDERS);
     any other one -- a typo or a new variant -- is drift.
  3. Deployment ``<Tabs>`` use only the canonical exact tab titles.

Run from the docs root: ``python3 ../ci/jobs/scripts/docs/sdk_docs_check.py .``
"""

import difflib
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

# The valid integration signals. A value omitted from a guide's `signals` prop
# renders as unsupported.
ALLOWED_INTEGRATIONS = ["logs", "traces", "metrics"]

# The component that renders the "This guide integrates" indicator, and the
# import path every guide must declare to use it.
INTEGRATES_COMPONENT = "ClickStackIntegrates"
INTEGRATES_IMPORT = "/snippets/components/ClickStackIntegrates/ClickStackIntegrates.jsx"

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

# Replaceable placeholders use the `<UPPER_SNAKE_CASE>` convention. Rather than
# denylisting every known-bad spelling (which lets a brand-new typo through),
# this is an allowlist: any `<UPPER_SNAKE>` token that is NOT one of these is
# reported as drift. So <YOU_OTEL_COLLECTOR_HTTP_ENDPOINT> (typo) or a new
# <YOUR_API_KEY> variant is caught automatically, without listing it here.
#
# Page-specific placeholders (e.g. a JAR name) are allowed too. Add a new
# placeholder here when you introduce one.
ALLOWED_PLACEHOLDERS = {
    "<YOUR_INGESTION_API_KEY>",
    "<YOUR_OTEL_COLLECTOR_HTTP_ENDPOINT>",
    "<MY_SERVICE_NAME>",
    # Page-specific.
    "<APPLICATION_JAR_FILE>",
}

# Any `<UPPER_SNAKE>` token is a placeholder candidate. Requiring an uppercase
# first char keeps JSX tags (<Tabs>, <Info>) and lowercase placeholders like
# <region> out of scope.
PLACEHOLDER_RE = re.compile(r"<[A-Z][A-Z0-9_]*>")

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)
TAB_TITLE_RE = re.compile(r'<Tab\s+title="([^"]*)"')
TABS_BLOCK_RE = re.compile(r"<Tabs>(.*?)</Tabs>", re.DOTALL)
# Matches <ClickStackIntegrates signals={[ ... ]} /> (preferred, array form) and
# captures the array body. The comma-separated string form signals="a,b,c" is
# also accepted for resilience. Either way the captured text is split on commas
# and stripped of quotes/brackets by check_integrates_component.
INTEGRATES_RE = re.compile(
    r"<" + INTEGRATES_COMPONENT + r"\b[^>]*?\bsignals=(?:"
    r"\{\[(?P<array>.*?)\]\}"
    r'|"(?P<string>[^"]*)"'
    r")",
    re.DOTALL,
)


def sdk_pages(docs_root: Path):
    """Yield every SDK page (.mdx) under the SDK folder, sorted."""
    yield from sorted((docs_root / SDK_DIR).glob("*.mdx"))


def check_integrates_component(name, body: str) -> list:
    """Validate the <ClickStackIntegrates signals={[...]} /> usage on a guide."""
    matches = list(INTEGRATES_RE.finditer(body))
    if not matches:
        return [
            f"{name}: missing a `<{INTEGRATES_COMPONENT} signals={{[...]}} />` "
            f"component (a list of {'/'.join(ALLOWED_INTEGRATIONS)}; omit a value "
            "it does not support). In-app onboarding parses `signals` to know "
            "which signals the guide integrates."
        ]
    errors = []
    if len(matches) > 1:
        errors.append(
            f"{name}: more than one `<{INTEGRATES_COMPONENT} />`; use exactly one"
        )
    if f"'{INTEGRATES_IMPORT}'" not in body and f'"{INTEGRATES_IMPORT}"' not in body:
        errors.append(
            f"{name}: `<{INTEGRATES_COMPONENT} />` used without importing it; add "
            f"`import {{ {INTEGRATES_COMPONENT} }} from '{INTEGRATES_IMPORT}'`"
        )

    # signals is captured from whichever form matched (array or string); split
    # on commas and strip quotes/brackets/whitespace from each value.
    raw = matches[0].group("array")
    if raw is None:
        raw = matches[0].group("string")
    signals = [
        v.strip().strip("'\"")
        for v in raw.split(",")
        if v.strip()
    ]
    if not signals:
        errors.append(
            f"{name}: `<{INTEGRATES_COMPONENT} />` has an empty `signals`; list at "
            f"least one of {'/'.join(ALLOWED_INTEGRATIONS)}"
        )
    for value in signals:
        if value not in ALLOWED_INTEGRATIONS:
            errors.append(
                f"{name}: unknown integration {value!r} in `signals` "
                f"(allowed: {', '.join(ALLOWED_INTEGRATIONS)})"
            )
    return errors


def _closest_allowed(token: str):
    """Return the allowed placeholder most similar to `token`, for a hint."""
    matches = difflib.get_close_matches(token, ALLOWED_PLACEHOLDERS, n=1, cutoff=0.6)
    return matches[0] if matches else None


def check_placeholders(name, body: str) -> list:
    errors = []

    # Allowlist: any <UPPER_SNAKE> placeholder not in the allowed set is drift.
    # This catches new typos/variants without enumerating them.
    for token in sorted(set(PLACEHOLDER_RE.findall(body))):
        if token in ALLOWED_PLACEHOLDERS:
            continue
        hint = _closest_allowed(token)
        suffix = f"; did you mean `{hint}`?" if hint else (
            "; add it to ALLOWED_PLACEHOLDERS if it is intentional"
        )
        errors.append(f"{name}: unexpected placeholder `{token}`{suffix}")

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
        body = src[m.end():]

        errors += check_placeholders(name, body)
        if page.name == OVERVIEW_PAGE:
            continue
        errors += check_integrates_component(name, body)
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
