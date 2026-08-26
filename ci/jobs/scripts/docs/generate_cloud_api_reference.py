#!/usr/bin/env python3
"""Generate the ClickHouse Cloud API reference documentation from the OpenAPI spec.

The Cloud API reference is backed by the live OpenAPI spec at
`https://api.clickhouse.cloud/v1`. Individual endpoints can be in different
maturity stages (GA, beta, private preview) or be deprecated, but Mintlify only
allows a `tag` badge on navigation *groups*, not on individual OpenAPI operation
entries. As a result a group badge could only be coarse, which is wrong whenever
a group mixes beta and non-beta endpoints.

To get a per-endpoint badge in the sidebar, an operation needs its own `.mdx`
page whose frontmatter carries the operation reference and a `tag` or
`deprecated` flag. This script generates such a page for every operation that
needs a badge; all other (GA) operations are referenced as plain
`"METHOD /path"` strings in the navigation. The mix is deliberate: Mintlify
bulk-appends every spec operation to a group that has an `openapi` field unless
at least one of the group's pages is a string-form operation reference (MDX
pages do not count), so an all-MDX navigation would render the whole tag tree a
second time. The navigation subtree is written as a single fragment
(`docs/products/cloud/api-reference/navigation.json`), and the consuming file
(`products/cloud/navigation.json`, itself `$ref`-ed by `docs.json`) references
that one definition via `$ref` so the navigation is never duplicated.
Localized navigation fragments receive the same endpoint structure while
retaining their translated group names.

The navigation grouping mirrors the spec's `x-tagGroups`/`tags` (the same
hierarchy the hosted Swagger view shows): each tag group is a top-level group;
groups with more than one tag nest every tag as a subgroup, while single-tag
groups list their operations directly.

Maturity is not a structured field in the spec; it is signalled in the operation
`summary`/`description` free text ("This endpoint is in beta.", "private
preview", ...) plus the standard OpenAPI `deprecated` boolean.
`classify_operation` centralises that detection.

Usage:
    generate_cloud_api_reference.py [--spec URL_OR_PATH] [--docs-dir DIR]
                                    [--write | --check]

  --write   Write the generated pages, the navigation fragment and the $ref
            wiring (default).
  --check   Exit non-zero if anything would change (for CI drift detection).
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
import urllib.request
from pathlib import Path

DEFAULT_SPEC = "https://api.clickhouse.cloud/v1"
API_REF_GROUP = "API reference"

# Directory (relative to the docs root) that holds the generated pages and the
# single navigation fragment that defines the whole "API reference" group.
API_REF_DIR = "products/cloud/api-reference"
FRAGMENT_REL = f"{API_REF_DIR}/navigation.json"
LOCALE_DIRS = ("ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh")

# Pinned copy of the spec, committed to the repo. The navigation and the
# generated pages reference this file rather than the live URL so that page
# resolution is unambiguous (GT-translated copies of the spec live at
# `<locale>/_specs/` and would otherwise compete for unqualified references)
# and so that spec changes surface as reviewable diffs / CI drift.
SPEC_PIN = "_specs/cloud-openapi.json"

# Files that consume the fragment via a `$ref` (relative to the consuming
# file's own directory — Mintlify resolves `$ref` relative to the file it
# appears in). `docs.json` is not a consumer: it reaches the group through its
# `$ref` to `products/cloud/navigation.json`.
CONSUMERS = {
    f"{Path(API_REF_DIR).parent}/navigation.json": "./api-reference/navigation.json",
}

# HTTP methods an OpenAPI path item may carry (other keys such as `parameters`
# are not operations). Operations are emitted in the spec's own path/method
# order so the sidebar mirrors the spec.
HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


def load_spec(source: str) -> dict:
    """Load the OpenAPI spec from a URL or a local file path."""
    if re.match(r"^https?://", source):
        # Explicit timeout: this runs in the nightly workflow, and a hung TCP
        # connection would otherwise pin a runner until the workflow timeout.
        with urllib.request.urlopen(source, timeout=60) as response:  # noqa: S310 (trusted URL)
            return json.loads(response.read())
    return json.loads(Path(source).read_text(encoding="utf-8"))


def dir_slug(tag: str) -> str:
    """Kebab-case a tag for use as a directory name (no camelCase splitting)."""
    return re.sub(r"[^a-z0-9]+", "-", tag.lower()).strip("-")


def file_slug(operation_id: str) -> str:
    """Kebab-case an operationId (camelCase aware) for use as a file name."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", operation_id)
    return re.sub(r"[^a-z0-9]+", "-", spaced.lower()).strip("-")


def classify_operation(operation: dict) -> tuple[str | None, bool]:
    """Return (tag, deprecated) for an operation.

    `tag` is the sidebar badge text ("Beta"/"Private preview") or None for GA.
    Maturity is detected from the free-text summary/description because the spec
    exposes no structured field for it.
    """
    text = f"{operation.get('summary', '') or ''} {operation.get('description', '') or ''}".lower()
    tag = None
    if "private preview" in text:
        tag = "Private preview"
    elif re.search(r"\bbeta\b", text):
        tag = "Beta"
    return tag, bool(operation.get("deprecated"))


def iter_operations(spec: dict):
    """Yield operations in spec traversal order.

    Yields dicts with: method, path, tag (OpenAPI tag), badge, deprecated,
    operation_id, summary.
    """
    for path, methods in spec.get("paths", {}).items():
        for method, operation in methods.items():
            if method.lower() not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            tags = operation.get("tags") or []
            badge, deprecated = classify_operation(operation)
            yield {
                "method": method.upper(),
                "path": path,
                "tag": tags[0] if tags else "Other",
                "badge": badge,
                "deprecated": deprecated,
                "operation_id": operation["operationId"],
                "summary": operation.get("summary") or operation["operationId"],
                "description": operation.get("description") or "",
            }


def needs_page(entry: dict) -> bool:
    """Whether an operation needs its own MDX page (i.e. carries a badge)."""
    return bool(entry["badge"] or entry["deprecated"])


# Intra-spec cross-link, e.g. `[Get ClickPipe](#tag/ClickPipes/operation/clickPipeGet)`.
# These anchors resolve on the single-page Swagger view only.
SPEC_LINK_RE = re.compile(r"\]\(#tag/[^/)]+/operation/([A-Za-z0-9_-]+)\)")


def operation_slug_map(spec: dict) -> dict:
    """Map every operationId to the site path of its reference page.

    Badge operations get a generated stub at `file_slug(operationId)`; GA
    operations get a Mintlify-generated page whose slug is the kebab-cased
    summary (e.g. "Get list of available organizations" becomes
    `organization/get-list-of-available-organizations`).
    """
    slugs = {}
    for entry in iter_operations(spec):
        leaf = file_slug(entry["operation_id"]) if needs_page(entry) else dir_slug(entry["summary"])
        slugs[entry["operation_id"]] = f"/{API_REF_DIR}/{dir_slug(entry['tag'])}/{leaf}"
    return slugs


def rewrite_spec_links(text: str, slugs: dict) -> str:
    """Rewrite intra-spec Swagger anchors to the per-endpoint page paths.

    `#tag/<Tag>/operation/<id>` anchors do not exist on the split
    per-endpoint pages, so descriptions copied into a page body would ship
    broken cross-links. Unknown operationIds fail the run rather than
    shipping a link to nowhere.
    """
    def replace(match: re.Match) -> str:
        operation_id = match.group(1)
        if operation_id not in slugs:
            raise SystemExit(f"Description links to unknown operation: {operation_id}")
        return f"]({slugs[operation_id]})"

    return SPEC_LINK_RE.sub(replace, text)


def mdx_escape(text: str) -> str:
    """Escape spec description text for use as MDX body content.

    Bare `{`/`}` outside backtick code spans would be parsed as JSX
    expressions; inside code spans they are literal and must stay unescaped.
    The only raw HTML the descriptions use is `<br />`, which is valid JSX.
    """
    parts = re.split(r"(`[^`]*`)", text)
    return "".join(
        part if part.startswith("`") else part.replace("{", "\\{").replace("}", "\\}")
        for part in parts
    )


def page_content(entry: dict) -> str:
    """Render the MDX page for a single operation.

    The `openapi` reference is qualified with the pinned spec path: the repo
    also carries GT-translated spec copies (`<locale>/_specs/...`), and an
    unqualified `"METHOD /path"` matches those too, so Mintlify may resolve
    the page against a translated copy (descriptions rendered in the wrong
    language, or dropped).

    The page-header description is hidden site-wide by a deliberate rule in
    `_site/styles.css`, so the maturity badge and the endpoint description
    (which leads with the disclaimer, e.g. "This endpoint is in beta. ...")
    are emitted as page body content instead, fenced by AUTOGENERATED
    markers.
    """
    lines = [
        "---",
        f"title: {json.dumps(entry['summary'])}",
        f"openapi: {json.dumps('/' + SPEC_PIN + ' ' + entry['method'] + ' ' + entry['path'])}",
    ]
    if entry["description"]:
        lines.append(f"description: {json.dumps(entry['description'])}")
    if entry["badge"]:
        lines.append(f"tag: {json.dumps(entry['badge'])}")
    if entry["deprecated"]:
        lines.append("deprecated: true")
    lines.append("---")
    lines.append("")
    body = []
    if entry["badge"]:
        # The wrapper's data attribute drives pure-CSS rules in
        # `_site/styles.css` that paint the badge as a pill next to the page
        # title (the slot the native deprecated pill uses) and hide this
        # in-body copy. The <Badge> itself is kept for contexts without the
        # stylesheet (markdown exports, `llms.txt`).
        color = {"Beta": "blue", "Private preview": "purple"}[entry["badge"]]
        body.append(
            f'<span data-endpoint-badge={json.dumps(entry["badge"])}>'
            f'<Badge color="{color}">{entry["badge"]}</Badge></span>'
        )
    if entry["description"]:
        body.append(mdx_escape(entry["description"]))
    if body:
        lines.append("{/* AUTOGENERATED_START */}")
        lines.append("")
        for block in body:
            lines.append(block)
            lines.append("")
        lines.append("{/* AUTOGENERATED_END */}")
        lines.append("")
    return "\n".join(lines)


def build_fragment(spec: dict, entries_by_tag: dict) -> list:
    """Build the "API reference" group object stored as the navigation fragment.

    Each x-tagGroup becomes a top-level group. A group with more than one tag
    nests every tag as a subgroup (mirroring the hosted Swagger view, e.g.
    "Organization" > "Organization"); a single-tag group lists its operations
    directly.
    """
    def page_ref(entry: dict) -> str:
        # String-form refs for GA operations: at least one of these must be
        # present so Mintlify skips bulk auto-population of the openapi group.
        if not needs_page(entry):
            return f"{entry['method']} {entry['path']}"
        return f"{API_REF_DIR}/{dir_slug(entry['tag'])}/{file_slug(entry['operation_id'])}"

    def pages_for(tag: str) -> list:
        return [page_ref(e) for e in entries_by_tag.get(tag, [])]

    groups = []
    covered = set()
    for tag_group in spec.get("x-tagGroups", []):
        tags = tag_group["tags"]
        covered.update(tags)
        if len(tags) == 1:
            pages = pages_for(tags[0])
        else:
            pages = [{"group": tag, "pages": pages_for(tag)} for tag in tags]
        groups.append({"group": tag_group["name"], "pages": pages})

    orphan_tags = sorted(set(entries_by_tag) - covered)
    if orphan_tags:
        raise SystemExit(
            f"OpenAPI tags not present in x-tagGroups (would be dropped from nav): {orphan_tags}"
        )

    # The fragment is the *pages array* only, not the group object: a JSON
    # file whose top level carries an `openapi` key is picked up by Mintlify's
    # OpenAPI-spec scan and produces a validation warning on every build. The
    # group object (with its `openapi` spec binding) lives inline in each
    # consumer, `$ref`-ing this array — the same pattern docs.json uses for
    # `redirects`.
    return groups


def operation_ref_keys(spec: dict) -> dict[str, str]:
    """Map both navigation forms of an operation to its stable method/path key."""
    keys = {}
    for entry in iter_operations(spec):
        operation_key = f"{entry['method']} {entry['path']}"
        badge_ref = (
            f"{API_REF_DIR}/{dir_slug(entry['tag'])}/{file_slug(entry['operation_id'])}"
        )
        for ref in (operation_key, badge_ref):
            previous = keys.get(ref)
            if previous is not None and previous != operation_key:
                raise SystemExit(
                    f"Navigation reference {ref!r} maps to multiple operations: "
                    f"{previous!r} and {operation_key!r}"
                )
            keys[ref] = operation_key
    return keys


def group_page_refs(group: dict, ref_keys: dict[str, str]) -> set[str]:
    """Return normalized leaf page references below one navigation group."""
    pages = group.get("pages")
    if not isinstance(pages, list):
        raise SystemExit(f"Navigation group has no pages list: {group.get('group', group)!r}")

    refs = set()
    for page in pages:
        if isinstance(page, str):
            refs.add(ref_keys.get(page, page))
        elif isinstance(page, dict):
            refs.update(group_page_refs(page, ref_keys))
        else:
            raise SystemExit(f"Unexpected navigation page entry: {page!r}")
    return refs


def localize_fragment(
    generated_groups: list, current_groups: list, ref_keys: dict[str, str]
) -> list:
    """Copy generated navigation while preserving existing translated group names.

    Groups are matched by their shared operations instead of by position or
    rendered reference. This preserves translated names across reordering and
    across maturity changes that switch a navigation leaf between `METHOD /path`
    and a generated badge-page path. A group containing only new
    endpoints keeps the name supplied by the OpenAPI spec until translation
    catches up.
    """
    if not isinstance(generated_groups, list) or not isinstance(current_groups, list):
        raise SystemExit("Cloud API navigation fragments must contain a list of groups")
    if not all(isinstance(group, dict) for group in generated_groups + current_groups):
        raise SystemExit("Cloud API navigation fragments contain a non-group entry")

    current_refs = [group_page_refs(group, ref_keys) for group in current_groups]
    used_current_groups = set()
    localized_groups = []

    for generated_group in generated_groups:
        generated_refs = group_page_refs(generated_group, ref_keys)
        candidates = []
        for index, refs in enumerate(current_refs):
            if index in used_current_groups:
                continue
            overlap = len(generated_refs & refs)
            if overlap:
                candidates.append((overlap, index))

        current_group = None
        if candidates:
            best_overlap = max(overlap for overlap, _ in candidates)
            best_matches = [index for overlap, index in candidates if overlap == best_overlap]
            # A tie means groups were merged or rebucketed. Do not guess which
            # translation to keep; leaving `current_group` unset retains the
            # generated English name.
            if len(best_matches) == 1:
                current_index = best_matches[0]
                used_current_groups.add(current_index)
                current_group = current_groups[current_index]

        localized_group = copy.deepcopy(generated_group)
        if current_group is not None:
            localized_group["group"] = current_group["group"]

        generated_children = [
            page for page in generated_group["pages"] if isinstance(page, dict)
        ]
        if generated_children:
            current_children = []
            if current_group is not None:
                current_children = [
                    page for page in current_group["pages"] if isinstance(page, dict)
                ]
            localized_group["pages"] = localize_fragment(
                generated_children, current_children, ref_keys
            )

        localized_groups.append(localized_group)

    return localized_groups


def match_object_span(text: str, open_brace: int) -> int:
    """Return the index just past the `}` matching the `{` at `open_brace`."""
    depth = 0
    in_string = False
    escaped = False
    for i in range(open_brace, len(text)):
        char = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return i + 1
    raise SystemExit("Unbalanced braces while locating the API reference group")


def rewire_to_ref(text: str, ref_path: str) -> str:
    """Point the consumer's "API reference" group at the pages fragment, once.

    The target form is a one-line group object whose `pages` is a `$ref` to
    the fragment array. A textual splice (rather than re-serializing the whole
    file) keeps every unrelated byte unchanged. Handles the two prior states —
    a bare `{ "$ref": ... }` group (older wiring) and the original multi-line
    inline group — and is idempotent once the target form is in place.
    """
    # The object form pins both the spec and the output directory of the
    # pages Mintlify generates for the string-form operation refs. The
    # explicit directory keeps those pages out of the default `api-reference`
    # namespace, which the locale navs' bare openapi groups also generate
    # into (from the un-pinned live spec) — first writer wins there, so the
    # English pages would otherwise randomly lose their pinned-spec content.
    wired = (
        f'{{ "group": "{API_REF_GROUP}", "openapi": '
        f'{{ "source": "{SPEC_PIN}", "directory": "{API_REF_DIR}" }}, '
        f'"pages": {{ "$ref": "{ref_path}" }} }}'
    )
    if wired in text:
        return text  # already rewired
    prior_forms = [
        f'{{ "group": "{API_REF_GROUP}", "openapi": "{SPEC_PIN}", '
        f'"pages": {{ "$ref": "{ref_path}" }} }}',
        f'{{ "$ref": "{ref_path}" }}',
    ]
    for prior in prior_forms:
        if prior in text:
            return text.replace(prior, wired)
    match = re.search(
        r'(?m)^(?P<indent> *)\{\n *"group": "' + re.escape(API_REF_GROUP) + r'",\n *"openapi":',
        text,
    )
    if match:
        indent = match.group("indent")
        start = match.start() + len(indent)  # position of the opening '{'
        end = match_object_span(text, start)
        return text[:start] + wired + text[end:]
    raise SystemExit(
        f'Could not find the "{API_REF_GROUP}" openapi group nor an existing '
        f'$ref "{ref_path}" to rewire'
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", default=DEFAULT_SPEC, help="OpenAPI spec URL or file path")
    parser.add_argument("--docs-dir", default="docs", help="Path to the docs root")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="Write changes (default)")
    mode.add_argument("--check", action="store_true", help="Fail if anything would change")
    args = parser.parse_args()
    check_only = args.check

    docs_dir = Path(args.docs_dir)
    api_ref_dir = docs_dir / API_REF_DIR

    spec = load_spec(args.spec)
    op_slugs = operation_slug_map(spec)

    # Group operations by OpenAPI tag, preserving spec traversal order.
    entries_by_tag: dict[str, list] = {}
    expected_files: dict[Path, str] = {}
    badge_counts = {"Beta": 0, "Private preview": 0}
    deprecated_count = 0
    total_operations = 0
    for entry in iter_operations(spec):
        total_operations += 1
        entry["description"] = rewrite_spec_links(entry["description"], op_slugs)
        entries_by_tag.setdefault(entry["tag"], []).append(entry)
        if entry["badge"]:
            badge_counts[entry["badge"]] += 1
        if entry["deprecated"]:
            deprecated_count += 1
        if not needs_page(entry):
            continue
        rel = Path(API_REF_DIR) / dir_slug(entry["tag"]) / f"{file_slug(entry['operation_id'])}.mdx"
        if rel in expected_files:
            raise SystemExit(f"Duplicate generated page path: {rel}")
        expected_files[rel] = page_content(entry)

    # The English navigation fragment is the single source of truth for the
    # endpoint structure. Locale fragments reuse that structure while keeping
    # their translated group names.
    fragment = build_fragment(spec, entries_by_tag)
    ref_keys = operation_ref_keys(spec)
    expected_files[Path(FRAGMENT_REL)] = json.dumps(fragment, indent=2, ensure_ascii=False) + "\n"
    for locale in LOCALE_DIRS:
        rel = Path(locale) / FRAGMENT_REL
        path = docs_dir / rel
        current_fragment = json.loads(path.read_text(encoding="utf-8"))
        localized_fragment = localize_fragment(fragment, current_fragment, ref_keys)
        expected_files[rel] = json.dumps(localized_fragment, indent=2, ensure_ascii=False) + "\n"

    # Pin the spec the navigation and pages were generated from, so that page
    # resolution never falls back to a translated locale copy and API changes
    # show up as reviewable diffs. GA operations have no stub (their pages are
    # generated by Mintlify from the string refs), so their description is
    # injected as `x-mint.content` — Mintlify appends it as page body content,
    # which is the only visible place for it given that the page-header
    # description is hidden site-wide by `_site/styles.css`. Swagger-only
    # anchors are rewritten to the per-endpoint pages here as well.
    for path_item in spec.get("paths", {}).values():
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            entry_badge, entry_deprecated = classify_operation(operation)
            description = operation.get("description")
            if description and not (entry_badge or entry_deprecated):
                x_mint = operation.setdefault("x-mint", {})
                x_mint["content"] = mdx_escape(rewrite_spec_links(description, op_slugs))
    # Defuse secret-shaped example values: the spec illustrates Slack webhook
    # fields with placeholder URLs (`T00000000/B00000000/XXXX...`) that still
    # match GitHub's secret-scanning pattern and block every push containing
    # the pinned spec.
    spec_text = json.dumps(spec, indent=2, ensure_ascii=False) + "\n"
    spec_text = re.sub(
        r"https://hooks\.slack\.com/services/[A-Za-z0-9/]+",
        "https://hooks.slack.com/services/EXAMPLE/WEBHOOK/URL",
        spec_text,
    )
    expected_files[Path(SPEC_PIN)] = spec_text

    # Determine page/fragment changes (create/update/delete).
    changes = []
    for rel, content in sorted(expected_files.items()):
        path = docs_dir / rel
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current != content:
            changes.append(("update" if current is not None else "create", rel))
    if api_ref_dir.exists():
        for path in api_ref_dir.rglob("*.mdx"):
            if path.relative_to(docs_dir) not in expected_files:
                changes.append(("delete", path.relative_to(docs_dir)))

    # Rewire the consuming files to the fragment via $ref (idempotent).
    consumer_updates = {}
    for rel, ref_path in CONSUMERS.items():
        path = docs_dir / rel
        text = path.read_text(encoding="utf-8")
        new_text = rewire_to_ref(text, ref_path)
        if new_text != text:
            json.loads(new_text)  # fail early if the splice produced invalid JSON
            consumer_updates[path] = new_text
            changes.append(("rewire", rel))

    total_pages = sum(rel.suffix == ".mdx" for rel in expected_files)
    print(
        f"{total_operations} operations ({total_pages} badge pages) | "
        f"Beta {badge_counts['Beta']} | "
        f"Private preview {badge_counts['Private preview']} | deprecated {deprecated_count}"
    )

    if check_only:
        if changes:
            for kind, rel in changes:
                print(f"  {kind}: {rel}")
            print(f"Drift detected: {len(changes)} change(s)")
            return 1
        print("No drift.")
        return 0

    # Write pages + fragment.
    for rel, content in expected_files.items():
        path = docs_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    # Remove stale generated pages and prune empty directories.
    if api_ref_dir.exists():
        for path in sorted(api_ref_dir.rglob("*.mdx")):
            if path.relative_to(docs_dir) not in expected_files:
                path.unlink()
        for path in sorted(api_ref_dir.rglob("*"), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()
    # Rewire consumers.
    for path, new_text in consumer_updates.items():
        path.write_text(new_text, encoding="utf-8")

    print(f"Wrote {total_pages} pages + fragment; rewired {len(consumer_updates)} consumer(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
