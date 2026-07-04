#!/usr/bin/env python3
"""
Helpers for maintaining the Mintlify navigation fragments.

Per-section `navigation.json` fragments (e.g. `docs/reference/navigation.json`)
are the navigation source of truth: `docs/docs.json` includes each fragment via
a `{"$ref": "./<section>/navigation.json"}` entry, which Mintlify resolves at
build time (nested `$ref`s between fragments resolve too). There is no compile
step -- editing a fragment is all it takes.

Generators that create pages for newly documented items use `insert_page` to
add the page to its fragment group; the fragment is then written back with
`dump_fragment` (byte-stable with the committed formatting).
"""

import json
import os


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def dump_fragment(data):
    # The committed fragments are exactly this serialization (2-space indent,
    # unescaped non-ASCII, trailing newline).
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def _group_pages(node, group_path):
    # Walk `group_path` (e.g. ["Engines", "Table Engines", "MergeTree Family"])
    # down from the fragment root and return the group's `pages` list.
    for name in group_path:
        children = node.get("pages") or node.get("groups") or []
        for child in children:
            if isinstance(child, dict) and child.get("group") == name:
                node = child
                break
        else:
            raise ValueError(f"navigation group {name!r} not found "
                             f"(path: {' > '.join(group_path)})")
    if "pages" not in node:
        node["pages"] = []
    return node["pages"]


def ensure_group(fragment, parent_path, group, pages, root=None, after_group=None):
    """Create or update a fully-generated navigation group: the group's
    `pages` are owned by a generator and set to exactly `pages`. A new group
    is placed after the sibling group named `after_group` (or appended).
    Returns True if the fragment changed."""
    parent_pages = _group_pages(fragment, parent_path)
    node = next(
        (c for c in parent_pages
         if isinstance(c, dict) and c.get("group") == group),
        None,
    )
    changed = False
    if node is None:
        node = {"group": group, "expandable": True, "expanded": False}
        at = len(parent_pages)
        if after_group:
            for i, entry in enumerate(parent_pages):
                if isinstance(entry, dict) and entry.get("group") == after_group:
                    at = i + 1
                    break
        parent_pages.insert(at, node)
        changed = True
    if root and node.get("root") != root:
        node["root"] = root
        changed = True
    if node.get("pages") != pages:
        node["pages"] = list(pages)
        changed = True
    return changed


def insert_page(fragment, group_path, page_id):
    """Insert `page_id` into the group at `group_path`, before the first string
    sibling that sorts after it (case-insensitive, by path basename). Group
    objects are pinned in place and existing entries are never reordered, so a
    curated order stays intact and the new page lands in its alphabetical slot.
    Returns True if the fragment changed."""
    pages = _group_pages(fragment, group_path)
    if page_id in pages:
        return False
    key = os.path.basename(page_id).lower()
    at = len(pages)
    for i, entry in enumerate(pages):
        if isinstance(entry, str) and os.path.basename(entry).lower() > key:
            at = i
            break
    pages.insert(at, page_id)
    return True
