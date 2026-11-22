"""
Tests for the revert resolution of the nightly changelog job
(`ci/jobs/changelog_nightly.py`).

A revert chain routinely spans the ~30 nightly increments of a release cycle:
the change lands on one day, the revert arrives on another, the revert of the
revert on a third. Only the last of them is in the raw blocks the run is
looking at, so `resolve_revert_targets` has to follow the chain through pull
requests it was not given. When it cannot, the verifier reads a revert of a
revert as a plain revert of a pull request without an entry and demands that
no trace of it remain, while the editing rules put its link on the entry it
brought back - the contradiction that wedged the 26.9 changelog for a week in
September 2026.
"""

import json
import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import changelog_nightly as cn

# `#100` is the original change, `#200` reverts it, `#300` reverts `#200`.
# `#300` is made by hand: its body quotes the commit instead of carrying the
# `Reverts owner/repo#N` marker, so its only trace of `#200` is the title it
# nests.
ORIGINAL = "100"
INNER_REVERT = "200"
OUTER_REVERT = "300"

METADATA = {
    ORIGINAL: ("Make the thing faster", "Speeds up the thing."),
    INNER_REVERT: ('Revert "Make the thing faster"', "This reverts commit abc123."),
    OUTER_REVERT: (
        'Revert "Revert "Make the thing faster""',
        "This reverts commit def456.",
    ),
}


def _fetch(prs):
    return {pr: METADATA[pr] for pr in prs if pr in METADATA}


def _search(title, before):
    return {
        pr: value
        for pr, value in METADATA.items()
        if value[0] == title and int(pr) < int(before)
    }


def _resolve(raw_prs, search=_search, **kwargs):
    with mock.patch.object(
        cn, "fetch_pull_requests", side_effect=_fetch
    ), mock.patch.object(cn, "search_pull_requests_by_title", side_effect=search):
        return cn.resolve_revert_targets(raw_prs, **kwargs)


def test_nested_revert_of_unknown_revert_is_followed():
    """The intermediate revert is neither in the raw blocks nor in the ledger:
    it has to be found by the title the outer revert nests, and the chain
    followed through it down to the original change."""
    targets, titles, unresolved = _resolve({OUTER_REVERT})
    assert unresolved == []
    assert targets == {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}
    assert titles[INNER_REVERT] == METADATA[INNER_REVERT][0]
    # The net effect of the chain: the original ships, so its entry is not
    # licensed for deletion; only the entry of the cancelled intermediate
    # revert is.
    credits, cancelled = cn.revert_net_effect(targets)
    assert credits == {INNER_REVERT: OUTER_REVERT}
    assert ORIGINAL not in credits
    assert cancelled == {INNER_REVERT}


def test_nested_revert_known_from_the_ledger_is_not_searched_for():
    """What an earlier night recorded is enough; no lookup by title happens."""

    def _no_search(title, before):
        raise AssertionError(f"searched for {title!r} although the ledger knows it")

    targets, _, unresolved = _resolve(
        {OUTER_REVERT},
        search=_no_search,
        known_titles={INNER_REVERT: METADATA[INNER_REVERT][0]},
        known_targets={INNER_REVERT: {ORIGINAL}},
    )
    assert unresolved == []
    assert targets[OUTER_REVERT] == {INNER_REVERT}


def test_ambiguous_title_stays_unresolved():
    """The same change reverted twice in a cycle gives two reverts with the
    same title; neither of them is the one the outer revert names, so the
    chain grants no deletion credit at all."""
    twin = "250"

    def _ambiguous(title, before):
        found = _search(title, before)
        if INNER_REVERT in found:
            found[twin] = METADATA[INNER_REVERT]
        return found

    targets, _, unresolved = _resolve({OUTER_REVERT}, search=_ambiguous)
    assert unresolved == [OUTER_REVERT]
    assert OUTER_REVERT not in targets


def test_plain_revert_binds_to_the_title_it_quotes():
    """The terminal link of the chain: a hand-made revert quotes only a commit
    in its body, so its target is found by the title it nests as well."""
    targets, _, unresolved = _resolve({INNER_REVERT})
    assert unresolved == []
    assert targets == {INNER_REVERT: {ORIGINAL}}
    credits, cancelled = cn.revert_net_effect(targets)
    assert credits == {ORIGINAL: INNER_REVERT}
    assert cancelled == set()


def test_a_title_with_too_many_hits_identifies_nothing():
    """The search is capped, so a title matching the cap may have a twin
    beyond it; nothing is bound rather than the one hit that came back."""
    original = cn.TITLE_SEARCH_LIMIT
    nodes = [
        {"number": 10 + i, "title": METADATA[ORIGINAL][0], "body": ""}
        for i in range(original)
    ]
    out = json.dumps({"data": {"search": {"nodes": nodes}}})
    with mock.patch.object(cn.Shell, "get_output", return_value=out), mock.patch.object(
        cn, "Info", return_value=mock.Mock(repo_name="o/n")
    ):
        assert (
            cn.search_pull_requests_by_title(METADATA[ORIGINAL][0], INNER_REVERT) == {}
        )


def test_marker_in_the_body_binds_without_a_search():
    """The `Reverts owner/repo#N` line GitHub writes into a revert opened with
    the web UI names the target outright."""

    def _no_search(title, before):
        if title == METADATA[INNER_REVERT][0]:
            raise AssertionError("searched although the body names the target")
        return _search(title, before)

    metadata = dict(METADATA)
    metadata[OUTER_REVERT] = (
        metadata[OUTER_REVERT][0],
        f"Reverts ClickHouse/ClickHouse#{INNER_REVERT}",
    )
    with mock.patch.object(
        cn,
        "fetch_pull_requests",
        side_effect=lambda prs: {p: metadata[p] for p in prs if p in metadata},
    ), mock.patch.object(cn, "search_pull_requests_by_title", side_effect=_no_search):
        targets, _, unresolved = cn.resolve_revert_targets({OUTER_REVERT})
    assert unresolved == []
    assert targets == {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}


def _bullet(pr, text):
    return (
        f"* {text} [#{pr}](https://github.com/ClickHouse/ClickHouse/pull/{pr}) "
        f"([Author](https://github.com/author))."
    )


def _changelog(reapply_category):
    """A raw block with a whole chain: `#100` (tests only), its revert `#200`,
    and `#300` reverting `#200` under `reapply_category`."""
    raw = "\n".join(
        [
            cn.RAW_BEGIN,
            "### ClickHouse release 26.11 (sha) FIXME as compared to 26.10 (sha)",
            "",
            "#### NOT FOR CHANGELOG / INSIGNIFICANT",
            "",
            _bullet(ORIGINAL, "Add tests."),
            "",
            "#### NO CL ENTRY",
            "",
            _bullet(INNER_REVERT, "NO CL ENTRY: 'Revert \"Add tests\"'."),
            "",
            f"#### {reapply_category}",
            "",
            _bullet(OUTER_REVERT, "Fix `INSERT` into a table with a projection."),
            cn.RAW_END,
        ]
    )
    return (
        f"{raw}\n\n"
        '### <a id="2610"></a> ClickHouse release 26.10, 2026-10-22\n\n'
        "#### Bug Fix (user-visible misbehavior in an official stable release)\n\n"
        + _bullet("50", "Fix something old.")
        + "\n"
    )


def _analyze(text):
    targets = {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}
    with mock.patch.object(
        cn, "read_revert_ledger", return_value=({}, {}, {}, set())
    ), mock.patch.object(
        cn,
        "resolve_revert_targets",
        return_value=(targets, {pr: METADATA[pr][0] for pr in targets}, []),
    ):
        return cn.analyze_reverts(text, "2611")


BUG_FIX = "Bug Fix (user-visible misbehavior in an official stable release)"


def test_reapply_with_an_entry_of_its_own_keeps_it():
    """`#129863` in 26.11: a revert of a revert of a tests-only change that
    also fixed something and described it in its own changelog entry. Its
    bullet is the only place the fix appears, so the verifier must not demand
    its deletion, and the strict-retention rule has to protect it."""
    text = _changelog(BUG_FIX)
    reverts = _analyze(text)
    assert OUTER_REVERT not in reverts["cancelling"]
    assert reverts["reapplying_with_entry"] == [OUTER_REVERT]
    # Permitted, not required: merging it into a re-applied entry is fine.
    assert OUTER_REVERT not in reverts["own_entry_required"]
    # The intermediate revert still cancels out.
    assert INNER_REVERT in reverts["cancelling"]

    # An edit that dropped the bullet loses a strict entry.
    edited = text[text.index('### <a id="2610">') :]
    lost = cn.disappeared_entries(text, edited, "2611", reverts["cancelling"])
    assert lost == {OUTER_REVERT}


def test_reapply_without_an_entry_of_its_own_leaves_no_trace():
    """The usual revert-of-revert, with no changelog entry of its own: it
    cancels the intermediate revert and its bullet goes with it."""
    reverts = _analyze(_changelog("NO CL ENTRY"))
    assert OUTER_REVERT in reverts["cancelling"]
    assert reverts["reapplying_with_entry"] == []


TOC = "**[ClickHouse release v26.11, FIXME](#2611)**<br/>"
RELEASED = (
    '### <a id="2610"></a> ClickHouse release 26.10, 2026-10-22\n\n'
    f"#### {BUG_FIX}\n\n" + _bullet("50", "Fix something old.") + "\n"
)
FIX_BULLET = _bullet(OUTER_REVERT, "Fix `INSERT` into a table with a projection.")
UNRELATED = "400"
UNRELATED_BULLET = _bullet(UNRELATED, "Make another thing faster.")


def _edited(*bullets):
    """A changelog after an edit: the in-progress section with `bullets`."""
    body = "\n".join(bullets)
    return (
        f"{TOC}\n\n"
        '### <a id="2611"></a> ClickHouse release 26.11, FIXME (in progress)\n\n'
        f"#### {BUG_FIX}\n\n{body}\n\n{RELEASED}"
    )


def _verify(old_text, new_text, reverts):
    def get_output(command, **_):
        if command.startswith("git diff --name-only"):
            return cn.CHANGELOG_FILE
        if command.startswith("git show"):
            return old_text
        raise AssertionError(command)

    with mock.patch.object(cn, "_sha", return_value="base"), mock.patch.object(
        cn.Shell, "get_output", side_effect=get_output
    ), mock.patch.object(cn, "_untracked_files", return_value=set()), mock.patch.object(
        cn, "_read_changelog", return_value=new_text
    ):
        return cn.verify_edit("26.11", "base", reverts)


def _second_run_base():
    """The second run: the first run's accepted edit with a new raw block of
    an unrelated pull request inserted above the in-progress section."""
    raw = "\n".join(
        [
            cn.RAW_BEGIN,
            "### ClickHouse release 26.11 (sha) FIXME as compared to 26.10 (sha)",
            "",
            "#### Performance Improvement",
            "",
            UNRELATED_BULLET,
            cn.RAW_END,
        ]
    )
    return cn.insert_before_first_release(_edited(FIX_BULLET), raw)


def _analyze_second_run(text):
    """The chain is known only from the ledger now; today's raw block holds
    no revert."""
    ledger = {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}
    titles = {pr: METADATA[pr][0] for pr in ledger}
    with mock.patch.object(
        cn, "read_revert_ledger", return_value=(ledger, titles, {}, set())
    ), mock.patch.object(cn, "resolve_revert_targets", return_value=({}, {}, [])):
        return cn.analyze_reverts(text, "2611")


def test_reapply_own_entry_survives_the_next_run():
    """Day one accepts the re-apply's own bullet; day two, whose raw block is
    unrelated, must keep accepting it and must refuse to lose it."""
    first_base = f"{TOC}\n\n" + _changelog(BUG_FIX)
    first = _analyze(first_base)
    assert _verify(first_base, _edited(FIX_BULLET), first) is None
    assert _verify(first_base, _edited(), first) is not None

    second_base = _second_run_base()
    second = _analyze_second_run(second_base)
    assert OUTER_REVERT not in second["cancelling"]
    # Already integrated: not put to the agent again.
    assert second["reapplying_with_entry_new"] == []
    assert _verify(second_base, _edited(FIX_BULLET, UNRELATED_BULLET), second) is None
    error = _verify(second_base, _edited(UNRELATED_BULLET), second)
    assert error is not None and f"'{OUTER_REVERT}'" in error


REMOVER = "600"
RESTORER = "700"
CHAIN = {OUTER_REVERT: {INNER_REVERT}, INNER_REVERT: {ORIGINAL}}
CHAIN_TITLES = {
    **{pr: METADATA[pr][0] for pr in CHAIN},
    REMOVER: f'Revert "{METADATA[OUTER_REVERT][0]}"',
    RESTORER: f'Revert "Revert "{METADATA[OUTER_REVERT][0]}""',
    "800": f'Revert "{METADATA[ORIGINAL][0]}"',
}


def _with_raw(text, *bullets):
    raw = "\n".join(
        [
            cn.RAW_BEGIN,
            "### ClickHouse release 26.11 (sha) FIXME as compared to 26.10 (sha)",
            "",
            "#### NO CL ENTRY",
            "",
            *bullets,
            cn.RAW_END,
        ]
    )
    return cn.insert_before_first_release(text, raw)


def _analyze_with_ledger(text, ledger, new_targets, deleted=None, integrated=()):
    titles = {pr: CHAIN_TITLES[pr] for pr in ledger}
    with mock.patch.object(
        cn,
        "read_revert_ledger",
        return_value=(ledger, titles, deleted or {}, set(integrated)),
    ), mock.patch.object(
        cn,
        "resolve_revert_targets",
        return_value=(new_targets, {pr: CHAIN_TITLES[pr] for pr in new_targets}, []),
    ):
        return cn.analyze_reverts(text, "2611")


def _record_deletions(old_text, new_text, reverts):
    with mock.patch.object(
        cn.Shell, "get_output", return_value=old_text
    ), mock.patch.object(cn, "_read_changelog", return_value=new_text):
        return cn.revert_licensed_deletions(
            "base",
            "26.11",
            reverts["credits"],
            reverts["cancelling"],
            reverts["brought_back"],
        )


def test_reapply_own_entry_is_restored_after_its_revert_is_reverted():
    """The accepted own bullet of `#300` is reverted by `#600` on a later day,
    and `#600` is reverted by `#700` after that: the bullet then comes back
    from the ledger, where neither today's raw block nor the section knows it,
    and has to be restorable - and required."""
    # `#600` takes the fix out.
    accepted = _edited(FIX_BULLET, UNRELATED_BULLET)
    removal_base = _with_raw(
        accepted, _bullet(REMOVER, f"NO CL ENTRY: '{CHAIN_TITLES[REMOVER]}'.")
    )
    removal = _analyze_with_ledger(removal_base, CHAIN, {REMOVER: {OUTER_REVERT}})
    removed = _edited(UNRELATED_BULLET)
    assert _verify(removal_base, removed, removal) is None
    assert _verify(removal_base, accepted, removal) is not None
    recorded = _record_deletions(removal_base, removed, removal)
    category, kind, line = recorded[OUTER_REVERT]
    assert (category, kind, line) == (BUG_FIX, "section", FIX_BULLET)

    # `#700` puts it back.
    restore_base = _with_raw(
        removed, _bullet(RESTORER, f"NO CL ENTRY: '{CHAIN_TITLES[RESTORER]}'.")
    )
    restore = _analyze_with_ledger(
        restore_base,
        {**CHAIN, REMOVER: {OUTER_REVERT}},
        {RESTORER: {REMOVER}},
        deleted={OUTER_REVERT: (category, line)},
        integrated={OUTER_REVERT},
    )
    assert OUTER_REVERT not in restore["cancelling"]
    assert OUTER_REVERT in restore["required"]
    restored = FIX_BULLET.removesuffix(".") + (
        f" [#{RESTORER}](https://github.com/ClickHouse/ClickHouse/pull/{RESTORER}) "
        "([Author](https://github.com/author))."
    )
    assert _verify(restore_base, _edited(restored, UNRELATED_BULLET), restore) is None
    assert _verify(restore_base, _edited(UNRELATED_BULLET), restore) is not None


def test_reapply_gets_one_bullet_of_its_own():
    """The entry of a re-apply's own is one bullet, also when the change it
    brought back has no entry for its link to sit on: the ledger keeps one
    record of an own entry per pull request."""
    base = f"{TOC}\n\n" + _changelog(BUG_FIX)
    reverts = _analyze(base)
    twice = _edited(FIX_BULLET, _bullet(OUTER_REVERT, "Fix it once more."))
    error = _verify(base, twice, reverts)
    assert error is not None and f"#{OUTER_REVERT} on 2 bullets of its own" in error


FEATURE = "New Feature"
FEATURE_BULLET = _bullet(ORIGINAL, "Add the thing.")


def _with_links(bullet, *prs):
    """`bullet` with the attributions of `prs` appended, the way a re-apply is
    recorded on the entry it brought back."""
    links = "".join(
        f" [#{pr}](https://github.com/ClickHouse/ClickHouse/pull/{pr}) "
        "([Author](https://github.com/author))"
        for pr in prs
    )
    return bullet.removesuffix(".") + links + "."


def _sections(categories):
    """A changelog after an edit, with the in-progress section holding the
    bullets of `categories`, a mapping of category -> bullets."""
    body = "\n\n".join(
        f"#### {category}\n\n" + "\n".join(bullets)
        for category, bullets in categories.items()
        if bullets
    )
    return (
        f"{TOC}\n\n"
        '### <a id="2611"></a> ClickHouse release 26.11, FIXME (in progress)\n\n'
        f"{body}\n\n{RELEASED}"
    )


def _raw(text, categories):
    """`text` with a raw block holding the bullets of `categories`."""
    lines = [
        cn.RAW_BEGIN,
        "### ClickHouse release 26.11 (sha) FIXME as compared to 26.10 (sha)",
    ]
    for category, bullets in categories.items():
        lines += ["", f"#### {category}", "", *bullets]
    lines.append(cn.RAW_END)
    return cn.insert_before_first_release(text, "\n".join(lines))


def _revert_bullet(pr):
    return _bullet(pr, f"NO CL ENTRY: '{CHAIN_TITLES[pr]}'.")


def test_pure_reapply_link_goes_when_the_original_is_reverted_again():
    """`#100` adds a feature, `#200` reverts it and `#300` re-applies it with
    no entry and no change of its own: its link on the feature bullet is an
    annotation. When `#600` reverts `#100` again on a later day, that link is
    no evidence of an entry of `#300`'s own - the bullet goes with both links,
    and `#300` gets no bullet for a feature that is not in the release."""
    arrival_base = _raw(
        f"{TOC}\n\n{RELEASED}",
        {
            FEATURE: [FEATURE_BULLET],
            BUG_FIX: [UNRELATED_BULLET],
            "NO CL ENTRY": [_revert_bullet(INNER_REVERT), _revert_bullet(OUTER_REVERT)],
        },
    )
    arrival = _analyze_with_ledger(arrival_base, {}, CHAIN)
    assert OUTER_REVERT in arrival["cancelling"]
    annotated = _with_links(FEATURE_BULLET, OUTER_REVERT)
    accepted = _sections({FEATURE: [annotated], BUG_FIX: [UNRELATED_BULLET]})
    assert _verify(arrival_base, accepted, arrival) is None

    # `#600` reverts `#100` directly.
    removal_base = _raw(accepted, {"NO CL ENTRY": [_revert_bullet(REMOVER)]})
    removal = _analyze_with_ledger(removal_base, CHAIN, {REMOVER: {ORIGINAL}})
    assert OUTER_REVERT in removal["cancelling"]
    assert removal["reapplying_with_entry"] == []
    removed = _sections({BUG_FIX: [UNRELATED_BULLET]})
    assert _verify(removal_base, removed, removal) is None
    # Neither the reverted feature nor a bullet of the re-apply for it stays.
    assert _verify(removal_base, accepted, removal) is not None
    standalone = _sections(
        {
            FEATURE: [_bullet(OUTER_REVERT, "Add the thing.")],
            BUG_FIX: [UNRELATED_BULLET],
        }
    )
    error = _verify(removal_base, standalone, removal)
    assert error is not None and f"#{OUTER_REVERT}" in error
    # The feature bullet is recorded for the feature; the re-apply has no entry.
    recorded = _record_deletions(removal_base, removed, removal)
    assert recorded == {ORIGINAL: (FEATURE, "section", annotated)}

    # `#700` reverts `#600`: the feature is back, with both re-applies on it.
    restore_base = _raw(removed, {"NO CL ENTRY": [_revert_bullet(RESTORER)]})
    restore = _analyze_with_ledger(
        restore_base,
        {**CHAIN, REMOVER: {ORIGINAL}},
        {RESTORER: {REMOVER}},
        deleted={ORIGINAL: (FEATURE, annotated)},
        integrated={ORIGINAL},
    )
    assert OUTER_REVERT in restore["cancelling"]
    restored = _sections(
        {FEATURE: [_with_links(annotated, RESTORER)], BUG_FIX: [UNRELATED_BULLET]}
    )
    assert _verify(restore_base, restored, restore) is None
    assert _verify(restore_base, accepted, restore) is not None


def test_reapply_own_entry_is_kept_apart_from_its_annotation():
    """`#100` adds a feature, `#200` reverts it and `#300` re-applies it and
    fixes something else, with an entry of its own. `#300` is then on two
    bullets: the feature it brought back, as an annotation, and its fix. Both
    are taken out when `#600` reverts `#300` and both have to come back when
    `#700` reverts `#600` - the fix from its own record, not from the feature
    bullet that merely carries the link of `#300`."""
    arrival_base = _raw(
        f"{TOC}\n\n{RELEASED}",
        {
            FEATURE: [FEATURE_BULLET],
            "NO CL ENTRY": [_revert_bullet(INNER_REVERT)],
            BUG_FIX: [FIX_BULLET],
        },
    )
    arrival = _analyze_with_ledger(arrival_base, {}, CHAIN)
    assert arrival["reapplying_with_entry"] == [OUTER_REVERT]
    annotated = _with_links(FEATURE_BULLET, OUTER_REVERT)
    accepted = _sections({FEATURE: [annotated], BUG_FIX: [FIX_BULLET]})
    assert _verify(arrival_base, accepted, arrival) is None

    # The next day, with an unrelated raw block: the fix bullet is the evidence
    # of an own entry, and losing it is noticed although the feature bullet
    # still attributes `#300`.
    next_base = _raw(accepted, {"Performance Improvement": [UNRELATED_BULLET]})
    next_day = _analyze_with_ledger(next_base, CHAIN, {})
    assert next_day["reapplying_with_entry"] == [OUTER_REVERT]
    kept = _sections({FEATURE: [annotated], BUG_FIX: [FIX_BULLET, UNRELATED_BULLET]})
    assert _verify(next_base, kept, next_day) is None
    fix_lost = _sections({FEATURE: [annotated], BUG_FIX: [UNRELATED_BULLET]})
    error = _verify(next_base, fix_lost, next_day)
    assert error is not None and f"'{OUTER_REVERT}'" in error

    # `#600` reverts `#300`: the feature is out again and so is the fix.
    removal_base = _raw(kept, {"NO CL ENTRY": [_revert_bullet(REMOVER)]})
    removal = _analyze_with_ledger(removal_base, CHAIN, {REMOVER: {OUTER_REVERT}})
    removed = _sections({BUG_FIX: [UNRELATED_BULLET]})
    assert _verify(removal_base, removed, removal) is None
    assert _verify(removal_base, fix_lost, removal) is not None
    fix_kept = _sections({BUG_FIX: [FIX_BULLET, UNRELATED_BULLET]})
    assert _verify(removal_base, fix_kept, removal) is not None
    # Each pull request is recorded with the bullet of its own entry.
    recorded = _record_deletions(removal_base, removed, removal)
    assert recorded == {
        ORIGINAL: (FEATURE, "section", annotated),
        OUTER_REVERT: (BUG_FIX, "section", FIX_BULLET),
    }

    # `#700` reverts `#600`: two bullets to restore.
    restore_base = _raw(removed, {"NO CL ENTRY": [_revert_bullet(RESTORER)]})
    restore = _analyze_with_ledger(
        restore_base,
        {**CHAIN, REMOVER: {OUTER_REVERT}},
        {RESTORER: {REMOVER}},
        deleted={pr: (category, line) for pr, (category, _, line) in recorded.items()},
        integrated=set(recorded),
    )
    assert [(group["prs"], group["line"]) for group in restore["missing"]] == [
        ([ORIGINAL], annotated),
        ([OUTER_REVERT], FIX_BULLET),
    ]
    feature_back = _with_links(annotated, RESTORER)
    fix_back = _with_links(FIX_BULLET, RESTORER)
    both = _sections({FEATURE: [feature_back], BUG_FIX: [fix_back, UNRELATED_BULLET]})
    assert _verify(restore_base, both, restore) is None
    # The feature bullet alone attributes `#300`, but the fix is still lost.
    only_feature = _sections({FEATURE: [feature_back], BUG_FIX: [UNRELATED_BULLET]})
    error = _verify(restore_base, only_feature, restore)
    assert error is not None and OUTER_REVERT in error
    only_fix = _sections({BUG_FIX: [fix_back, UNRELATED_BULLET]})
    assert _verify(restore_base, only_fix, restore) is not None
    # The fix records its own re-apply, not only the feature bullet does.
    unrecorded = _sections(
        {FEATURE: [feature_back], BUG_FIX: [FIX_BULLET, UNRELATED_BULLET]}
    )
    assert _verify(restore_base, unrecorded, restore) is not None
    # And it comes back under its own category, not the feature's.
    misplaced = _sections(
        {FEATURE: [feature_back, fix_back], BUG_FIX: [UNRELATED_BULLET]}
    )
    assert _verify(restore_base, misplaced, restore) is not None


def test_reapply_own_entry_stays_when_the_original_is_reverted_again():
    """The same `#300` with a fix of its own, but this time `#600` reverts the
    original `#100` directly: the feature bullet goes, with the link of `#300`
    on it, and the fix bullet stays - that change still ships."""
    annotated = _with_links(FEATURE_BULLET, OUTER_REVERT)
    accepted = _sections(
        {FEATURE: [annotated], BUG_FIX: [FIX_BULLET, UNRELATED_BULLET]}
    )
    removal_base = _raw(accepted, {"NO CL ENTRY": [_revert_bullet(REMOVER)]})
    removal = _analyze_with_ledger(removal_base, CHAIN, {REMOVER: {ORIGINAL}})
    assert removal["reapplying_with_entry"] == [OUTER_REVERT]
    fix_stays = _sections({BUG_FIX: [FIX_BULLET, UNRELATED_BULLET]})
    assert _verify(removal_base, fix_stays, removal) is None
    assert _verify(removal_base, accepted, removal) is not None
    both_gone = _sections({BUG_FIX: [UNRELATED_BULLET]})
    error = _verify(removal_base, both_gone, removal)
    assert error is not None and f"'{OUTER_REVERT}'" in error
    # Only the feature was taken out, so only the feature is recorded.
    recorded = _record_deletions(removal_base, fix_stays, removal)
    assert recorded == {ORIGINAL: (FEATURE, "section", annotated)}


SECOND_REMOVER = "800"
REPEATED_BULLET = _bullet(ORIGINAL, "Fix the thing.")
REPEATING_BULLET = _bullet(OUTER_REVERT, "Fix the thing.")
# The whole chain and the revert of the re-apply, arriving on one night.
REVERTED_ON_ARRIVAL = {**CHAIN, REMOVER: {OUTER_REVERT}}


def _ledger_records(recorded):
    """What the ledger gives back for the deletions `recorded`: the records
    and the pull requests among them whose entry was in the section."""
    deleted = {pr: (category, line) for pr, (category, _, line) in recorded.items()}
    integrated = {pr for pr, (_, where, _) in recorded.items() if where == "section"}
    return deleted, integrated


def test_repeating_reapply_entry_can_be_merged_on_restoration():
    """`#100` and its re-apply `#300` carry the same entry, and `#600` reverts
    `#300` on the night they all arrive, so both raw entries are deleted before
    any edit has placed them. When `#700` reverts `#600`, the entry of `#300`
    is placed as it would have been on arrival: merged into the entry it
    brought back, or as a bullet of its own. A merged one makes `#300` a plain
    re-apply again, whose link goes when `#100` is reverted once more."""
    arrival_base = _raw(
        f"{TOC}\n\n{RELEASED}",
        {
            BUG_FIX: [REPEATED_BULLET, REPEATING_BULLET, UNRELATED_BULLET],
            "NO CL ENTRY": [_revert_bullet(INNER_REVERT), _revert_bullet(REMOVER)],
        },
    )
    arrival = _analyze_with_ledger(arrival_base, {}, REVERTED_ON_ARRIVAL)
    removed = _sections({BUG_FIX: [UNRELATED_BULLET]})
    assert _verify(arrival_base, removed, arrival) is None
    recorded = _record_deletions(arrival_base, removed, arrival)
    assert recorded == {
        ORIGINAL: (BUG_FIX, "raw", REPEATED_BULLET),
        OUTER_REVERT: (BUG_FIX, "raw", REPEATING_BULLET),
    }
    deleted, integrated = _ledger_records(recorded)

    # `#700` reverts `#600`: both are back, and `#300` is still to be placed.
    restore_base = _raw(removed, {"NO CL ENTRY": [_revert_bullet(RESTORER)]})
    restore = _analyze_with_ledger(
        restore_base,
        REVERTED_ON_ARRIVAL,
        {RESTORER: {REMOVER}},
        deleted=deleted,
        integrated=integrated,
    )
    assert restore["unplaced"] == [OUTER_REVERT]
    assert restore["required"] == [ORIGINAL, OUTER_REVERT]
    merged_bullet = _with_links(REPEATED_BULLET, OUTER_REVERT, RESTORER)
    merged = _sections({BUG_FIX: [merged_bullet, UNRELATED_BULLET]})
    assert _verify(restore_base, merged, restore) is None
    separate = _sections(
        {
            BUG_FIX: [
                merged_bullet,
                _with_links(REPEATING_BULLET, RESTORER),
                UNRELATED_BULLET,
            ]
        }
    )
    assert _verify(restore_base, separate, restore) is None
    # Not without a trace of `#300`, and not without the entries at all.
    untraced = _sections(
        {BUG_FIX: [_with_links(REPEATED_BULLET, RESTORER), UNRELATED_BULLET]}
    )
    assert _verify(restore_base, untraced, restore) is not None
    assert _verify(restore_base, removed, restore) is not None

    # The next day the merged link is what the section says about `#300`: a
    # plain re-apply. The raw record asks for nothing more.
    later_ledger = {**REVERTED_ON_ARRIVAL, RESTORER: {REMOVER}}
    another = _bullet("450", "Speed something up.")
    next_base = _raw(merged, {"Performance Improvement": [another]})
    next_day = _analyze_with_ledger(
        next_base, later_ledger, {}, deleted=deleted, integrated=integrated
    )
    assert OUTER_REVERT in next_day["cancelling"]
    assert next_day["unplaced"] == []
    assert next_day["required"] == [ORIGINAL]
    kept = _sections({BUG_FIX: [merged_bullet, UNRELATED_BULLET, another]})
    assert _verify(next_base, kept, next_day) is None

    # `#800` reverts `#100` again: the bullet goes with all of its links, and
    # the old raw record does not ask for `#300` back.
    again_base = _raw(kept, {"NO CL ENTRY": [_revert_bullet(SECOND_REMOVER)]})
    again = _analyze_with_ledger(
        again_base,
        later_ledger,
        {SECOND_REMOVER: {ORIGINAL}},
        deleted=deleted,
        integrated=integrated,
    )
    assert OUTER_REVERT in again["cancelling"]
    assert again["required"] == []
    gone = _sections({BUG_FIX: [UNRELATED_BULLET, another]})
    assert _verify(again_base, gone, again) is None
    assert _verify(again_base, kept, again) is not None
    standalone = _sections(
        {
            BUG_FIX: [
                _with_links(REPEATING_BULLET, RESTORER),
                UNRELATED_BULLET,
                another,
            ]
        }
    )
    error = _verify(again_base, standalone, again)
    assert error is not None and f"#{OUTER_REVERT}" in error


def test_unplaced_reapply_entry_comes_back_as_a_bullet_when_nothing_carries_its_link():
    """The `#129863` shape, reverted on the night it arrives: the change it
    re-applied has no entry, so when it comes back its raw entry can only be
    placed as a bullet of its own - and from then on that bullet is its entry."""
    arrival_base = _raw(
        f"{TOC}\n\n{RELEASED}",
        {
            "NOT FOR CHANGELOG / INSIGNIFICANT": [_bullet(ORIGINAL, "Add tests.")],
            BUG_FIX: [FIX_BULLET, UNRELATED_BULLET],
            "NO CL ENTRY": [_revert_bullet(INNER_REVERT), _revert_bullet(REMOVER)],
        },
    )
    arrival = _analyze_with_ledger(arrival_base, {}, REVERTED_ON_ARRIVAL)
    removed = _sections({BUG_FIX: [UNRELATED_BULLET]})
    assert _verify(arrival_base, removed, arrival) is None
    recorded = _record_deletions(arrival_base, removed, arrival)
    assert recorded[OUTER_REVERT] == (BUG_FIX, "raw", FIX_BULLET)
    deleted, integrated = _ledger_records(recorded)

    restore_base = _raw(removed, {"NO CL ENTRY": [_revert_bullet(RESTORER)]})
    restore = _analyze_with_ledger(
        restore_base,
        REVERTED_ON_ARRIVAL,
        {RESTORER: {REMOVER}},
        deleted=deleted,
        integrated=integrated,
    )
    assert restore["unplaced"] == [OUTER_REVERT]
    # The tests-only original was a prunable raw bullet: offered, not required.
    assert restore["required"] == [OUTER_REVERT]
    fix_back = _with_links(FIX_BULLET, RESTORER)
    back = _sections({BUG_FIX: [fix_back, UNRELATED_BULLET]})
    assert _verify(restore_base, back, restore) is None
    assert _verify(restore_base, removed, restore) is not None

    another = _bullet("450", "Speed something up.")
    next_base = _raw(back, {"Performance Improvement": [another]})
    next_day = _analyze_with_ledger(
        next_base,
        {**REVERTED_ON_ARRIVAL, RESTORER: {REMOVER}},
        {},
        deleted=deleted,
        integrated=integrated,
    )
    assert next_day["reapplying_with_entry"] == [OUTER_REVERT]
    lost = _sections({BUG_FIX: [UNRELATED_BULLET, another]})
    assert _verify(next_base, lost, next_day) is not None


def test_unplaced_reapply_entry_is_offered_when_the_original_is_out_again():
    """The revert of `#600` and a second revert of `#100` arrive together: the
    re-apply is back but what it re-applied is not, so there is no entry for
    its link to sit on. Whether its raw entry says more than the re-apply is
    then the agent's call - it is offered, with or without a bullet passing."""
    arrival_base = _raw(
        f"{TOC}\n\n{RELEASED}",
        {
            BUG_FIX: [REPEATED_BULLET, REPEATING_BULLET, UNRELATED_BULLET],
            "NO CL ENTRY": [_revert_bullet(INNER_REVERT), _revert_bullet(REMOVER)],
        },
    )
    arrival = _analyze_with_ledger(arrival_base, {}, REVERTED_ON_ARRIVAL)
    removed = _sections({BUG_FIX: [UNRELATED_BULLET]})
    deleted, integrated = _ledger_records(
        _record_deletions(arrival_base, removed, arrival)
    )

    restore_base = _raw(
        removed,
        {"NO CL ENTRY": [_revert_bullet(RESTORER), _revert_bullet(SECOND_REMOVER)]},
    )
    restore = _analyze_with_ledger(
        restore_base,
        REVERTED_ON_ARRIVAL,
        {RESTORER: {REMOVER}, SECOND_REMOVER: {ORIGINAL}},
        deleted=deleted,
        integrated=integrated,
    )
    assert restore["unplaced"] == [OUTER_REVERT]
    assert restore["required"] == []
    assert [
        (group["prs"], group["required"], group["unplaced"])
        for group in restore["missing"]
    ] == [([OUTER_REVERT], False, True)]
    assert _verify(restore_base, removed, restore) is None
    own = _sections(
        {BUG_FIX: [_with_links(REPEATING_BULLET, RESTORER), UNRELATED_BULLET]}
    )
    assert _verify(restore_base, own, restore) is None
    # The original stays out either way.
    with_original = _sections(
        {
            BUG_FIX: [
                _with_links(REPEATED_BULLET, OUTER_REVERT, RESTORER),
                UNRELATED_BULLET,
            ]
        }
    )
    assert _verify(restore_base, with_original, restore) is not None


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
