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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))


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
            '### ClickHouse release 26.11 (sha) FIXME as compared to 26.10 (sha)',
            "",
            "#### NOT FOR CHANGELOG / INSIGNIFICANT",
            "",
            _bullet(ORIGINAL, "Add tests."),
            "",
            "#### NO CL ENTRY",
            "",
            _bullet(INNER_REVERT, 'NO CL ENTRY: \'Revert "Add tests"\'.'),
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
    assert (
        _verify(second_base, _edited(FIX_BULLET, UNRELATED_BULLET), second) is None
    )
    error = _verify(second_base, _edited(UNRELATED_BULLET), second)
    assert error is not None and f"'{OUTER_REVERT}'" in error
