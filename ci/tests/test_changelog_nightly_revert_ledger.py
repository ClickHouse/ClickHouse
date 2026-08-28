"""Regression coverage for the revert bookkeeping of the `NightlyChangelog` job.

A release cycle is edited in ~30 nightly increments, so a revert chain
routinely spans several of them. These tests pin the three timelines that
decide whether an entry ships: the whole chain inside one raw block, the
revert alone, and - the case the job used to get wrong - the revert of the
revert arriving days after the entry was already deleted.
"""

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ci"))

from ci.jobs import changelog_nightly as cl


ANCHOR = "269"
VERSION = "26.9"

FIX = (
    "* Fix the propagation of settings in `accurateCastOrDefault`. "
    "[#109946](https://github.com/ClickHouse/ClickHouse/pull/109946) "
    "([Someone](https://github.com/someone))."
)
FIX_TITLE = "Fix the propagation of settings in `accurateCastOrDefault`"
REVERT = (
    "* NO CL ENTRY: 'Revert \"%s\"'. "
    "[#114911](https://github.com/ClickHouse/ClickHouse/pull/114911) "
    "([Someone](https://github.com/someone))." % FIX_TITLE
)
REVERT_OF_REVERT = (
    "* NO CL ENTRY: 'Revert \"Revert \"%s\"\"'. "
    "[#114912](https://github.com/ClickHouse/ClickHouse/pull/114912) "
    "([Someone](https://github.com/someone))." % FIX_TITLE
)

PULL_REQUESTS = {
    "114911": {
        "title": f'Revert "{FIX_TITLE}"',
        "body": "Reverts ClickHouse/ClickHouse#109946",
    },
    # A revert of a revert made with the web UI carries no `Reverts ...#N`
    # marker; only its nested title identifies the target.
    "114912": {
        "title": f'Revert "Revert "{FIX_TITLE}""',
        "body": "Restores the change.",
    },
    "115500": {
        "title": f'Revert "Revert "Revert "{FIX_TITLE}"""',
        "body": "Reverts ClickHouse/ClickHouse#114912",
    },
}

LEDGER_REVERT = (
    f'Changelog-revert: 114911 109946 Revert "{FIX_TITLE}"'
)
LEDGER_REVERT_OF_REVERT = (
    f'Changelog-revert: 114912 114911 Revert "Revert "{FIX_TITLE}""'
)
BUG_FIX = "Bug Fix (user-visible misbehavior in an official stable release)"
LEDGER_DELETED = f"Changelog-deleted-entry: 109946 [{BUG_FIX}] {FIX}"


def changelog(entries, raw_bullets=None, released=(), raw_category="NO CL ENTRY"):
    """A minimal CHANGELOG.md: table of contents, an optional raw block (under
    `raw_category`), the in-progress section, and one already-released section
    (`released`)."""
    raw = ""
    if raw_bullets is not None:
        raw = "\n".join(
            [
                cl.RAW_BEGIN,
                "",
                f"#### {raw_category}",
                "",
                *raw_bullets,
                "",
                cl.RAW_END,
                "",
            ]
        )
    return "\n".join(
        [
            "## Table of Contents",
            f"**[ClickHouse release v{VERSION}, FIXME](#{ANCHOR})**<br/>",
            "**[ClickHouse release v26.8, 2026-08-01](#268)**<br/>",
            "",
            raw,
            f'### <a id="{ANCHOR}"></a> ClickHouse release {VERSION}, '
            "FIXME (in progress)",
            "",
            "#### Bug Fix (user-visible misbehavior in an official stable release)",
            "",
            *entries,
            "",
            '### <a id="268"></a> ClickHouse release 26.8, 2026-08-01',
            "",
            "* Released. [#1](https://github.com/ClickHouse/ClickHouse/pull/1) "
            "([X](https://github.com/x)).",
            *released,
        ]
    )


def analyze(monkeypatch, text, ledger=()):
    """`analyze_reverts` with `gh pr view` and the branch log stubbed out.
    `ledger` is newest first, the order `git log` walks the branch in."""

    def fake_get_output(command, strict=False, verbose=False, retries=1, delay=2):
        if command.startswith("gh pr view"):
            return json.dumps(PULL_REQUESTS[command.split()[3]])
        assert command.startswith("git log origin/master..HEAD"), command
        return "\n".join(["Update changelog: edit new entries", "", *ledger])

    monkeypatch.setattr(cl.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(
        cl, "Info", lambda: type("I", (), {"repo_name": "ClickHouse/ClickHouse"})()
    )
    return cl.analyze_reverts(text, ANCHOR)


def with_reapply(line, pr):
    """The bullet with the re-applying pull request's link appended, which is
    what skill section 2.5 asks for and what `verify_edit` requires of a
    restoration."""
    return (
        f"{line} [#{pr}](https://github.com/ClickHouse/ClickHouse/pull/{pr}) "
        "([Someone](https://github.com/someone))."
    )


def not_restored(reverts, text):
    section = cl.extract_in_progress_section(text, ANCHOR) or ""
    return sorted(pr for pr in reverts["restore"] if not cl.is_attributed(section, pr))


def uncredited(reverts, before, after):
    return sorted(
        cl.disappeared_entries(before, after, ANCHOR) - set(reverts["credits"])
    )


def test_whole_chain_in_one_raw_block(monkeypatch):
    """The fix, its revert and the revert of the revert in the same range: the
    fix ships, so only the cancelled revert may be dropped."""
    before = changelog([FIX], [REVERT, REVERT_OF_REVERT])
    reverts = analyze(monkeypatch, before)
    assert reverts["credits"] == {"114911": "114912"}
    assert reverts["restore"] == {}
    assert reverts["unresolved"] == []
    assert reverts["ledger"] == [LEDGER_REVERT, LEDGER_REVERT_OF_REVERT]
    # Dropping the fix is not licensed by anything.
    assert uncredited(reverts, before, changelog([])) == ["109946"]
    assert uncredited(reverts, before, changelog([FIX])) == []


def test_revert_alone_licenses_the_deletion(monkeypatch):
    """The run that only sees the revert: the entry goes, and the ledger keeps
    the relation and the text so a later run can bring it back."""
    before = changelog([FIX], [REVERT])
    reverts = analyze(monkeypatch, before)
    assert reverts["credits"] == {"109946": "114911"}
    assert reverts["restore"] == {}
    assert reverts["ledger"] == [LEDGER_REVERT]
    assert uncredited(reverts, before, changelog([])) == []
    assert cl.entry_placement(before, "109946") == (BUG_FIX, FIX)


def test_revert_of_revert_arrives_after_the_entry_was_deleted(monkeypatch):
    """The timeline the job used to pass while losing a shipped fix: the entry
    was deleted on an earlier run, and today's raw block holds only the revert
    of that revert. Nothing disappears in this edit, so only the ledger can
    demand the entry back."""
    before = changelog([], [REVERT_OF_REVERT])
    reverts = analyze(monkeypatch, before, [LEDGER_REVERT, LEDGER_DELETED])
    # The cancelled revert no longer licenses the deletion of the fix.
    assert reverts["credits"] == {"114911": "114912"}
    assert reverts["restore"] == {"109946": (BUG_FIX, FIX)}
    assert reverts["missing"] == [
        {
            "prs": ["109946"],
            "siblings": [],
            "withheld": [],
            "category": BUG_FIX,
            "line": FIX,
            "removed_by": ["114911"],
            "reapplied_by": ["114912"],
        }
    ]
    assert reverts["unresolved"] == []
    # An edit that leaves the entry out is rejected...
    assert not_restored(reverts, changelog([])) == ["109946"]
    # ... and one that puts it back, with the re-applying link appended, passes.
    restored = changelog(
        [
            FIX + " [#114912](https://github.com/ClickHouse/ClickHouse/pull/114912) "
            "([Someone](https://github.com/someone))."
        ]
    )
    assert not_restored(reverts, restored) == []
    assert uncredited(reverts, before, restored) == []


def test_prompt_quotes_the_entries_to_restore(monkeypatch):
    """The agent cannot find the deleted entry anywhere, so the prompt has to
    carry its text and the pull requests that removed and re-applied it."""
    reverts = analyze(
        monkeypatch, changelog([], [REVERT_OF_REVERT]), [LEDGER_REVERT, LEDGER_DELETED]
    )
    prompt = cl._edit_prompt(VERSION, reverts)
    assert "Entries to restore" in prompt
    assert (
        "`#109946`, deleted when `#114911` reverted it, re-applied by `#114912`"
        in prompt
    )
    assert FIX in prompt

    quiet = analyze(monkeypatch, changelog([FIX], []))
    assert "Entries to restore" not in cl._edit_prompt(VERSION, quiet)


def test_reverted_again_licenses_the_deletion_again(monkeypatch):
    """Chains are reduced at any depth: a revert of the re-apply takes the fix
    out of the release again, so its entry may go again."""
    reverts = analyze(
        monkeypatch,
        changelog(
            [FIX],
            [
                "* NO CL ENTRY: 'Revert \"Revert \"Revert \"%s\"\"\"'. "
                "[#115500](https://github.com/ClickHouse/ClickHouse/pull/115500) "
                "([Someone](https://github.com/someone))." % FIX_TITLE
            ],
        ),
        [LEDGER_REVERT, LEDGER_DELETED, LEDGER_REVERT_OF_REVERT],
    )
    assert reverts["credits"]["109946"] == "114911"
    assert reverts["restore"] == {}


def test_branch_without_a_ledger_keeps_the_previous_behaviour(monkeypatch):
    """An in-flight branch whose edit commits predate the trailers: no history
    to consult, and a real entry still cannot be dropped."""
    before = changelog([FIX], [])
    reverts = analyze(monkeypatch, before)
    assert reverts == {
        "credits": {},
        "restore": {},
        "missing": [],
        "withheld": {},
        "unresolved": [],
        "ledger": [],
    }
    assert uncredited(reverts, before, changelog([])) == ["109946"]


def test_unresolvable_revert_grants_no_credit(monkeypatch):
    """A manual revert with neither the marker nor a nested title is not bound
    to a target, so it licenses nothing and is not recorded."""
    PULL_REQUESTS["116000"] = {
        "title": "Revert the thing manually",
        "body": "no marker here",
    }
    before = changelog(
        [FIX],
        [
            "* NO CL ENTRY: 'Revert the thing manually'. "
            "[#116000](https://github.com/ClickHouse/ClickHouse/pull/116000) "
            "([Someone](https://github.com/someone))."
        ],
    )
    reverts = analyze(monkeypatch, before)
    assert reverts["unresolved"] == ["116000"]
    assert reverts["credits"] == {}
    assert reverts["ledger"] == []
    assert uncredited(reverts, before, changelog([])) == ["109946"]


def test_ambiguous_nested_title_binds_to_nothing(monkeypatch):
    """The same change can be reverted twice in a cycle, and both reverts are
    then titled `Revert "X"`. A `Revert "Revert "X""` fits both equally well,
    so it binds to neither: guessing would hand out deletion credit for, or
    demand the restoration of, an entry nobody reverted."""
    PULL_REQUESTS["114800"] = {
        "title": f'Revert "{FIX_TITLE}"',
        "body": "Reverts ClickHouse/ClickHouse#109000",
    }
    reverts = analyze(
        monkeypatch,
        changelog([], [REVERT_OF_REVERT]),
        [
            LEDGER_REVERT,
            LEDGER_DELETED,
            f'Changelog-revert: 114800 109000 Revert "{FIX_TITLE}"',
        ],
    )
    assert reverts["unresolved"] == ["114912"]
    assert reverts["ledger"] == []
    # Both earlier reverts still stand, so both deletions stay licensed and
    # nothing is demanded back.
    assert reverts["credits"] == {"109946": "114911", "109000": "114800"}
    assert reverts["restore"] == {}


def test_restoring_uses_the_newest_recorded_bullet(monkeypatch):
    """After delete, restore, delete again, the entry carries the link of the
    pull request that re-applied it the first time. The restoration has to use
    that text, not the one recorded before the first restoration."""
    reapplied = (
        FIX + " [#114912](https://github.com/ClickHouse/ClickHouse/pull/114912) "
        "([Someone](https://github.com/someone))."
    )
    PULL_REQUESTS["116000"] = {
        "title": f'Revert "Revert "Revert "Revert "{FIX_TITLE}""""',
        "body": "Reverts ClickHouse/ClickHouse#115500",
    }
    reverts = analyze(
        monkeypatch,
        changelog(
            [],
            [
                "* NO CL ENTRY: 'Revert \"Revert \"Revert \"Revert \"%s\"\"\"\"'. "
                "[#116000](https://github.com/ClickHouse/ClickHouse/pull/116000) "
                "([Someone](https://github.com/someone))." % FIX_TITLE
            ],
        ),
        [
            f"Changelog-deleted-entry: 109946 [{BUG_FIX}] {reapplied}",
            f'Changelog-revert: 115500 114912 Revert "Revert "Revert "{FIX_TITLE}"""',
            LEDGER_REVERT_OF_REVERT,
            LEDGER_DELETED,
            LEDGER_REVERT,
        ],
    )
    assert reverts["restore"] == {"109946": (BUG_FIX, reapplied)}
    assert [group["line"] for group in reverts["missing"]] == [reapplied]
    assert reapplied in cl._edit_prompt(VERSION, reverts)


MERGED = (
    "* The default value of `max_insert_threads` changed from `1` to `auto`. "
    "[#109000](https://github.com/ClickHouse/ClickHouse/pull/109000) "
    "([Someone](https://github.com/someone)). "
    "[#109006](https://github.com/ClickHouse/ClickHouse/pull/109006) "
    "([Someone](https://github.com/someone))."
)
SOLO = (
    "* The default value of `max_insert_threads` changed from `1` to `auto`. "
    "[#109006](https://github.com/ClickHouse/ClickHouse/pull/109006) "
    "([Someone](https://github.com/someone))."
)
MERGED_TITLE = "The default value of `max_insert_threads`"
REVERT_OF_MERGED = (
    "* NO CL ENTRY: 'Revert \"Revert \"%s\"\"'. "
    "[#115001](https://github.com/ClickHouse/ClickHouse/pull/115001) "
    "([Someone](https://github.com/someone))." % MERGED_TITLE
)


def test_restoring_into_a_merged_bullet_names_the_surviving_siblings(monkeypatch):
    """A merge (skill section 7) puts several pull requests on one bullet. When
    only one of them is reverted, the bullet stays in the file carrying the
    others, so the restoration has to put the link back into that bullet — the
    recorded line cannot be pasted as a second bullet."""
    PULL_REQUESTS["115000"] = {
        "title": f'Revert "{MERGED_TITLE}"',
        "body": "Reverts ClickHouse/ClickHouse#109000",
    }
    PULL_REQUESTS["115001"] = {
        "title": f'Revert "Revert "{MERGED_TITLE}""',
        "body": "Reverts ClickHouse/ClickHouse#115000",
    }
    # The bullet #109000 is recorded from is the merged one, siblings included.
    assert cl.entry_placement(changelog([MERGED]), "109000") == (BUG_FIX, MERGED)

    reverts = analyze(
        monkeypatch,
        changelog([SOLO], [REVERT_OF_MERGED]),
        [
            f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
        ],
    )
    assert reverts["restore"] == {"109000": (BUG_FIX, MERGED)}
    assert reverts["missing"] == [
        {
            "prs": ["109000"],
            "siblings": ["109006"],
            "withheld": [],
            "category": BUG_FIX,
            "line": MERGED,
            "removed_by": ["115000"],
            "reapplied_by": ["115001"],
        }
    ]
    prompt = cl._edit_prompt(VERSION, reverts)
    assert "That bullet also carries `#109006`" in prompt
    assert "do not paste the line above as a second bullet" in prompt


def test_a_merged_bullet_is_restored_once_for_all_its_pull_requests(monkeypatch):
    """When the whole merged bullet was deleted, both of its pull requests are
    recorded against the same line: that is one entry to bring back, not two."""
    reverts = analyze(
        monkeypatch,
        changelog([], [REVERT_OF_MERGED]),
        [
            f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
            f"Changelog-deleted-entry: 109006 [{BUG_FIX}] {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
            f'Changelog-revert: 115000 109006 Revert "{MERGED_TITLE}"',
        ],
    )
    assert reverts["missing"] == [
        {
            "prs": ["109000", "109006"],
            "siblings": [],
            "withheld": [],
            "category": BUG_FIX,
            "line": MERGED,
            "removed_by": ["115000"],
            "reapplied_by": ["115001"],
        }
    ]
    prompt = cl._edit_prompt(VERSION, reverts)
    assert prompt.count(MERGED) == 1
    assert "`#109000`, `#109006`, deleted when `#115000` reverted them" in prompt


def test_a_duplicated_attribution_is_rejected():
    """What `verify_edit` needs on top of "the link is present": pasting the
    recorded merged bullet back next to the surviving one satisfies that check
    but attributes `#109006` twice."""
    before = changelog([SOLO])
    pasted = changelog([SOLO, MERGED])
    merged_back = changelog([MERGED])
    assert cl.attribution_counts(before)["109006"] == 1
    assert cl.attribution_counts(pasted)["109006"] == 2
    assert cl.attribution_counts(merged_back)["109006"] == 1
    # Both restorations carry the link the earlier check looks for ...
    assert "/pull/109000)" in pasted and "/pull/109000)" in merged_back
    # ... only the duplicate-free one is acceptable.
    assert cl.attribution_counts(pasted)["109006"] > max(
        1, cl.attribution_counts(before).get("109006", 0)
    )
    assert cl.attribution_counts(merged_back)["109006"] == 1


def run_verify_edit(monkeypatch, tmp_path, old_text, new_text, reverts):
    """`verify_edit` against a working tree of one file, with the git plumbing
    stubbed out: `base_sha` holds `old_text`, the checkout holds `new_text`."""
    (tmp_path / cl.CHANGELOG_FILE).write_text(new_text, encoding="utf-8")

    def fake_get_output(command, strict=False, verbose=False, retries=1, delay=2):
        if command.startswith("git diff --name-only"):
            return cl.CHANGELOG_FILE
        assert command.startswith("git show"), command
        return old_text

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cl.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(cl, "_sha", lambda ref: "base")
    monkeypatch.setattr(cl, "_untracked_files", lambda: set())
    return cl.verify_edit(VERSION, "base", reverts)


def test_verify_edit_accepts_only_the_exact_merged_bullet_restoration(
    monkeypatch, tmp_path
):
    """End to end for the merged-bullet path: `#109000` and `#109006` shared a
    bullet, `#109000` was reverted and dropped from it, and the revert has now
    been reverted. Leaving `#109000` out is rejected, pasting the recorded
    bullet next to the surviving one is rejected, merging the link back in
    passes."""
    PULL_REQUESTS.setdefault(
        "115000",
        {
            "title": f'Revert "{MERGED_TITLE}"',
            "body": "Reverts ClickHouse/ClickHouse#109000",
        },
    )
    PULL_REQUESTS.setdefault(
        "115001",
        {
            "title": f'Revert "Revert "{MERGED_TITLE}""',
            "body": "Reverts ClickHouse/ClickHouse#115000",
        },
    )
    old_text = changelog([SOLO], [REVERT_OF_MERGED])
    reverts = analyze(
        monkeypatch,
        old_text,
        [
            f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
        ],
    )

    left_out = run_verify_edit(
        monkeypatch, tmp_path, old_text, changelog([SOLO]), reverts
    )
    assert left_out is not None and "missing from the in-progress section" in left_out
    assert "['109000']" in left_out

    pasted = run_verify_edit(
        monkeypatch,
        tmp_path,
        old_text,
        changelog([SOLO, with_reapply(MERGED, "115001")]),
        reverts,
    )
    assert pasted is not None
    assert "attributes pull requests more than once" in pasted
    assert "#109006 2 times" in pasted

    assert (
        run_verify_edit(
            monkeypatch,
            tmp_path,
            old_text,
            changelog([with_reapply(MERGED, "115001")]),
            reverts,
        )
        is None
    )


def test_a_numeric_revert_title_resolves_across_runs(monkeypatch):
    """The skill's other common revert title is `Revert #NNNNN`, which names
    the target outright. A re-apply titled that way has to cancel the earlier
    revert just as the nested-title form does — otherwise the stale deletion
    credit survives from the ledger and the shipped fix stays dropped."""
    PULL_REQUESTS["114913"] = {"title": "Revert #114911", "body": "No marker."}
    numeric_revert_of_revert = (
        "* NO CL ENTRY: 'Revert #114911'. "
        "[#114913](https://github.com/ClickHouse/ClickHouse/pull/114913) "
        "([Someone](https://github.com/someone))."
    )
    reverts = analyze(
        monkeypatch,
        changelog([], [numeric_revert_of_revert]),
        [LEDGER_REVERT, LEDGER_DELETED],
    )
    assert reverts["unresolved"] == []
    assert reverts["ledger"] == [
        "Changelog-revert: 114913 114911 Revert #114911",
    ]
    # `#114911` is cancelled, so it licenses nothing any more ...
    assert reverts["credits"] == {"114911": "114913"}
    # ... and the fix it removed has to come back.
    assert reverts["restore"] == {"109946": (BUG_FIX, FIX)}
    assert [group["prs"] for group in reverts["missing"]] == [["109946"]]
    assert not_restored(reverts, changelog([])) == ["109946"]


def test_a_numeric_title_pointing_forward_is_not_a_relation(monkeypatch):
    """A revert is always merged after what it reverts. A title naming a
    higher number is not describing a revert of it, and binding it would break
    the descending single-pass reduction, so it stays unresolved."""
    PULL_REQUESTS["114700"] = {"title": "Revert #114911", "body": "No marker."}
    reverts = analyze(
        monkeypatch,
        changelog(
            [],
            [
                "* NO CL ENTRY: 'Revert #114911'. "
                "[#114700](https://github.com/ClickHouse/ClickHouse/pull/114700) "
                "([Someone](https://github.com/someone))."
            ],
        ),
    )
    assert reverts["unresolved"] == ["114700"]
    assert reverts["ledger"] == []


def test_a_quoted_title_mentioning_an_issue_is_not_a_numeric_revert(monkeypatch):
    """`Revert "Fix #12345"` quotes an original title that happens to name an
    issue; it is the nested-title form, not `Revert #NNNNN`."""
    PULL_REQUESTS["115900"] = {"title": 'Revert "Fix #12345"', "body": "No marker."}
    reverts = analyze(
        monkeypatch,
        changelog(
            [],
            [
                "* NO CL ENTRY: 'Revert \"Fix #12345\"'. "
                "[#115900](https://github.com/ClickHouse/ClickHouse/pull/115900) "
                "([Someone](https://github.com/someone))."
            ],
        ),
    )
    # No candidate is titled `Fix #12345`, so it resolves to nothing at all -
    # crucially not to `#12345`, which it does not revert.
    assert reverts["unresolved"] == ["115900"]
    assert reverts["credits"] == {}


def test_a_sibling_surviving_only_in_a_released_section_is_not_a_bullet(monkeypatch):
    """`siblings` says "the bullet is still there, merge into it". A pull
    request that appears only in an already-released section further down the
    file is not that bullet: the same pull request legitimately sits in the
    previous release or a backport."""
    PULL_REQUESTS.setdefault(
        "115000",
        {
            "title": f'Revert "{MERGED_TITLE}"',
            "body": "Reverts ClickHouse/ClickHouse#109000",
        },
    )
    PULL_REQUESTS.setdefault(
        "115001",
        {
            "title": f'Revert "Revert "{MERGED_TITLE}""',
            "body": "Reverts ClickHouse/ClickHouse#115000",
        },
    )
    ledger = [
        f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
        f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
    ]
    # The sibling is in the in-progress section: there is a bullet to merge into.
    in_section = analyze(
        monkeypatch, changelog([SOLO], [REVERT_OF_MERGED]), ledger
    )
    assert [group["siblings"] for group in in_section["missing"]] == [["109006"]]

    # The same sibling only below, in the released section: no bullet to merge
    # into, so the entry is restored as its own bullet.
    below = analyze(
        monkeypatch, changelog([], [REVERT_OF_MERGED], released=[SOLO]), ledger
    )
    assert [group["siblings"] for group in below["missing"]] == [[]]
    prompt = cl._edit_prompt(VERSION, below)
    assert "That bullet also carries" not in prompt


def test_a_released_copy_does_not_stand_in_for_the_restoration(
    monkeypatch, tmp_path
):
    """The entry was in the in-progress section before it was deleted, so that
    is where it has to come back. The same pull request appearing in an
    already-released section further down - published in the last release, or
    backported - is not the restoration, and must not let the edit pass."""
    old_text = changelog([], [REVERT_OF_REVERT], released=[FIX])
    reverts = analyze(monkeypatch, old_text, [LEDGER_REVERT, LEDGER_DELETED])
    # The link is in the file, but not in the section: still to be restored.
    assert "/pull/109946)" in old_text
    assert [group["prs"] for group in reverts["missing"]] == [["109946"]]

    left_out = run_verify_edit(
        monkeypatch, tmp_path, old_text, changelog([], released=[FIX]), reverts
    )
    assert left_out is not None
    assert "missing from the in-progress section" in left_out
    assert "['109946']" in left_out

    assert (
        run_verify_edit(
            monkeypatch,
            tmp_path,
            old_text,
            changelog([with_reapply(FIX, "114912")], released=[FIX]),
            reverts,
        )
        is None
    )


PROMOTED = (
    "* Make the parser reject a trailing comma in `GROUP BY`. "
    "[#110500](https://github.com/ClickHouse/ClickHouse/pull/110500) "
    "([Someone](https://github.com/someone))."
)


def improvement_changelog(entries, raw_bullets=None):
    """The helper's in-progress section with an `Improvement` header instead of
    the `Bug Fix` one, to place a restored entry under a second category."""
    return changelog(entries, raw_bullets).replace(
        "#### Bug Fix (user-visible misbehavior in an official stable release)",
        "#### Improvement",
        1,
    )


def test_a_promoted_entry_is_restored_under_the_category_it_was_moved_to(
    monkeypatch, tmp_path
):
    """`#110500` came out of `NOT FOR CHANGELOG` (skill section 3) into
    `Improvement`, so its pull request does not say where it belongs. The
    ledger records the category the edit chose, and the restoration has to use
    it - re-deriving it from the pull request would undo that decision."""
    PULL_REQUESTS["115700"] = {
        "title": 'Revert "Make the parser reject a trailing comma in `GROUP BY`"',
        "body": "Reverts ClickHouse/ClickHouse#110500",
    }
    PULL_REQUESTS["115701"] = {
        "title": (
            'Revert "Revert "Make the parser reject a trailing comma in '
            '`GROUP BY`""'
        ),
        "body": "Reverts ClickHouse/ClickHouse#115700",
    }
    raw = [
        "* NO CL ENTRY: 'Revert \"Revert \"Make the parser reject a trailing "
        "comma in `GROUP BY`\"\"'. "
        "[#115701](https://github.com/ClickHouse/ClickHouse/pull/115701) "
        "([Someone](https://github.com/someone))."
    ]
    ledger = [
        f"Changelog-deleted-entry: 110500 [Improvement] {PROMOTED}",
        'Changelog-revert: 115700 110500 Revert "Make the parser reject a '
        'trailing comma in `GROUP BY`"',
    ]
    old_text = improvement_changelog([], raw)
    reverts = analyze(monkeypatch, old_text, ledger)
    assert reverts["restore"] == {"110500": ("Improvement", PROMOTED)}
    assert [group["category"] for group in reverts["missing"]] == ["Improvement"]
    assert "which sat under `#### Improvement`" in cl._edit_prompt(VERSION, reverts)

    # Back under `Improvement`: accepted.
    restored = with_reapply(PROMOTED, "115701")
    assert (
        run_verify_edit(
            monkeypatch, tmp_path, old_text, improvement_changelog([restored]), reverts
        )
        is None
    )
    # Back under the category of its own pull request instead: rejected.
    wrong = run_verify_edit(
        monkeypatch, tmp_path, old_text, changelog([restored]), reverts
    )
    assert wrong is not None
    assert "wrong category" in wrong
    assert "#110500 under Bug Fix" in wrong


MENTIONING = (
    "* Speed up the parser. This also revisits the change of "
    "[#109946](https://github.com/ClickHouse/ClickHouse/pull/109946). "
    "[#120000](https://github.com/ClickHouse/ClickHouse/pull/120000) "
    "([Someone](https://github.com/someone))."
)


def test_an_inline_mention_does_not_stand_in_for_the_restoration(
    monkeypatch, tmp_path
):
    """A bullet attributed to `#120000` that merely links `#109946` in its prose
    is not `#109946`'s entry - the file has 17 pull requests linked that way and
    never attributed. Restoration is satisfied by the attribution only, and the
    category is read from the attributed bullet, not from the mentioning one."""
    old_text = changelog([], [REVERT_OF_REVERT])
    reverts = analyze(monkeypatch, old_text, [LEDGER_REVERT, LEDGER_DELETED])
    assert [group["prs"] for group in reverts["missing"]] == [["109946"]]

    mention_only = changelog([MENTIONING])
    # The substring the old check looked for is right there ...
    assert "/pull/109946)" in mention_only
    # ... but it is not an attribution, so the entry is still missing.
    assert not cl.is_attributed(mention_only, "109946")
    assert cl.entry_placement(mention_only, "109946") == ("", "")
    rejected = run_verify_edit(monkeypatch, tmp_path, old_text, mention_only, reverts)
    assert rejected is not None
    assert "missing from the in-progress section" in rejected
    assert "['109946']" in rejected

    # The real entry alongside the mentioning bullet is accepted.
    assert (
        run_verify_edit(
            monkeypatch,
            tmp_path,
            old_text,
            changelog([MENTIONING, with_reapply(FIX, "114912")]),
            reverts,
        )
        is None
    )


def test_a_sibling_only_mentioned_inline_is_not_a_bullet_to_merge_into(monkeypatch):
    """The same distinction for `siblings`: a merged bullet is "still there"
    only if its other pull request is attributed in the section."""
    mentions_sibling = (
        "* Unrelated entry, see "
        "[#109006](https://github.com/ClickHouse/ClickHouse/pull/109006). "
        "[#120001](https://github.com/ClickHouse/ClickHouse/pull/120001) "
        "([Someone](https://github.com/someone))."
    )
    PULL_REQUESTS.setdefault(
        "115000",
        {
            "title": f'Revert "{MERGED_TITLE}"',
            "body": "Reverts ClickHouse/ClickHouse#109000",
        },
    )
    PULL_REQUESTS.setdefault(
        "115001",
        {
            "title": f'Revert "Revert "{MERGED_TITLE}""',
            "body": "Reverts ClickHouse/ClickHouse#115000",
        },
    )
    reverts = analyze(
        monkeypatch,
        changelog([mentions_sibling], [REVERT_OF_MERGED]),
        [
            f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
        ],
    )
    assert [group["siblings"] for group in reverts["missing"]] == [[]]
    assert "That bullet also carries" not in cl._edit_prompt(VERSION, reverts)


def test_an_inline_mention_does_not_preserve_a_raw_entry():
    """The retention side of the same distinction: a strict raw entry is not
    kept by another bullet mentioning its pull request in prose. Without this,
    the edit passes and `revert_licensed_deletions` has no attributed bullet to
    snapshot, so the entry is unrecoverable as well as gone."""
    old_text = changelog(
        [],
        [FIX],
        raw_category="Bug Fix (user-visible misbehavior in an official stable release)",
    )
    # Only a mention of #109946, inside a bullet attributed to #120000.
    mention_only = changelog([MENTIONING])
    assert "/pull/109946)" in mention_only
    assert cl.disappeared_entries(old_text, mention_only, ANCHOR) == {"109946"}
    assert cl.entry_placement(mention_only, "109946") == ("", "")
    # The real entry keeps it.
    assert cl.disappeared_entries(old_text, changelog([FIX]), ANCHOR) == set()


def test_dropping_an_inline_mention_is_not_losing_an_entry():
    """The other direction: an entry is an attribution, so rewording a bullet
    and dropping the inline reference it made to another pull request does not
    count as losing that pull request's entry."""
    old_text = changelog([MENTIONING])
    reworded = changelog(
        [
            "* Speed up the parser. "
            "[#120000](https://github.com/ClickHouse/ClickHouse/pull/120000) "
            "([Someone](https://github.com/someone))."
        ]
    )
    assert cl.disappeared_entries(old_text, reworded, ANCHOR) == set()


def test_a_restoration_records_the_pull_request_that_reapplied_it(
    monkeypatch, tmp_path
):
    """Skill section 2.5: the re-applying pull request's link is appended to the
    restored entry. Its own revert bullet is deleted, so that link is the only
    trace of the re-apply left in the release."""
    old_text = changelog([], [REVERT_OF_REVERT])
    reverts = analyze(monkeypatch, old_text, [LEDGER_REVERT, LEDGER_DELETED])
    assert [group["reapplied_by"] for group in reverts["missing"]] == [["114912"]]

    bare = run_verify_edit(monkeypatch, tmp_path, old_text, changelog([FIX]), reverts)
    assert bare is not None
    assert "do not record the pull request that re-applied the change" in bare
    assert "#114912 on the entry of #109946" in bare

    # The link has to be on the restored entry, not merely somewhere near it.
    elsewhere = run_verify_edit(
        monkeypatch,
        tmp_path,
        old_text,
        changelog(
            [
                FIX,
                "* Unrelated. "
                "[#114912](https://github.com/ClickHouse/ClickHouse/pull/114912) "
                "([Someone](https://github.com/someone)).",
            ]
        ),
        reverts,
    )
    assert elsewhere is not None
    assert "do not record the pull request that re-applied the change" in elsewhere

    assert (
        run_verify_edit(
            monkeypatch,
            tmp_path,
            old_text,
            changelog([with_reapply(FIX, "114912")]),
            reverts,
        )
        is None
    )


def test_an_already_duplicated_entry_cannot_gain_another_copy(monkeypatch, tmp_path):
    """The duplicate guard compares counts, not the set of duplicated pull
    requests: an entry that was already written twice must not be allowed a
    third copy just because it was already in the set."""
    old_text = changelog([FIX, FIX], [REVERT_OF_REVERT])
    reverts = analyze(monkeypatch, old_text, [])
    assert cl.attribution_counts(changelog([FIX, FIX]))["109946"] == 2

    third = run_verify_edit(
        monkeypatch, tmp_path, old_text, changelog([FIX, FIX, FIX]), reverts
    )
    assert third is not None
    assert "attributes pull requests more than once" in third
    assert "#109946 3 times" in third

    # Staying at the count it came in with is tolerated: this edit did not
    # introduce it, and the retention rule forbids removing an entry.
    assert (
        run_verify_edit(
            monkeypatch, tmp_path, old_text, changelog([FIX, FIX]), reverts
        )
        is None
    )


# `#109000` and `#109006` shared a bullet; both were reverted, `#115000` taking
# out `#109000` and `#115100` taking out `#109006`. Only the first revert has
# been reverted, so only `#109000` comes back.
SPLIT_LEDGER = [
    f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
    f"Changelog-deleted-entry: 109006 [{BUG_FIX}] {MERGED}",
    f'Changelog-revert: 115100 109006 Revert "{MERGED_TITLE} follow-up"',
    f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
]


def _merged_revert_prs():
    PULL_REQUESTS.setdefault(
        "115000",
        {
            "title": f'Revert "{MERGED_TITLE}"',
            "body": "Reverts ClickHouse/ClickHouse#109000",
        },
    )
    PULL_REQUESTS.setdefault(
        "115001",
        {
            "title": f'Revert "Revert "{MERGED_TITLE}""',
            "body": "Reverts ClickHouse/ClickHouse#115000",
        },
    )


def test_a_still_reverted_sibling_is_not_resurrected(monkeypatch, tmp_path):
    """Only `#109000` has been re-applied, but the bullet recorded for it also
    attributes `#109006`, whose revert still stands. Replaying the line
    unchanged would advertise a change that is out of the release."""
    _merged_revert_prs()
    old_text = changelog([], [REVERT_OF_MERGED])
    reverts = analyze(monkeypatch, old_text, SPLIT_LEDGER)
    assert reverts["restore"] == {"109000": (BUG_FIX, MERGED)}
    assert reverts["withheld"] == {"109006": "115100"}
    assert [group["withheld"] for group in reverts["missing"]] == [["109006"]]
    prompt = cl._edit_prompt(VERSION, reverts)
    assert "Leave `#109006` off that bullet" in prompt

    pasted = run_verify_edit(
        monkeypatch,
        tmp_path,
        old_text,
        changelog([with_reapply(MERGED, "115001")]),
        reverts,
    )
    assert pasted is not None
    assert "whose revert still stands" in pasted
    assert "#109006 (reverted by #115100)" in pasted

    # `#109000` back on its own, `#109006` left off: accepted.
    trimmed = (
        "* The default value of `max_insert_threads` changed from `1` to `auto`. "
        "[#109000](https://github.com/ClickHouse/ClickHouse/pull/109000) "
        "([Someone](https://github.com/someone))."
    )
    assert (
        run_verify_edit(
            monkeypatch,
            tmp_path,
            old_text,
            changelog([with_reapply(trimmed, "115001")]),
            reverts,
        )
        is None
    )


def test_a_recorded_bullet_may_not_be_restored_as_two(monkeypatch, tmp_path):
    """The whole merged bullet comes back, so both of its pull requests are
    restored - on one bullet. Splitting it undoes the merge of skill section 7
    and lets each half lose the re-applying link separately."""
    _merged_revert_prs()
    old_text = changelog([], [REVERT_OF_MERGED])
    reverts = analyze(
        monkeypatch,
        old_text,
        [
            f"Changelog-deleted-entry: 109000 [{BUG_FIX}] {MERGED}",
            f"Changelog-deleted-entry: 109006 [{BUG_FIX}] {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
            f'Changelog-revert: 115000 109006 Revert "{MERGED_TITLE}"',
        ],
    )
    assert [group["prs"] for group in reverts["missing"]] == [["109000", "109006"]]

    halves = [
        with_reapply(
            "* The default value of `max_insert_threads` changed from `1` to "
            "`auto`. "
            "[#109000](https://github.com/ClickHouse/ClickHouse/pull/109000) "
            "([Someone](https://github.com/someone)).",
            "115001",
        ),
        # The second half even carries the re-apply link, so only the
        # one-bullet rule catches this.
        with_reapply(
            "* The default value of `max_insert_threads` changed from `1` to "
            "`auto`. "
            "[#109006](https://github.com/ClickHouse/ClickHouse/pull/109006) "
            "([Someone](https://github.com/someone)).",
            "115001",
        ),
    ]
    split = run_verify_edit(monkeypatch, tmp_path, old_text, changelog(halves), reverts)
    assert split is not None
    assert "were split across several" in split
    assert "`#109000`, `#109006` on 2 bullets" in split

    # And the second half without the link is caught too, on its own bullet.
    lopsided = run_verify_edit(
        monkeypatch,
        tmp_path,
        old_text,
        changelog([halves[0], halves[1].split(" [#115001]")[0]]),
        reverts,
    )
    assert lopsided is not None

    assert (
        run_verify_edit(
            monkeypatch,
            tmp_path,
            old_text,
            changelog([with_reapply(MERGED, "115001")]),
            reverts,
        )
        is None
    )
