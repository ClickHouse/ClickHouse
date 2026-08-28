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
LEDGER_DELETED = f"Changelog-deleted-entry: 109946 {FIX}"


def changelog(entries, raw_bullets=None, released=()):
    """A minimal CHANGELOG.md: table of contents, an optional raw block, the
    in-progress section, and one already-released section (`released`)."""
    raw = ""
    if raw_bullets is not None:
        raw = "\n".join(
            [cl.RAW_BEGIN, "", "#### NO CL ENTRY", "", *raw_bullets, "", cl.RAW_END, ""]
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


def not_restored(reverts, text):
    return sorted(pr for pr in reverts["restore"] if f"/pull/{pr})" not in text)


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
    assert cl.entry_line(before, "109946") == FIX


def test_revert_of_revert_arrives_after_the_entry_was_deleted(monkeypatch):
    """The timeline the job used to pass while losing a shipped fix: the entry
    was deleted on an earlier run, and today's raw block holds only the revert
    of that revert. Nothing disappears in this edit, so only the ledger can
    demand the entry back."""
    before = changelog([], [REVERT_OF_REVERT])
    reverts = analyze(monkeypatch, before, [LEDGER_REVERT, LEDGER_DELETED])
    # The cancelled revert no longer licenses the deletion of the fix.
    assert reverts["credits"] == {"114911": "114912"}
    assert reverts["restore"] == {"109946": FIX}
    assert reverts["missing"] == [
        {
            "prs": ["109946"],
            "siblings": [],
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
            f"Changelog-deleted-entry: 109946 {reapplied}",
            f'Changelog-revert: 115500 114912 Revert "Revert "Revert "{FIX_TITLE}"""',
            LEDGER_REVERT_OF_REVERT,
            LEDGER_DELETED,
            LEDGER_REVERT,
        ],
    )
    assert reverts["restore"] == {"109946": reapplied}
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
    assert cl.entry_line(changelog([MERGED]), "109000") == MERGED

    reverts = analyze(
        monkeypatch,
        changelog([SOLO], [REVERT_OF_MERGED]),
        [
            f"Changelog-deleted-entry: 109000 {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
        ],
    )
    assert reverts["restore"] == {"109000": MERGED}
    assert reverts["missing"] == [
        {
            "prs": ["109000"],
            "siblings": ["109006"],
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
            f"Changelog-deleted-entry: 109000 {MERGED}",
            f"Changelog-deleted-entry: 109006 {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
            f'Changelog-revert: 115000 109006 Revert "{MERGED_TITLE}"',
        ],
    )
    assert reverts["missing"] == [
        {
            "prs": ["109000", "109006"],
            "siblings": [],
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
    assert cl.duplicate_attributions(before) == set()
    assert cl.duplicate_attributions(pasted) == {"109006"}
    assert cl.duplicate_attributions(merged_back) == set()
    # Both restorations carry the link the earlier check looks for ...
    assert "/pull/109000)" in pasted and "/pull/109000)" in merged_back
    # ... only the duplicate-free one is acceptable.
    assert cl.duplicate_attributions(pasted) - cl.duplicate_attributions(before)
    assert not (
        cl.duplicate_attributions(merged_back) - cl.duplicate_attributions(before)
    )


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
            f"Changelog-deleted-entry: 109000 {MERGED}",
            f'Changelog-revert: 115000 109000 Revert "{MERGED_TITLE}"',
        ],
    )

    left_out = run_verify_edit(
        monkeypatch, tmp_path, old_text, changelog([SOLO]), reverts
    )
    assert left_out is not None and "still missing (['109000'])" in left_out

    pasted = run_verify_edit(
        monkeypatch, tmp_path, old_text, changelog([SOLO, MERGED]), reverts
    )
    assert pasted is not None and "attributes pull requests twice" in pasted
    assert "['109006']" in pasted

    assert (
        run_verify_edit(monkeypatch, tmp_path, old_text, changelog([MERGED]), reverts)
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
    assert reverts["restore"] == {"109946": FIX}
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
        f"Changelog-deleted-entry: 109000 {MERGED}",
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
