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


def changelog(entries, raw_bullets=None):
    """A minimal CHANGELOG.md: table of contents, an optional raw block, the
    in-progress section, and one already-released section."""
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
        ]
    )


def analyze(monkeypatch, text, ledger=()):
    """`analyze_reverts` with `gh pr view` and the branch log stubbed out."""

    def fake_get_output(command, strict=False, verbose=False, retries=1, delay=2):
        if command.startswith("gh pr view"):
            return json.dumps(PULL_REQUESTS[command.split()[3]])
        assert command.startswith("git log origin/master..HEAD"), command
        return "\n".join(["Update changelog: edit new entries", "", *ledger])

    monkeypatch.setattr(cl.Shell, "get_output", staticmethod(fake_get_output))
    monkeypatch.setattr(
        cl, "Info", lambda: type("I", (), {"repo_name": "ClickHouse/ClickHouse"})()
    )
    return cl.analyze_reverts(text)


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
    assert reverts["missing"] == {"109946": FIX}
    assert reverts["context"] == {"109946": (["114911"], ["114912"])}
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
        "missing": {},
        "context": {},
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
