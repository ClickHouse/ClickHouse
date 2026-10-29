"""Tests for ci/jobs/changelog_nightly.py: CHANGELOG.md text manipulation,
the git flow (state trailer, restricted-refspec fetches, reconciliation) in
scratch repositories, the fail-closed edit verification (including an editing
agent that creates commits), and the command construction for pushes and PR
lookups."""

import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import changelog_nightly as m

MINI = """### Table of Contents
**[ClickHouse release v26.6, 2026-06-25](#266)**<br/>
**[Changelog for 2025](https://clickhouse.com/docs/whats-new/changelog/2025/)**<br/>

# 2026 Changelog

### <a id="266"></a> ClickHouse release 26.6, 2026-06-25. [Presentation](https://p/), [Video](https://v/)

#### New Feature
* Old entry. [#100000](https://github.com/ClickHouse/ClickHouse/pull/100000) ([A](https://github.com/a)).
"""

GENERATED = """---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release master (abc123def45) FIXME as compared to v26.7.1.1-new (d5846cd8b28)

#### New Feature
* Added function `foo`. [#111111](https://github.com/ClickHouse/ClickHouse/pull/111111) ([A](https://github.com/a)).

#### NOT FOR CHANGELOG / INSIGNIFICANT
* Fix flaky test_something. [#111113](https://github.com/ClickHouse/ClickHouse/pull/111113) ([B](https://github.com/b)).

"""

EDITED_SECTION = """### <a id="267"></a> ClickHouse release 26.7, FIXME (in progress)

#### New Feature
* Added function `foo`. [#111111](https://github.com/ClickHouse/ClickHouse/pull/111111) ([A](https://github.com/a)).
"""

TOC_LINE = "**[ClickHouse release v26.7, FIXME](#267)**<br/>"


def mini_edited():
    text = MINI.replace(
        "**[ClickHouse release v26.6",
        f"{TOC_LINE}\n**[ClickHouse release v26.6",
    )
    return m.insert_before_first_release(text, EDITED_SECTION)


# --- pure text manipulation --------------------------------------------------


def test_strip_preamble_and_count_entries():
    body = m.strip_generated_preamble(GENERATED)
    assert body.startswith("### ClickHouse release master")
    assert m.count_entries(body) == 2
    with pytest.raises(ValueError):
        m.strip_generated_preamble("no header here")


def test_insert_and_extract_raw_blocks():
    body = m.strip_generated_preamble(GENERATED)
    block = m.make_raw_block(body)
    inserted = m.insert_before_first_release(MINI, block)
    lines = inserted.splitlines()
    i_begin = next(i for i, l in enumerate(lines) if l.startswith(m.RAW_BEGIN_PREFIX))
    i_release = next(i for i, l in enumerate(lines) if m.RELEASE_HEADING_RE.match(l))
    assert i_begin < i_release
    assert '<a id="266">' in lines[i_release]
    assert m.extract_raw_blocks(inserted) == [block]
    # A second block (yesterday's edit failed, new entries today)
    inserted2 = m.insert_before_first_release(inserted, m.make_raw_block("### second"))
    assert len(m.extract_raw_blocks(inserted2)) == 2


def test_toc_and_section_extraction():
    edited = mini_edited()
    assert m.extract_toc_line(edited, "26.7") == TOC_LINE
    section = m.extract_in_progress_section(edited, "267")
    assert section.startswith('### <a id="267">')
    assert m.count_entries(section) == 1
    _, tail_before = m.split_at_first_released_section(MINI, "267")
    _, tail_after = m.split_at_first_released_section(edited, "267")
    assert tail_before.rstrip() == tail_after.rstrip()
    corrupted = edited.replace("Old entry", "Tampered entry")
    _, tail_corr = m.split_at_first_released_section(corrupted, "267")
    assert tail_before.rstrip() != tail_corr.rstrip()


def test_compose_on_master_rollover():
    # Master gained the released 26.7 section while our branch holds the 26.8
    # in-progress section and a pending raw block.
    master = mini_edited().replace("FIXME (in progress)", "2026-07-30.").replace(
        "v26.7, FIXME](#267)", "v26.7, 2026-07-30](#267)"
    )
    composed = m.compose_on_master(
        master,
        "26.8",
        TOC_LINE.replace("267", "268").replace("26.7", "26.8"),
        [m.make_raw_block("### pending")],
        EDITED_SECTION.replace("267", "268").replace("26.7", "26.8"),
    )
    lines = composed.splitlines()
    i_raw = next(i for i, l in enumerate(lines) if l.startswith(m.RAW_BEGIN_PREFIX))
    i_268 = next(i for i, l in enumerate(lines) if l.startswith('### <a id="268">'))
    i_267 = next(i for i, l in enumerate(lines) if l.startswith('### <a id="267">'))
    assert i_raw < i_268 < i_267
    i_toc_268 = next(i for i, l in enumerate(lines) if l.startswith("**[ClickHouse release v26.8"))
    i_toc_267 = next(i for i, l in enumerate(lines) if l.startswith("**[ClickHouse release v26.7"))
    assert i_toc_268 < i_toc_267
    with pytest.raises(ValueError):
        m.compose_on_master(master, "26.7", None, [], "whatever")


def test_repo_changelog_has_insertion_anchor():
    # The real CHANGELOG.md must contain the heading the insertion anchors on.
    root = os.path.join(os.path.dirname(__file__), "../..")
    with open(os.path.join(root, m.CHANGELOG_FILE), encoding="utf-8") as fd:
        real = fd.read()
    inserted = m.insert_before_first_release(real, m.make_raw_block("### test"))
    assert len(m.extract_raw_blocks(inserted)) == 1


def test_cycle_end_ref(monkeypatch):
    monkeypatch.setattr(
        m.Shell,
        "get_output",
        lambda *_a, **_k: "v26.6.1.1-new\nv26.7.1.1-new\nv26.10.1.1-new\nv2.b.broken\n",
    )
    assert m.get_cycle_start_tags()[(26, 7)] == "v26.7.1.1-new"
    assert m.get_cycle_end_ref("26.7", "26.7") == "origin/master"
    assert m.get_cycle_end_ref("26.6", "26.7") == "v26.7.1.1-new"
    # Numeric, not lexicographic: the cycle after 26.7 is 26.10
    assert m.get_cycle_end_ref("26.7", "26.10") == "v26.10.1.1-new"
    assert m.get_cycle_end_ref("26.10", "26.11") is None


# --- command construction ----------------------------------------------------


class _FakeInfo:
    repo_name = "ClickHouse/ClickHouse"


def test_push_branch_uses_app_token_without_verbose_logging(monkeypatch):
    checks = []
    monkeypatch.setattr(m, "Info", _FakeInfo)
    monkeypatch.setattr(m.Shell, "get_output", lambda *_a, **_k: "abc123 commit")
    monkeypatch.setattr(
        m.Shell,
        "check",
        lambda command, verbose=False, **_k: checks.append((command, verbose)) or True,
    )
    assert m.push_branch("auto/changelog-26.7", True) == "Pushed auto/changelog-26.7"
    assert len(checks) == 1
    command, verbose = checks[0]
    assert verbose is False
    assert 'token="$(gh auth token)"' in command
    assert "-c http.https://github.com/.extraheader= push" in command
    assert "https://x-access-token:${token}@github.com/ClickHouse/ClickHouse.git" in command
    assert "HEAD:refs/heads/auto/changelog-26.7" in command


def test_push_branch_nothing_to_push(monkeypatch):
    monkeypatch.setattr(m.Shell, "get_output", lambda *_a, **_k: "")
    monkeypatch.setattr(
        m.Shell, "check", lambda *_a, **_k: pytest.fail("must not push")
    )
    assert m.push_branch("auto/changelog-26.7", True) == "Nothing to push"


def test_get_pr_states_scopes_repo_and_filters_fork_prs(monkeypatch):
    commands = []
    monkeypatch.setattr(m, "Info", _FakeInfo)
    monkeypatch.setattr(
        m.Shell,
        "get_output",
        lambda command, **_k: commands.append(command) or "111 OPEN\n",
    )
    assert m.get_pr_states("auto/changelog-26.7") == [("111", "OPEN")]
    (command,) = commands
    # Scoped to the base repository and filtering out fork PRs that use the
    # same head branch name: they must not suppress the bot's own PR.
    assert "--repo ClickHouse/ClickHouse" in command
    assert "isCrossRepository" in command
    assert "select(.isCrossRepository | not)" in command


# --- git flow in scratch repositories ----------------------------------------


def _run(*args, **kwargs):
    subprocess.run(list(args), check=True, **kwargs)


def _commit_all(message):
    _run("git", "add", "-A")
    _run("git", "commit", "-qm", message)


@pytest.fixture
def scratch_repo(tmp_path, monkeypatch):
    """A work clone with MINI committed on master, and its bare origin."""
    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    _run("git", "init", "--bare", "-q", "-b", "master", str(origin))
    _run("git", "clone", "-q", str(origin), str(work))
    monkeypatch.chdir(work)
    _run("git", "config", "user.name", "test")
    _run("git", "config", "user.email", "test@test")
    (work / m.CHANGELOG_FILE).write_text(MINI)
    _commit_all("initial")
    _run("git", "push", "-q", "origin", "master")
    return work


GEN_SHA = "1" * 40


def _make_generate_commit():
    body = m.strip_generated_preamble(GENERATED)
    with open(m.CHANGELOG_FILE, encoding="utf-8") as fd:
        current = fd.read()
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(m.insert_before_first_release(current, m.make_raw_block(body)))
    _commit_all(
        f"Update changelog for 26.7: generate raw entries (x..y)\n\n"
        f"{m.STATE_TRAILER} {GEN_SHA}\n"
    )


def test_state_trailer_with_restricted_refspec(scratch_repo):
    branch = m._branch("26.7")
    assert not m.remote_branch_exists(branch)
    m.checkout_branch(branch, False)
    _make_generate_commit()
    _run("git", "push", "-q", "origin", f"{branch}:refs/heads/{branch}")
    assert m.remote_branch_exists(branch)
    # Runner-like conditions: remote.origin.fetch restricted to master, no
    # remote-tracking ref for the bot branch. The explicit refspec in
    # checkout_branch must still create origin/<branch>, and the trailer must
    # be readable after that checkout.
    _run(
        "git", "config", "remote.origin.fetch",
        "+refs/heads/master:refs/remotes/origin/master",
    )
    _run("git", "update-ref", "-d", f"refs/remotes/origin/{branch}")
    m.checkout_branch(branch, True)
    assert m.get_last_generated_sha(branch) == GEN_SHA
    assert m.determine_from_ref("26.7", branch, True) == GEN_SHA


def test_determine_from_ref_guards(scratch_repo):
    branch = m._branch("26.7")
    m.checkout_branch(branch, False)
    # Fresh branch: fall back to the cycle start tag.
    assert m.determine_from_ref("26.7", branch, False) == "v26.7.1.1-new"
    # A branch that has changelog content but no state trailer must be
    # refused: regenerating from the cycle start would duplicate entries.
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(m.insert_before_first_release(MINI, EDITED_SECTION))
    _commit_all("content without trailer")
    _run("git", "push", "-q", "origin", f"{branch}:refs/heads/{branch}")
    _run("git", "fetch", "-q", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}")
    with pytest.raises(RuntimeError, match="refusing to regenerate"):
        m.determine_from_ref("26.7", branch, True)
    # A pristine branch (no content, no trailer) falls back to the tag.
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(MINI)
    _commit_all("pristine")
    _run("git", "push", "-q", "origin", f"{branch}:refs/heads/{branch}")
    _run("git", "fetch", "-q", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}")
    assert m.determine_from_ref("26.7", branch, True) == "v26.7.1.1-new"


def test_verify_edit_accepts_good_edit_and_catches_bad_ones(scratch_repo):
    m.checkout_branch(m._branch("26.7"), False)
    _make_generate_commit()
    base_sha = m._sha("HEAD")

    def write_edit(text):
        with open(m.CHANGELOG_FILE, "w") as fd:
            fd.write(text)

    good = mini_edited()
    write_edit(good)
    assert m.verify_edit("26.7", base_sha) is None
    # Leftover markers
    write_edit(good + "\n" + m.RAW_END + "\n")
    assert "markers" in m.verify_edit("26.7", base_sha)
    # Missing TOC line
    write_edit(good.replace(TOC_LINE + "\n", ""))
    assert "Table-of-contents" in m.verify_edit("26.7", base_sha)
    # Modified released section
    write_edit(good.replace("Old entry", "Tampered entry"))
    assert "already-released" in m.verify_edit("26.7", base_sha)
    # Wiped section that had accumulated >= 2 entries
    m._reset_worktree(base_sha)
    two_entries = mini_edited().replace(
        "#### New Feature\n* Added function",
        "#### New Feature\n* Another. [#2](https://github.com/ClickHouse/ClickHouse/pull/2) ([A](https://github.com/a)).\n* Added function",
    )
    write_edit(two_entries)
    _commit_all("edited with two entries")
    _make_generate_commit()
    base2 = m._sha("HEAD")
    wiped = two_entries.replace(
        m.extract_in_progress_section(two_entries, "267"),
        '### <a id="267"></a> ClickHouse release 26.7, FIXME (in progress)\n',
    )
    write_edit(wiped)
    assert "disappeared" in m.verify_edit("26.7", base2)
    # Touching another tracked file
    m._reset_worktree(base2)
    write_edit(two_entries)
    with open("other.txt", "w") as fd:
        fd.write("x")
    _run("git", "add", "-N", "other.txt")
    assert "Unexpected working tree changes" in m.verify_edit("26.7", base2)
    _run("git", "rm", "-q", "--cached", "other.txt")
    os.unlink("other.txt")


def test_verify_edit_rejects_agent_commit_and_reset_recovers(scratch_repo):
    """An editing agent that runs `git commit` must not bypass verification,
    and the pinned reset must restore the generate commit."""
    m.checkout_branch(m._branch("26.7"), False)
    _make_generate_commit()
    base_sha = m._sha("HEAD")
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(mini_edited())
    with open("smuggled.txt", "w") as fd:
        fd.write("payload")
    _commit_all("agent-made commit")
    error = m.verify_edit("26.7", base_sha)
    assert "created commits" in error
    m._reset_worktree(base_sha)
    assert m._sha("HEAD") == base_sha
    assert not os.path.exists("smuggled.txt")
    assert m.RAW_BEGIN_PREFIX in open(m.CHANGELOG_FILE).read()


def test_verify_edit_rejects_created_files_and_cleanup_removes_them(scratch_repo):
    """Untracked files created by the editing agent are rejected and removed;
    files that existed before the edit are left alone."""
    m.checkout_branch(m._branch("26.7"), False)
    _make_generate_commit()
    with open("preexisting.txt", "w") as fd:
        fd.write("not the agent's")
    base_sha = m._sha("HEAD")
    pre = m._untracked_files()
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(mini_edited())
    with open("agent_notes.md", "w") as fd:
        fd.write("scratch")
    error = m.verify_edit("26.7", base_sha, pre)
    assert "created files" in error and "agent_notes.md" in error
    m._reset_worktree(base_sha, pre)
    assert not os.path.exists("agent_notes.md")
    assert os.path.exists("preexisting.txt")
    # With no new files the same edit passes.
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(mini_edited())
    assert m.verify_edit("26.7", base_sha, pre) is None


def test_ci_tmp_is_gitignored():
    """_untracked_files relies on the job's scratch space being ignored.
    Checked via ci/.gitignore content rather than `git check-ignore`: in the
    CI Tests container the repo is owned by another uid and git refuses to
    operate on it (dubious ownership)."""
    ci_dir = os.path.join(os.path.dirname(__file__), "..")
    with open(os.path.join(ci_dir, ".gitignore"), encoding="utf-8") as fd:
        patterns = [line.strip() for line in fd]
    assert "/tmp" in patterns


def test_cycle_skip_reason():
    open_pr = [("1", "OPEN")]
    closed = [("1", "CLOSED")]
    # First run of the current cycle: no branch, no PR history -> proceed.
    assert m.cycle_skip_reason("b", True, False, []) is None
    # Normal daily run -> proceed.
    assert m.cycle_skip_reason("b", True, True, open_pr) is None
    # Finished cycle without a branch -> skip.
    assert "finished cycle" in m.cycle_skip_reason("b", False, False, [])
    # A human closed/merged every PR -> leave the cycle alone.
    assert "closed or merged" in m.cycle_skip_reason("b", True, True, closed)
    assert "closed or merged" in m.cycle_skip_reason("b", False, True, closed)
    # The branch was deleted while PR history exists (deleting a branch
    # auto-closes its PR, but keep the guard independent of PR state) ->
    # never silently recreate the branch from the cycle start.
    assert "refusing to recreate" in m.cycle_skip_reason("b", True, False, open_pr)


TWO_ENTRIES = """### <a id="267"></a> ClickHouse release 26.7, FIXME (in progress)

#### New Feature
* Added function `foo`. [#111111](https://github.com/ClickHouse/ClickHouse/pull/111111) ([A](https://github.com/a)).
* Added function `bar`. [#111112](https://github.com/ClickHouse/ClickHouse/pull/111112) ([A](https://github.com/a)).
"""

REVERT_BLOCK = """### ClickHouse release master (abc) FIXME as compared to (def)

#### NO CL ENTRY
* NO CL ENTRY:  'Revert "Add function `bar`"'. [#111130](https://github.com/ClickHouse/ClickHouse/pull/111130) ([B](https://github.com/b)).
"""

NO_REVERT_BLOCK = """### ClickHouse release master (abc) FIXME as compared to (def)

#### NOT FOR CHANGELOG / INSIGNIFICANT
* Something unrelated. [#111131](https://github.com/ClickHouse/ClickHouse/pull/111131) ([B](https://github.com/b)).
"""


def test_verify_edit_deletion_needs_revert(scratch_repo):
    """Old entries may disappear only when the raw block carries reverts, at
    most one deletion per revert entry."""
    m.checkout_branch(m._branch("26.7"), False)
    base = MINI.replace(
        "**[ClickHouse release v26.6",
        f"{TOC_LINE}\n**[ClickHouse release v26.6",
    )
    with_section = m.insert_before_first_release(base, TWO_ENTRIES)

    def make_base(raw_block_body):
        with open(m.CHANGELOG_FILE, "w") as fd:
            fd.write(
                m.insert_before_first_release(
                    with_section, m.make_raw_block(raw_block_body)
                )
            )
        _commit_all("generate")
        return m._sha("HEAD")

    dropped_bar = with_section.replace(
        "* Added function `bar`. [#111112](https://github.com/ClickHouse/ClickHouse/pull/111112) ([A](https://github.com/a)).\n",
        "",
    )
    # Raw block without a revert: the deletion must be rejected.
    base_sha = make_base(NO_REVERT_BLOCK)
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(dropped_bar)
    error = m.verify_edit("26.7", base_sha)
    assert "disappeared" in error and "111112" in error
    # Keeping every old entry passes.
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(with_section)
    assert m.verify_edit("26.7", base_sha) is None
    # Raw block with one revert resolving to #111112: deleting #111112 is
    # allowed, deleting more (or other entries) is not. The resolver is
    # stubbed: tests must not call gh.
    m._reset_worktree(base_sha)
    base_sha = make_base(REVERT_BLOCK)
    import unittest.mock as mock

    with mock.patch.object(
        m, "resolve_reverted_originals", return_value=({"111112"}, 0)
    ):
        with open(m.CHANGELOG_FILE, "w") as fd:
            fd.write(dropped_bar)
        assert m.verify_edit("26.7", base_sha) is None
        dropped_both = dropped_bar.replace(
            "* Added function `foo`. [#111111](https://github.com/ClickHouse/ClickHouse/pull/111111) ([A](https://github.com/a)).\n",
            "",
        )
        with open(m.CHANGELOG_FILE, "w") as fd:
            fd.write(dropped_both)
        error = m.verify_edit("26.7", base_sha)
        assert "disappeared" in error and "111111" in error
    # An unresolvable revert (no `Reverts owner/repo#N` marker or a failed
    # lookup) grants no deletion credit: fail closed.
    with mock.patch.object(
        m, "resolve_reverted_originals", return_value=(set(), ["111130"])
    ):
        with open(m.CHANGELOG_FILE, "w") as fd:
            fd.write(dropped_bar)
        error = m.verify_edit("26.7", base_sha)
        assert "could not be resolved" in error and "111112" in error


def test_revert_entry_detection():
    """Only bullets that ARE reverts earn deletion credit."""
    block = m.make_raw_block(
        "### header\n"
        "#### NO CL ENTRY\n"
        "* NO CL ENTRY:  'Revert \"Add function `bar`\"'. [#1](https://github.com/ClickHouse/ClickHouse/pull/1) ([A](https://github.com/a)).\n"
        "* Reverts ClickHouse/ClickHouse#2. [#3](https://github.com/ClickHouse/ClickHouse/pull/3) ([A](https://github.com/a)).\n"
        "#### Improvement\n"
        "* Improve revert logic in backup restore. [#4](https://github.com/ClickHouse/ClickHouse/pull/4) ([A](https://github.com/a)).\n"
        "* This reverts commit abc123 which broke X. [#5](https://github.com/ClickHouse/ClickHouse/pull/5) ([A](https://github.com/a)).\n"
    )
    strict, reverts = m.raw_strict_prs_and_reverts(block)
    assert set(reverts) == {"1", "3", "5"}
    assert "4" in strict and "5" in strict


def test_verify_edit_rejects_lost_new_entries(scratch_repo):
    """A strict-category raw entry that vanishes in the edit fails
    verification (the state trailer has already advanced past it); junk
    sections are still freely prunable."""
    m.checkout_branch(m._branch("26.7"), False)
    _make_generate_commit()
    base_sha = m._sha("HEAD")
    # mini_edited keeps New Feature #111111 and drops NOT-FOR #111113: OK.
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(mini_edited())
    assert m.verify_edit("26.7", base_sha) is None
    # Dropping the New Feature entry too is a silent loss: rejected.
    lost = mini_edited().replace(
        "* Added function `foo`. [#111111](https://github.com/ClickHouse/ClickHouse/pull/111111) ([A](https://github.com/a)).\n",
        "",
    )
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(lost)
    error = m.verify_edit("26.7", base_sha)
    assert "disappeared" in error and "111111" in error


def test_untracked_files_with_spaces(scratch_repo):
    m.checkout_branch(m._branch("26.7"), False)
    _make_generate_commit()
    base_sha = m._sha("HEAD")
    pre = m._untracked_files()
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(mini_edited())
    with open("agent scratch notes.md", "w") as fd:
        fd.write("x")
    assert m._untracked_files() - pre == {"agent scratch notes.md"}
    error = m.verify_edit("26.7", base_sha, pre)
    assert "created files" in error and "agent scratch notes.md" in error
    m._reset_worktree(base_sha, pre)
    assert not os.path.exists("agent scratch notes.md")


def test_reconcile_aborts_merge_on_failed_resolution(scratch_repo):
    """A structural-resolution failure must not leave the repository
    mid-merge (it would break the next cycle's checkout in the same run)."""
    branch = m._branch("26.7")
    m.checkout_branch(branch, False)
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(m.insert_before_first_release(MINI, EDITED_SECTION))
    _commit_all("our section")
    # Master gains a conflicting 26.7 section of its own: the merge
    # conflicts and compose_on_master refuses (duplicate section).
    _run("git", "checkout", "-q", "master")
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(m.insert_before_first_release(MINI, EDITED_SECTION.replace("function `foo`", "function `other`")))
    _commit_all("master got its own 26.7 section")
    _run("git", "push", "-q", "origin", "master")
    _run("git", "checkout", "-q", branch)
    _run("git", "fetch", "-q", "origin", "+refs/heads/master:refs/remotes/origin/master")
    with pytest.raises(ValueError, match="already contains a section"):
        m.reconcile_with_master("26.7")
    git_dir = subprocess.run(
        ["git", "rev-parse", "--git-dir"], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert not os.path.exists(os.path.join(git_dir, "MERGE_HEAD"))
    # The repository is usable for the next cycle.
    m.checkout_branch(m._branch("26.8"), False)


def test_reconcile_with_master(scratch_repo):
    branch = m._branch("26.7")
    m.checkout_branch(branch, False)
    _make_generate_commit()
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(mini_edited())
    _commit_all("edited")
    assert m.reconcile_with_master("26.7") == "No CHANGELOG.md changes on master to reconcile"
    # A hotfix to CHANGELOG.md lands on master: the merge conflicts and is
    # resolved structurally, keeping both the hotfix and our section.
    _run("git", "checkout", "-q", "master")
    with open(m.CHANGELOG_FILE, "w") as fd:
        fd.write(MINI.replace("Old entry", "Old entry, fixed typo"))
    _commit_all("hotfix on master")
    _run("git", "push", "-q", "origin", "master")
    _run("git", "checkout", "-q", branch)
    _run("git", "fetch", "-q", "origin", "+refs/heads/master:refs/remotes/origin/master")
    info = m.reconcile_with_master("26.7")
    assert info.startswith("Merged origin/master")
    final = open(m.CHANGELOG_FILE).read()
    assert "fixed typo" in final
    assert '<a id="267">' in final
    assert m.extract_toc_line(final, "26.7") is not None
    assert "<<<<<<<" not in final
    status = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True
    ).stdout.strip()
    assert status == ""
