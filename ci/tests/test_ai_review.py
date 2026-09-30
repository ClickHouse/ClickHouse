"""
Tests for the AI code review job helpers (ci/jobs/scripts/ai_review/).

The job posts what the agent wrote only after validating it: inline comments
must target lines of the PR diff, nits stay in the summary, thread actions obey
the ownership rules, and the Loom client never fails the review and never sends
a private repository's content to the public namespace.
"""

import json
import os
import sys
import tempfile
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.ai_review import context, loom, prompt, publish

PATCH = (
    "@@ -10,4 +10,5 @@ void f()\n"
    " int a = 1;\n"
    "-int b = 2;\n"
    "+int b = 3;\n"
    "+int c = 4;\n"
    " int d = 5;\n"
    "@@ -40,2 +41,3 @@ void g()\n"
    " x();\n"
    "+y();\n"
    " z();\n"
    "\\ No newline at end of file"
)
FILES = [{"filename": "src/Foo.cpp", "patch": PATCH}, {"filename": "big.bin"}]


def _thread(tid, ours=True, resolved=False, resolved_by=None, path="src/Foo.cpp", line=11):
    return {
        "id": tid, "isResolved": resolved, "resolvedBy": {"login": resolved_by} if resolved_by else None,
        "path": path, "line": line,
        "comments": {"nodes": [{"databaseId": 7, "viewerDidAuthor": ours,
                                "author": {"login": "clickhouse-gh" if ours else "someone"}, "body": "x"}]},
    }


def _body(directory, name, text="finding"):
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def test_commentable_lines():
    lines = publish.commentable_lines(PATCH)
    assert sorted(lines["RIGHT"]) == [10, 11, 12, 13, 41, 42, 43]
    assert sorted(lines["LEFT"]) == [10, 11, 12, 40, 41]
    assert lines["RIGHT"][10] == lines["RIGHT"][13] == 0
    assert lines["RIGHT"][42] == 1


def test_validate_comments():
    with tempfile.TemporaryDirectory() as d:
        b = _body(d, "b.md")
        entries = [
            {"path": "src/Foo.cpp", "line": 12, "side": "RIGHT", "severity": "blocker", "body_file": b},
            {"path": "src/Foo.cpp", "line": 11, "side": "LEFT", "severity": "major", "body_file": b},
            {"path": "src/Foo.cpp", "start_line": 10, "line": 13, "severity": "major", "body_file": b},
            {"path": "src/Foo.cpp", "start_line": 12, "line": 42, "severity": "major", "body_file": b},  # crosses hunks
            {"path": "src/Foo.cpp", "line": 30, "severity": "major", "body_file": b},  # outside the hunks
            {"path": "src/Other.cpp", "line": 1, "severity": "major", "body_file": b},  # not in the PR
            {"path": "big.bin", "line": 1, "severity": "major", "body_file": b},  # no patch
            {"path": "src/Foo.cpp", "line": 42, "severity": "nit", "body_file": b},
            {"path": "src/Foo.cpp", "line": 41, "severity": "major", "body_file": os.path.join(d, "missing.md")},
            {"path": "src/Foo.cpp", "line": 12, "side": "RIGHT", "severity": "major", "body_file": b},  # repeat
            {"path": "src/Foo.cpp", "line": "x", "body_file": b},
        ]
        postable, moved = publish.validate_comments(entries, FILES, [], d)
        assert [(c["line"], c["side"], c.get("start_line")) for c in postable] == [
            (12, "RIGHT", None), (11, "LEFT", None), (13, "RIGHT", 10), (42, "RIGHT", None)]
        reasons = [r for _, _, r in moved]
        assert len(moved) == 5
        assert "line 30 (RIGHT) is not in the PR diff" in reasons
        assert "file is not part of the PR diff" in reasons
        assert "nits are listed in the summary only" in reasons
        assert "no valid line number" in reasons


def test_validate_comments_skips_lines_with_an_open_thread_of_ours():
    with tempfile.TemporaryDirectory() as d:
        b = _body(d, "b.md")
        entries = [{"path": "src/Foo.cpp", "line": 11, "severity": "major", "body_file": b},
                   {"path": "src/Foo.cpp", "line": 12, "severity": "major", "body_file": b}]
        threads = [_thread("T1", line=11), _thread("T2", ours=False, line=12)]
        postable, moved = publish.validate_comments(entries, FILES, threads, d)
        assert [c["line"] for c in postable] == [12] and not moved


def test_thread_action_policy():
    with tempfile.TemporaryDirectory() as d:
        r = _body(d, "r.md", "still broken at src/Foo.cpp:12")
        threads = [
            _thread("ours_open"),
            _thread("theirs", ours=False),
            _thread("ours_resolved_by_bot", resolved=True, resolved_by="clickhouse-gh[bot]"),
            _thread("ours_resolved_by_author", resolved=True, resolved_by="author"),
            _thread("ours_resolved_by_author_replied", resolved=True, resolved_by="author"),
        ]
        entries = [
            {"action": "resolve", "thread_id": "ours_open"},
            {"action": "resolve", "thread_id": "theirs"},
            {"action": "unresolve", "thread_id": "ours_resolved_by_bot"},
            {"action": "unresolve", "thread_id": "ours_resolved_by_author"},
            {"action": "unresolve", "thread_id": "ours_resolved_by_author_replied"},
            {"action": "reply", "thread_id": "ours_resolved_by_author_replied", "body_file": r},
            {"action": "reply", "thread_id": "ours_resolved_by_author_replied", "body_file": r},  # second reply
            {"action": "reply", "thread_id": "theirs", "body_file": r},
            {"action": "resolve", "thread_id": "unknown"},
        ]
        got = [(a, t["id"]) for a, t, _ in publish.validate_thread_actions(entries, threads, d)]
        assert got == [
            ("reply", "ours_resolved_by_author_replied"),
            ("reply", "theirs"),
            ("resolve", "ours_open"),
            ("unresolve", "ours_resolved_by_bot"),
            ("unresolve", "ours_resolved_by_author_replied"),
        ]


def test_publish_posts_once_and_moves_rejected_comments_into_summary():
    with tempfile.TemporaryDirectory() as d:
        b = _body(d, "c1.md", "⚠️ problem")
        with open(os.path.join(d, "comments.json"), "w") as f:
            json.dump([{"path": "src/Foo.cpp", "line": 12, "severity": "major", "body_file": b},
                       {"path": "src/Foo.cpp", "line": 99, "severity": "major", "body_file": b}], f)
        gh = mock.MagicMock()
        gh.post_pr_review.return_value = True
        summary = publish.publish(gh, "ClickHouse/ClickHouse", 1, "abc", FILES, [], d, "---\n#### AI Review\n")
        assert gh.post_pr_review.call_count == 1
        assert [c["line"] for c in gh.post_pr_review.call_args.kwargs["comments"]] == [12]
        assert "could not be attached" in summary and "`src/Foo.cpp:99`" in summary

        gh.post_pr_review.return_value = False
        summary = publish.publish(gh, "ClickHouse/ClickHouse", 1, "abc", FILES, [], d, "---\n#### AI Review\n")
        assert "GitHub rejected the inline review" in summary


def test_reviewed_sha_marker_round_trip():
    body = "x\n" + context.REVIEWED_SHA_MARKER.format(sha="0123456789abcdef0123456789abcdef01234567")
    assert context.reviewed_sha(body) == "0123456789abcdef0123456789abcdef01234567"
    assert context.reviewed_sha("no marker") == ""


def test_linked_numbers():
    pr = {"number": 5, "body": "Closes #123, see https://github.com/ClickHouse/ClickHouse/issues/4567 "
                              "and #5 and foo#99 and other/repo#77 and #123 again"}
    assert context._linked_numbers(pr, "ClickHouse/ClickHouse") == [123, 4567]


def test_skill_section_is_embedded():
    with tempfile.TemporaryDirectory() as d:
        skill = _body(d, "SKILL.md", "# Skill\n\n## Obtaining the Diff\nfetch\n\n## Review Instructions\n\nROLE\nsee `references.md#x`\n")
        text = prompt.skill_review_instructions(skill)
        assert text.startswith("ROLE") and "Obtaining" not in text
        assert "`.claude/skills/review/references.md#x`" in text
        assert "Follow the Review Instructions" in prompt.skill_review_instructions(os.path.join(d, "missing.md"))


def test_prompt_mentions_loom_only_when_available():
    with_loom = prompt.build("u", "r", "- files", False, "- brief line", True, "out")
    without = prompt.build("u", "r", "- files", False, "", False, "out")
    assert "- brief line" in with_loom and "verify-citations out/summary.md" in with_loom
    assert "not available in this run" in without and "verify-citations out/" not in without


def test_loom_unconfigured_and_errors_fail_soft():
    assert loom.call(loom.Config(), "code.symbol", {"name": "x"}) is None
    cfg = loom.Config(base_url="http://127.0.0.1:9", token="t", namespace="code-clickhouse")
    assert loom.call(cfg, "code.symbol", {"name": "x"}) is None  # connection refused


def test_loom_refuses_private_repo_on_public_namespace():
    cfg = loom.Config(base_url="http://loom", token="t", namespace=loom.PUBLIC_NAMESPACE, private=True)
    with mock.patch.object(loom.urllib.request, "urlopen") as urlopen:
        assert loom.call(cfg, "code.symbol", {"name": "x"}) is None
        assert not urlopen.called


def test_loom_config_for_repo():
    secrets = {"/ci/loom/base_url": "http://loom/", "/ci/loom/api_key": "k"}
    cfg = loom.Config.for_repo("ClickHouse/ClickHouse", 12, secrets.__getitem__)
    assert cfg.available() and cfg.namespace == "code-clickhouse" and cfg.base_url == "http://loom"
    # A missing secret or an unknown repository means "no Loom", not a failure.
    assert not loom.Config.for_repo("ClickHouse/ClickHouse-private", 12, secrets.__getitem__).available()
    assert not loom.Config.for_repo("someone/fork", 12, secrets.__getitem__).available()


def test_loom_config_round_trips_through_env():
    cfg = loom.Config(base_url="http://loom", token="t", namespace="ns", repo="r", pr_number=3, private=True, pr_overlay=False)
    with mock.patch.dict(os.environ, cfg.env(), clear=False):
        back = loom.Config.from_env()
    assert vars(back) == vars(cfg)


def test_previous_review_is_the_review_section_of_the_ci_comment():
    body = ("<!-- CI automatic comment start :report: -->report<!-- CI automatic comment end :report: -->\n"
            "<!-- CI automatic comment start :review: -->\n---\n#### AI Review\nold\n"
            "<!-- CI automatic comment end :review: -->")
    comments = [{"body": "human"}, {"body": body}]
    assert context._previous_review(comments) == "---\n#### AI Review\nold"
    assert context._previous_review([{"body": "human"}]) == ""


def test_since_last_review_is_an_interdiff_of_the_pr():
    commits = [{"sha": "a" * 40, "parents": [1]}, {"sha": "b" * 40, "parents": [1], "commit": {"message": "Fix it\n\nbody"}},
               {"sha": "c" * 40, "parents": [1, 2]}, {"sha": "d" * 40, "parents": [1], "commit": {"message": "More"}}]
    then = {"files": [
        {"filename": "src/A.cpp", "patch": "@@ -1,2 +1,2 @@\n ctx\n-x\n+y"},
        {"filename": "src/B.cpp", "patch": "@@ -5,2 +5,2 @@\n ctx\n+z"},
        {"filename": "src/Gone.cpp", "patch": "@@ -1 +1 @@\n+g"},
    ]}
    now = [
        {"filename": "src/A.cpp", "patch": "@@ -9,2 +9,2 @@\n other context\n-x\n+y"},  # only the base moved
        {"filename": "src/B.cpp", "patch": "@@ -5,2 +5,3 @@\n ctx\n+z\n+w"},
        {"filename": "src/New.cpp", "patch": "@@ -0,0 +1 @@\n+n"},
    ]

    def fake_gh_json(endpoint, paginate=False, strict=False):
        return commits if "/commits" in endpoint else then

    with mock.patch.object(context, "gh_json", side_effect=fake_gh_json):
        text = context._render_since_last_review("r/r", 1, "master", "a" * 12, "d" * 40, now)
        assert "`bbbbbbbbbbbb` Fix it" in text and "`dddddddddddd` More" in text and "cccc" not in text
        assert "1 merge(s) of the base branch" in text
        assert "`src/A.cpp`" not in text and "`src/B.cpp`" in text
        assert "`src/Gone.cpp` (no longer changed by the PR)" in text and "`src/New.cpp` (new in the PR)" in text
        assert context._render_since_last_review("r/r", 1, "master", "d" * 12, "d" * 40, now) == ""
        assert "force-pushed" in context._render_since_last_review("r/r", 1, "master", "e" * 12, "d" * 40, now)
