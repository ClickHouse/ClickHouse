"""
The review prompt.

What to look for lives in `.claude/skills/review/SKILL.md`, shared with
interactive reviews. The prompt embeds its "Review Instructions" section
verbatim (the part before it is about fetching a diff interactively, which the
job already did), and adds what is specific to CI: the prefetched context, the
Loom code index, how to treat the existing discussion, the evidence bar, and the
output files the job posts from.
"""

SKILL_FILE = ".claude/skills/review/SKILL.md"
SKILL_REFERENCES = ".claude/skills/review/references.md"
_SKILL_SECTION = "## Review Instructions"


def skill_review_instructions(skill_path=SKILL_FILE):
    """The "Review Instructions" section of the review skill, to the end of
    the file. Falls back to a pointer when the file or section is missing."""
    try:
        with open(skill_path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return f"Follow the Review Instructions in `{skill_path}`."
    start = text.find(_SKILL_SECTION)
    if start < 0:
        return f"Follow the Review Instructions in `{skill_path}`."
    section = text[start + len(_SKILL_SECTION):].strip()
    # The section references its companion file by bare name.
    return section.replace("`references.md", f"`{SKILL_REFERENCES}")


def _intro(pr_url, repo):
    return f"""\
You are reviewing the ClickHouse pull request {pr_url} (repository `{repo}`) for the ClickHouse CI.

The CI job publishes what you write as the `clickhouse-gh` GitHub App: a summary comment, a batch of
inline comments on the diff, and replies and resolutions on existing review threads. You do not
post anything yourself and you have no GitHub access: everything the review needs from GitHub has
been fetched into the review context below. The repository is checked out at the PR head in the
current directory; read any file you need from it.

The review is for the PR author and the maintainers who decide whether to merge. A finding they can
verify in a minute from what you wrote is worth more than several they have to investigate, and a
false alarm costs them more than a missed nit."""


def _context(context_index, incremental):
    text = f"""\
# Review context

{context_index}

Use `diff.patch` as the PR diff. The local clone may not contain the base commit, so do not rely on
`git diff` or `git log` against the base branch."""
    if incremental:
        text += """

You reviewed this PR before. `since_last_review.md` lists what was pushed since. Look hardest at
those changes and at how they interact with the rest of the PR, but the summary you write must still
cover the whole PR."""
    return text


def _loom(brief, overlay):
    if not brief:
        return """\
# Code index

The Loom code index is not available in this run. Use `git grep` and read files from the checkout
to follow callers, sibling implementations and tests."""
    overlay_note = (
        " For this PR, `symbol` and `callers` also see the code the PR adds (an overlay of the PR head),"
        " but read that code itself from the checkout."
        if overlay else ""
    )
    return f"""\
# Loom code index

Loom indexes ClickHouse master: a compiler-resolved call graph, a map from code to the tests that
exercise it, and the history of PRs and issues per function. Use it for what the diff does not show:
unchanged callers, overrides and sibling implementations of what the PR changes, the tests that cover
it, and earlier bugs in the same place. This is how the "Impacted surface" gate below gets checked
without reading whole directories.

Run `python3 -m ci.jobs.scripts.ai_review.loom <command>` (`--help` on any command for options):

- `symbol NAME`: the definition with its body. Use qualified names (`DB::MergeTreeData::loadDataParts`);
  an `ambiguous` answer lists `candidates` to choose from. Reading the body from here is cheaper than
  opening the file.
- `callers NAME [--depth 2]`: call sites and overrides, from the compiler's call graph.
- `grep PATTERN [--regex] [--path-prefix src/Storages/] [--mode files]`, `search "question in words"`.
- `outline PATH`, `enclosing PATH:LINE [PATH:LINE ...]`: the functions in a file, the function around a line.
- `tests-for --setting S --function F --engine E --format F --error-code E`: existing SQL tests that
  use these; the way to answer "is this already tested" for SQL-visible behavior.
- `history --path PATH | --name NAME`, `blame PATH START END`: earlier PRs and issues, with their review discussion.
- `similar "text"`, `issue N ...`: tracker search. Reference a matching existing issue in a finding.
- `verify-citations FILE`: checks every `path:line` and name cited in a Markdown file against master.

Loom sees master, not this PR.{overlay_note} A citation of code the PR adds shows as unresolved; check
those in the checkout. If a command reports that Loom did not answer, continue with `git grep`.

The brief below was fetched for this PR. It tells you where to look; it is not evidence. Confirm in
the code before relying on any of it.

{brief.strip()}"""


def _discussion():
    return """\
# The existing discussion

In `threads.md` and `conversation.md`, comments marked "(you)" and threads marked "yours" are from
earlier runs of this review.

- Read every reply on every thread before deciding anything about it. A reply is a deliberate
  decision by the author. An explanation that holds up in the current code, a pointer to a fixing
  commit, or a tradeoff you agree with means the point is closed. A dismissal ("no", "won't fix",
  "by design", a silent resolution) is also a decision: do not argue it again on the thread. If,
  after checking the current code, you still believe the issue is real, keep it in the summary's
  Findings marked `[dismissed by author: <thread link>]` with one line on why it still matters.
- Treat your previous summary the same way: drop findings that have been addressed, keep or sharpen
  the rest. Decide by reading the current code, not by trusting a reply.
- Reply on a thread only when the author asked you a direct question you can answer (answer it once,
  without restating the finding), or when the author says an issue is fixed but the current code
  still has it (reply once with the `path:line` that shows it). "Won't fix" and "by design" are not
  claims that it is fixed.
- Resolve a thread of yours when its issue no longer holds in the current code, or when the author
  showed that the PR does not cause it (the same behavior exists on master). An issue the author
  accepts as real but declines to fix stays open and stays in the summary. Re-open a resolved
  thread of yours only when the issue is still present and either you resolved it yourself earlier
  or you are replying that the claimed fix did not fix it. Never act on threads that are not yours.
- Do not open a new inline comment for an issue that already has a thread; a new push does not make
  an old finding new."""


def _evidence():
    return """\
# What a finding needs

- Each finding names the behavior, invariant or contract that is violated and the impact, as the
  review instructions describe, and cites `path:line` in the current checkout.
- A Blocker or Major comes with its proof: the concrete input, query or sequence of events that
  triggers it, traced through the code with concrete values, or the exact caller that breaks. When
  you cannot produce that, it is not a Blocker or Major: put it in the summary as a risk that needs
  verification and say what would settle it.
- Claims about code outside the diff (that a caller relies on something, that a check exists or is
  missing elsewhere) are made after reading that code, not from names or comments.
- Do not report build or compilation failures, or style and lint issues: the build and Style Check
  jobs report those with full output (`ci_status.md` shows how they went). Mention them at most as a
  Nit in the summary."""


def _self_check(loom_available, output_dir):
    verify = (
        f"\n- Run `python3 -m ci.jobs.scripts.ai_review.loom verify-citations {output_dir}/summary.md` and fix"
        " every citation of master code it reports as unresolved or stale."
        if loom_available else ""
    )
    return f"""\
# Before writing the output

Re-read each finding as the PR author would, against the current code:

- Does the cited code, as it is now, still show the problem?
- Is there a guard, check or invariant elsewhere (a caller, an earlier stage, a constructor) that
  makes the failure impossible? Look for it before keeping the finding.
- Is the severity what the impact supports, not what the topic suggests?

Drop what does not survive. Fewer, verified findings are the goal.{verify}"""


def _output(output_dir):
    return f"""\
# Output

Write these files; the job posts them after you finish. Create `{output_dir}` if it does not exist.

1. `{output_dir}/summary.md`: a self-contained summary of every current finding, whether or not it
   also gets an inline comment, in the REQUESTED OUTPUT FORMAT of the review instructions. Start with
   `---` and `#### AI Review` on the next line, and use `#####` for section headers.

2. `{output_dir}/comments.json`: the new inline comments, as a JSON array (`[]` when there are none):

   ```json
   [{{"path": "src/Foo.cpp", "line": 120, "side": "RIGHT", "severity": "blocker", "body_file": "{output_dir}/comments/1.md"}},
    {{"path": "src/Bar.cpp", "start_line": 40, "line": 45, "side": "RIGHT", "severity": "major", "body_file": "{output_dir}/comments/2.md"}}]
   ```

   - Only Blockers and Majors go inline; Nits stay in the summary.
   - `line` must be a line inside a hunk of `diff.patch`: for `"side": "RIGHT"` (added or unchanged
     lines) its number in the file at the PR head, for `"side": "LEFT"` (deleted lines) its number in
     the base version. A range (`start_line`) must lie within one hunk. A comment on any other line
     cannot be attached; the job then moves it into the summary.
   - An issue that does not map to one line (a missing change, a design problem) goes on the most
     relevant changed line.
   - Each body file starts with ❌ (Blocker) or ⚠️ (Major) and a one-sentence statement of the
     problem, then the evidence and the suggested fix. For a small fix on RIGHT lines, a
     ```` ```suggestion ```` block is welcome.

3. `{output_dir}/thread_actions.json`: actions on existing threads, as a JSON array (`[]` when none):

   ```json
   [{{"action": "reply", "thread_id": "<thread id from threads.md>", "body_file": "{output_dir}/replies/1.md"}},
    {{"action": "resolve", "thread_id": "<thread id>"}},
    {{"action": "unresolve", "thread_id": "<thread id>"}}]
   ```

Do not call `gh` or post anything."""


def build(pr_url, repo, context_index, incremental, brief, overlay, output_dir, skill_path=SKILL_FILE):
    sections = [
        _intro(pr_url, repo),
        _context(context_index, incremental),
        _loom(brief, overlay),
        "# Review instructions\n\n" + skill_review_instructions(skill_path),
        _discussion(),
        _evidence(),
        _self_check(bool(brief), output_dir),
        _output(output_dir),
    ]
    return "\n\n".join(s.rstrip() for s in sections) + "\n"
