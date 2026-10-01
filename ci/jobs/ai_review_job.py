"""
AI-based automated PR code review job, run with the OpenAI Codex CLI (auth:
`OPENAI_API_KEY` from `/ci/llm/openai_api_key`).

A run has three stages, and only the middle one involves the agent:

1. Context. The job fetches the PR, its diff, review threads, conversation,
   linked issues, CI status and the previous AI review into `CONTEXT_DIR`
   (`ai_review/context.py`), and a Loom code-index brief for the diff
   (`ai_review/loom.py`) when Loom is configured for the repository.
2. Review. The agent reads the context and the checkout, may query Loom through
   `python3 -m ci.jobs.scripts.ai_review.loom`, and writes its summary, inline
   comments and thread actions as files into `OUTPUT_DIR`. The agent runs as a
   user of its own in a copy of the tree, with no GitHub token and no route to
   the runner's cloud credentials (`ai_review/sandbox.py`); the job mints a
   fresh token to publish. An attempt is retried when the agent fails or its output is
   incomplete; nothing has been posted at that point, so a retry cannot
   duplicate comments.
3. Publish. The job validates the inline comments against the diff and the
   thread actions against the thread ownership rules (`ai_review/publish.py`),
   posts the inline comments as one review, applies the thread actions, and
   posts the summary via `post-or-update --tag review` as the pre-authenticated
   app, with a hidden marker of the reviewed commit for the next run.
"""

import json
import os
import re
import shlex
import shutil
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request

from ci.jobs.scripts.ai_review import context as review_context
from ci.jobs.scripts.ai_review import loom, prompt, publish, sandbox
from ci.praktika import Secret
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

WORK_DIR = "./ci/tmp/ai_review"
CONTEXT_DIR = f"{WORK_DIR}/context"
LOOM_DIR = f"{CONTEXT_DIR}/loom"
OUTPUT_DIR = f"{WORK_DIR}/out"
SUMMARY_FILE = f"{OUTPUT_DIR}/summary.md"
PROMPT_FILE = f"{WORK_DIR}/prompt.md"
LOOM_CALL_LOG = f"{WORK_DIR}/loom_calls.jsonl"

MODEL = "gpt-6.1-sol"
REASONING_EFFORT = "high"

# Number of attempts at a full agent run. The agents make model-provider API
# calls during execution, which can hit transient 5xx errors that no single
# inner subprocess controls. Retrying the whole run is the only reliable way
# to recover.
MAX_ATTEMPTS = 3
# Wall-clock limit of one attempt, and the point after which no new attempt is
# started. A hung agent otherwise holds the runner until the job timeout.
ATTEMPT_TIMEOUT_SECONDS = 50 * 60
NO_NEW_ATTEMPT_AFTER_SECONDS = 100 * 60
# How often the job checks, while the agent runs, whether a newer commit was
# pushed. A review of a superseded commit is stopped: the newer commit's run
# reviews the PR anyway, and in parallel (the workflow's concurrency group is
# per commit, so a push does not cancel the previous commit's run).
SUPERSEDED_CHECK_SECONDS = 120

# OpenAI API key for the Codex CLI, written into `$CODEX_HOME/auth.json`
# via `codex login --with-api-key`.
OPENAI_KEY_SECRET = "/ci/llm/openai_api_key"


def _repo_from_pr_url(pr_url):
    path_parts = urllib.parse.urlparse(pr_url).path.strip("/").split("/")
    if len(path_parts) >= 4 and path_parts[2] == "pull":
        return f"{path_parts[0]}/{path_parts[1]}"
    return ""


def _pr_repository(info):
    # Always derive the PR repository from the PR URL or the CI event, never
    # from the local checkout remote, which may point to a fork.
    return _repo_from_pr_url(info.pr_url) or info.repo_name


def _ssm(name):
    return Secret.Config(name=name, type=Secret.Type.AWS_SSM_PARAMETER, region="us-east-1").get_value()


def _reset_output_dir():
    """Remove the previous attempt's outputs so they cannot be mistaken for the
    result of a later failed attempt."""
    Shell.check(f"rm -rf {shlex.quote(OUTPUT_DIR)}", verbose=False)
    for sub in ("comments", "replies", "simplicity"):
        os.makedirs(f"{OUTPUT_DIR}/{sub}", exist_ok=True)


def _run_codex_once(loom_config, commit="HEAD"):
    """One attempt: `codex login` + `codex exec`, confined (see `sandbox.py`).

    Codex stores credentials in `$CODEX_HOME/auth.json` and does NOT consult
    `OPENAI_API_KEY` directly when invoked — you have to run
    `codex login --with-api-key` first, which reads the key from stdin and
    writes it into `auth.json`. The login runs as the job user in the
    attempt's scratch directory, which is then handed to the agent's user.

    The agent runs as `sandbox.AGENT_USER` in a copy of the tree, with an
    empty environment apart from what is listed below, no GitHub credential
    and no route to the runner's cloud credentials. Its outputs are copied
    back into `OUTPUT_DIR`.
    """
    codex = shutil.which("codex")
    if not codex:
        raise RuntimeError("the codex CLI is not installed on this runner")
    root = sandbox.scratch_root()
    try:
        ws = sandbox.Workspace(root, CONTEXT_DIR, WORK_DIR, commit)
        for sub in ("out/comments", "out/replies", "out/simplicity", "scratch"):
            os.makedirs(os.path.join(ws.work_dir, sub), exist_ok=True)
        openai_key = _ssm(OPENAI_KEY_SECRET)
        _mask(openai_key)
        sandbox.codex_login(codex, ws.codex_home, openai_key)
        agent_log = os.path.join(ws.work_dir, "loom_calls.jsonl")
        env = {
            "HOME": ws.codex_home,
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LANG": "C.UTF-8",
            "CODEX_HOME": ws.codex_home,
            "GH_CONFIG_DIR": ws.gh_config,
            "PYTHONPATH": ws.tree,
            "LOOM_CALL_LOG": agent_log,
            **loom_config.env(),
        }
        # -s workspace-write: writable workspace + /tmp + CODEX_HOME,
        #   read-only elsewhere; sufficient for the review output.
        # sandbox_workspace_write.network_access: the agent's commands reach
        #   the network only when Loom is configured, for the Loom CLI; the
        #   agent needs nothing else from the network (Codex's own model calls
        #   do not depend on this).
        # approval_policy=never: codex `exec` is non-interactive,
        #   but the approval policy still applies; "never" lets the
        #   agent execute without blocking on an approval request.
        # --color never: no ANSI codes in the job log.
        # project_doc_max_bytes=0: do not load AGENTS.md, written for agents
        #   that change code (build, test, commit); the prompt carries the
        #   few wording rules from it that matter for a review.
        # --skip-git-repo-check: the tree is a `git archive` copy, deliberately
        #   without `.git`, and `codex exec` refuses to start outside a repository.
        # `-` reads the prompt from stdin (redirected by the job's shell),
        #   which has no argument size limit.
        command = [
            codex, "exec", "-m", MODEL, "-c", f"model_reasoning_effort={REASONING_EFFORT}",
            "-s", "workspace-write",
            "-c", f"sandbox_workspace_write.network_access={'true' if loom_config.available() else 'false'}",
            "-c", "approval_policy=never", "-c", "project_doc_max_bytes=0",
            "--color", "never", "--skip-git-repo-check", "-",
        ]
        exit_code = ws.run(command, env, ATTEMPT_TIMEOUT_SECONDS, stdin_file=os.path.abspath(PROMPT_FILE))
        ws.collect(os.path.join(WORK_DIR, "out"), OUTPUT_DIR)
        if os.path.isfile(agent_log) and not os.path.islink(agent_log):
            with open(agent_log, "r", encoding="utf-8", errors="replace") as src, \
                    open(LOOM_CALL_LOG, "a", encoding="utf-8") as dst:
                dst.write(src.read())
        return exit_code
    finally:
        Shell.check(f"rm -rf {shlex.quote(root)}", verbose=False)


class Superseded(Exception):
    """A newer commit was pushed; its own run reviews the PR."""


def _mask(value):
    """Keep a secret out of the public job log, which also carries the agent's
    output (GitHub Actions replaces masked values with ***)."""
    if value:
        print(f"::add-mask::{value}")


class _SupersededWatch:
    """Watches whether a newer commit's review has taken over this one.

    A newer head alone is not enough: its Code Review may never run (it is
    skipped when Style check or Fast test fails, or by labels), and stopping
    this review would then leave the PR unreviewed. So this review is
    superseded only once the newer head's Code Review is running or has
    succeeded. GitHub is asked with an installation token minted in this
    process (and refreshed before it expires), which the agent, running as
    another user, cannot read."""

    def __init__(self, repo, pr_number, sha):
        from ci.praktika.gh_auth import GHTokenProvider

        self.api = f"https://api.github.com/repos/{repo}"
        self.pr_number = pr_number
        self.sha = sha
        self._token = GHTokenProvider()
        self.superseded = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _get(self, path):
        try:
            token = self._token()
        except Exception:  # noqa: BLE001 - the public repository can be read without a token
            token = ""
        request = urllib.request.Request(f"{self.api}{path}", headers={
            "Accept": "application/vnd.github+json", **({"Authorization": f"Bearer {token}"} if token else {})})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode())
        except Exception as e:  # noqa: BLE001 - a failed check is not a reason to stop the review
            print(f"WARNING: could not ask GitHub about the PR: {type(e).__name__}")
            return {}

    def head(self):
        return ((self._get(f"/pulls/{self.pr_number}") or {}).get("head") or {}).get("sha") or ""

    def newer_review(self):
        """The status of the Code Review of the PR head when that head is not
        this run's own commit: "running", "succeeded" or "" (this run is the
        head's own review, or the head's review has not started)."""
        head = self.head()
        if not self.sha or not head or head == self.sha:
            return ""
        runs = (self._get(f"/commits/{head}/check-runs?check_name=Code%20Review") or {}).get("check_runs") or []
        statuses = {(r.get("status"), r.get("conclusion")) for r in runs if isinstance(r, dict)}
        if ("completed", "success") in statuses:
            return "succeeded"
        if any(status == "in_progress" for status, _ in statuses):
            return "running"
        return ""

    def _loop(self):
        while not self._stop.wait(SUPERSEDED_CHECK_SECONDS):
            if not self.superseded.is_set() and self.newer_review():
                print(f"The review of a newer commit has started; stopping the review of {self.sha[:12]}")
                self.superseded.set()
            if self.superseded.is_set():
                sandbox.stop_agent()  # again on every tick, in case an attempt was starting

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()


def _outputs_problem():
    """Why the agent's output cannot be published, or "" when it can. All three
    files are required (the JSON ones as `[]` when empty): the prompt has the
    agent write the summary last, so a complete set means the run finished."""
    for name in ("coverage.json", "comments.json", "thread_actions.json"):
        path = f"{OUTPUT_DIR}/{name}"
        if not os.path.exists(path):
            return f"agent did not write {path}"
        try:
            with open(path, "r", encoding="utf-8") as f:
                if not isinstance(json.load(f), list):
                    return f"{path} is not a JSON array"
        except ValueError as e:
            return f"{path} is not valid JSON: {e}"
    if not os.path.exists(SUMMARY_FILE):
        return f"agent did not write {SUMMARY_FILE}"
    if os.path.getsize(SUMMARY_FILE) == 0:
        return f"{SUMMARY_FILE} is empty"
    return ""


def _run_agent(loom_config, watch=None, commit="HEAD"):
    """Run the agent until it produces publishable output. Returns the model
    that produced it. Raises otherwise."""
    started = time.time()
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if attempt > 1 and time.time() - started > NO_NEW_ATTEMPT_AFTER_SECONDS:
            print(f"Not starting attempt {attempt}: {int(time.time() - started)}s already spent")
            break
        if watch and watch.superseded.is_set():
            raise Superseded()
        _reset_output_dir()
        attempt_started = time.time()
        print(f"Codex attempt {attempt}/{MAX_ATTEMPTS} with {MODEL} ({REASONING_EFFORT})")
        try:
            exit_code = _run_codex_once(loom_config, commit)
            problem = _outputs_problem()
            if exit_code != 0 and problem:
                last_error = f"Codex exited with code {exit_code}: {problem}"
            elif problem:
                last_error = problem
            else:
                if exit_code != 0:
                    # All outputs are there and the summary is written last,
                    # so the run finished; a non-zero exit after that is a CLI
                    # shutdown issue.
                    print(f"WARNING: Codex exited with code {exit_code} after writing complete output")
                return MODEL
        except Exception as e:  # noqa: BLE001 — broad catch: any exception is retryable here
            last_error = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        if watch and watch.superseded.is_set():
            raise Superseded()
        print(f"WARNING: Codex attempt {attempt}/{MAX_ATTEMPTS} failed: {last_error}")
        if time.time() - attempt_started >= ATTEMPT_TIMEOUT_SECONDS - 60:
            # It ran out of time; another attempt would most likely do the
            # same and double the cost.
            print("Not retrying: the attempt hit the time limit")
            break
        if attempt < MAX_ATTEMPTS:
            delay = min(2 ** attempt, 60)
            print(f"Retrying Codex in {delay}s ...")
            time.sleep(delay)
    raise RuntimeError(f"Codex review failed: {last_error}")


_MARKER_RE = re.compile(r"\n*<!-- ai-review-(?:reviewed-sha|model|state): [^>]*-->")


def _verified_memory(repo, records):
    """The recalled memory records rebuilt from GitHub, dropping those GitHub
    does not confirm.

    The agent holds the Loom token, so a prompt-injected agent could write a
    record that claims an author dismissed some finding, or rewrite one. Of a
    record only the comment id is used: the comment must exist and have been
    posted by the app, and the finding, the thread's state and path and every
    reply by a person are taken from GitHub."""
    out = []
    pr_threads = {}  # one listing per earlier PR, however many of its threads were recalled
    for r in records or []:
        comment = review_context.gh_json(f"/repos/{repo}/pulls/comments/{r['comment_id']}") or {}
        pr = (comment.get("pull_request_url") or "").rsplit("/", 1)[-1]
        if not (pr.isdigit() and review_context.is_bot((comment.get("user") or {}).get("login"))):
            print(f"Memory record for comment {r['comment_id']} not confirmed by GitHub; not used")
            continue
        if pr not in pr_threads:
            try:
                pr_threads[pr] = GH.list_pr_review_threads(pr=int(pr), repo=repo)
            except Exception as e:  # noqa: BLE001 - unverifiable means not used
                print(f"WARNING: could not list the review threads of PR #{pr}: {e}")
                pr_threads[pr] = []
        thread = next((t for t in pr_threads[pr]
                       if ((t.get("comments") or {}).get("nodes") or [{}])[0].get("databaseId") == r["comment_id"]), None)
        if not thread:
            print(f"Memory record for comment {r['comment_id']} not confirmed by GitHub; not used")
            continue
        replies = [((c.get("author") or {}).get("login") or "?", review_context.untrusted(c.get("body")))
                   for c in thread["comments"]["nodes"][1:]
                   if (c.get("body") or "").strip() and not c.get("viewerDidAuthor")
                   and not review_context.is_automation((c.get("author") or {}).get("login"))]
        out.append({"path": thread.get("path") or comment.get("path") or "", "pr": pr,
                    "state": loom.thread_state(thread), "comment_id": r["comment_id"],
                    "finding": review_context.untrusted(comment.get("body")), "replies": replies})
    return out


def _print_loom_usage():
    """One line per Loom operation used in this run (the job's and the
    agent's): calls, failures and latency."""
    stats = {}
    try:
        with open(LOOM_CALL_LOG, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    c = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(c, dict):
                    continue
                st = stats.setdefault(str(c.get("op")), {"n": 0, "failed": 0, "ms": 0})
                st["n"] += 1
                st["failed"] += c.get("status") != "ok"
                st["ms"] += int(c.get("ms") or 0) if str(c.get("ms") or "0").isdigit() else 0
    except OSError:
        return
    for op, st in sorted(stats.items(), key=lambda kv: -kv[1]["n"]):
        print(f"Loom usage: {op}: {st['n']} call(s), {st['failed']} failed, {st['ms'] // max(st['n'], 1)} ms average")


def _strip_markers(text):
    return _MARKER_RE.sub("", text or "").rstrip()


def _state_marker(previous_state, units, activity):
    from ci.jobs.scripts.ai_review import units as review_units

    return review_units.encode_state(units, (previous_state or {}).get("findings") or [],
                                     (previous_state or {}).get("contract", ""), activity)


def _post_summary(summary, head_sha, model, state=""):
    """Post the summary as the updateable `review` comment. Raises on failure,
    failing the job. The hidden markers tell the next run which commit this
    review saw, and let reviews be compared by model."""
    body = summary.rstrip() + "\n"
    if state:
        body += "\n" + state + "\n"
    body += "\n" + review_context.REVIEWED_SHA_MARKER.format(sha=head_sha) + "\n"
    if model:
        body += f"<!-- ai-review-model: {model} -->\n"
    path = f"{WORK_DIR}/summary_to_post.md"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    Shell.check(
        f"{shlex.quote(sys.executable)} -m ci.praktika.gh post-or-update --tag {review_context.REVIEW_COMMENT_TAG} "
        f"--file {shlex.quote(path)}",
        strict=True,
    )


def review():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return []

    repo = _pr_repository(info)
    Shell.check(f"rm -rf {shlex.quote(WORK_DIR)}", verbose=False)
    os.makedirs(WORK_DIR, exist_ok=True)

    ctx = review_context.fetch(CONTEXT_DIR, repo, info.pr_number)
    # The context is the PR as it is now, so a run whose commit was already
    # superseded reviews the newer head. It stands down only when that head's
    # own review is running or done: that one may never run at all (Style
    # check or Fast test failed, a label), and the PR would go unreviewed.
    watch = _SupersededWatch(repo, info.pr_number, info.sha)
    if ctx.head_sha != info.sha and watch.newer_review():
        print(f"Not reviewing: the review of the PR head {ctx.head_sha[:12]} has already started")
        return []
    if ctx.nothing_new:
        # A merge of the base branch, a rebase or a re-run that leaves the
        # PR's diff as it was, with nothing written since: the previous review
        # still stands. Only the markers move to this commit.
        print("Nothing changed since the previous review; keeping it")
        previous_model = re.search(r"<!-- ai-review-model: ([\w.-]+) -->", ctx.previous_review or "")
        _post_summary(_strip_markers(ctx.previous_review), ctx.head_sha, previous_model.group(1) if previous_model else "",
                      state=_state_marker(ctx.previous_state, ctx.units, ctx.activity))
        return []

    loom_config = loom.Config.for_repo(repo, info.pr_number, _ssm)
    os.environ["LOOM_CALL_LOG"] = os.path.abspath(LOOM_CALL_LOG)
    # Loom is an aid: whatever goes wrong there, the review runs without it.
    try:
        brief = loom.write_brief(loom_config, ctx.pr, ctx.files, LOOM_DIR)
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: Loom brief failed: {type(e).__name__}: {e}")
        brief = ""
    print(f"Loom brief: {'written' if brief else 'not available'}")
    try:
        memory = loom.recall_outcomes(
            loom_config, info.pr_number, loom.source_first([f["filename"] for f in ctx.files]))
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: Loom memory recall failed: {type(e).__name__}: {e}")
        memory = []
    memory = _verified_memory(repo, memory)
    memory_md = loom.render_outcomes(memory)
    if memory_md:
        with open(f"{CONTEXT_DIR}/memory.md", "w", encoding="utf-8") as f:
            f.write("# Earlier review findings on the files this PR changes\n\n" + memory_md)
    print(f"Loom memory: {len(memory)} earlier finding(s) recalled")

    # A backport copies reviewed code; simplicity findings there are noise.
    is_backport = (ctx.pr.get("title") or "").startswith("Backport") or any(
        (label.get("name") or "") == "pr-backport" for label in ctx.pr.get("labels") or [])
    text = prompt.build(
        pr_url=info.pr_url,
        repo=repo,
        context_index=review_context.index_markdown(CONTEXT_DIR),
        incremental=os.path.exists(f"{CONTEXT_DIR}/since_last_review.md"),
        brief=brief,
        overlay=loom_config.pr_overlay,
        output_dir=OUTPUT_DIR,
        loom_available=loom_config.available(),
        simplicity=not is_backport,
    )
    with open(PROMPT_FILE, "w", encoding="utf-8") as f:
        f.write(text)

    Shell.check("codex --version", verbose=True)
    commit = sandbox.pr_head_commit(ctx.head_sha)
    _mask(loom_config.token)
    # From here until the agent is done, the job's gh store holds no token
    # (the watcher keeps its own in memory). It is minted again whatever
    # happens: publishing needs it, and so does the runner, which posts the
    # commit status after the job command.
    try:
        sandbox.prepare()
        with watch:
            model = _run_agent(loom_config, watch, commit)
    except Superseded:
        print("Review stopped: the review of a newer commit has started")
        return []
    finally:
        if not sandbox.reauthenticate():
            print("ERROR: no GitHub token after the review; publishing will fail")
        _print_loom_usage()

    # A newer commit's review may have started or finished while this one ran
    # (the agent can finish between two watcher ticks). It reviews the newer
    # head and publishes for it; this older review must not post comments it
    # did not account for, or overwrite its summary.
    if watch.superseded.is_set() or watch.newer_review():
        print("Not publishing: the review of a newer commit has taken over")
        return []

    # Re-read the threads: the author may have replied or resolved while the
    # agent ran, and thread actions are checked against the current state.
    try:
        threads = GH.list_pr_review_threads(pr=info.pr_number, repo=repo)
    except Exception as e:  # noqa: BLE001
        # Thread actions are authorized by the current thread state; a stale
        # snapshot could re-open a thread the author has since resolved. Post
        # the review and the summary, but change no thread.
        print(f"WARNING: failed to re-read review threads, applying no thread actions: {e}")
        threads = ctx.threads
        action_file = f"{OUTPUT_DIR}/thread_actions.json"
        if os.path.exists(action_file):
            os.unlink(action_file)

    summary, _ = publish._read_body({"body_file": SUMMARY_FILE}, OUTPUT_DIR)
    summary = publish.publish(GH, repo, info.pr_number, ctx.head_sha, ctx.files, threads, OUTPUT_DIR, summary,
                              ctx.units, ctx.previous_state, simplicity=not is_backport, activity=ctx.activity)
    _post_summary(summary, ctx.head_sha, model)

    # Record every review thread of ours, with its current state and replies,
    # in the review's Loom memory. Best effort: the review is already posted.
    try:
        threads = GH.list_pr_review_threads(pr=info.pr_number, repo=repo)
        written = loom.record_threads(loom_config, repo, info.pr_number, threads, review_context.thread_is_ours)
        print(f"Loom memory: {written} review thread record(s) written")
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: recording review threads in Loom failed: {e}")

    return [p for p in (PROMPT_FILE, SUMMARY_FILE, LOOM_CALL_LOG) if os.path.exists(p)]


if __name__ == "__main__":
    status = Result.Status.OK
    info = ""
    files = []
    try:
        files = review()
    except Exception as e:
        info = f"ERROR: {e}"
        print(info)
        traceback.print_exc()
        status = Result.Status.FAIL

    Result.create_from(status=status, info=info, files=files).complete_job()
