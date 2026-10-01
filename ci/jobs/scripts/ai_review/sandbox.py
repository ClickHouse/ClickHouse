"""
Run the review agent without any way to reach a GitHub or cloud credential.

The agent executes commands, with network access, over text a contributor
wrote. Pointing its `GH_CONFIG_DIR` at an empty directory is no boundary: it can
unset the override and read the job user's `gh` store, and the runner's AWS
role (reachable through the instance metadata service) can mint a new GitHub
token or read any CI secret. So, as `ci/jobs/revert_ci_regressions.py` does for
its investigation agent, and with the same helpers:

  * the job's `gh` token store is removed before the agent starts, and a fresh
    token is minted for publishing after it has finished (`reauthenticate`);
  * the agent runs as `AGENT_USER`, a uid of its own whose packets to the
    credential endpoints the runner's firewall rejects (checked by a probe),
    with an environment built from nothing (`env -i`); every process of that
    user is killed at the time limit and after each attempt, as that user, so
    a process that forks or ignores SIGTERM cannot survive;
  * it works in a copy of the PR head (`git archive`, so no `.git`, no git
    config and no hooks) under `/var/tmp`, outside the job user's home,
    together with a copy of the review context. Its outputs are copied back,
    regular files only and without following links, before anything reads
    them.
"""

import os
import shlex
import shutil
import stat
import subprocess
import threading

from ci.jobs.revert_ci_regressions import (
    AGENT_SCRATCH_PARENT,
    AGENT_USER,
    chown,
    confine_agent_user,
    scrub_gh_credentials,
)
from ci.praktika.gh_auth import GHAuth
from ci.praktika.utils import Shell


# The agent's working directory. A fixed path, unlike the revert job's random
# ones: Codex puts the working directory into the context it sends ahead of
# the prompt, so a random path would make every review's prompt uncacheable.
# Runners run one job at a time and `prepare` removes what an earlier job left,
# so the name can be fixed; it is created fresh by this job (mkdir fails if
# anything already holds the name) and is execute-only for others.
SCRATCH_ROOT = os.path.join(AGENT_SCRATCH_PARENT, "praktika-ai-review")


def scratch_root():
    Shell.check(f"sudo -n rm -rf {shlex.quote(SCRATCH_ROOT)}", verbose=False)
    os.mkdir(SCRATCH_ROOT, 0o711)
    os.chmod(SCRATCH_ROOT, 0o711)
    return SCRATCH_ROOT


def kill_agent():
    """Kill every process of the agent's user and wait until none is left.
    `kill -KILL -1` run as that user signals all of its processes at once, so
    unlike a scan it cannot miss a child forked meanwhile."""
    for _ in range(10):
        Shell.check(f"sudo -n -u {AGENT_USER} kill -KILL -1", verbose=False)
        if not Shell.check(f"pgrep -U {AGENT_USER} > /dev/null"):
            return
    raise RuntimeError(f"processes of {AGENT_USER} survive repeated SIGKILL")


def prepare():
    """Remove the job's GitHub credential, clear what an earlier job on this
    runner may have left, and make sure the agent's user is confined. Raises
    when the confinement cannot be established.

    Runners are reused across jobs but run one job at a time, so any agent
    process or scratch directory present now belongs to an earlier job that
    was killed before its own cleanup (a timeout, a cancelled workflow)."""
    scrub_gh_credentials()
    confine_agent_user()
    kill_agent()
    Shell.check(
        f"sudo -n find {shlex.quote(AGENT_SCRATCH_PARENT)} -maxdepth 1 \\( -name 'praktika-agent-*' -o -name "
        f"{shlex.quote(os.path.basename(SCRATCH_ROOT))} \\) -exec rm -rf {{}} +",
        verbose=True,
    )


def stop_agent():
    """Stop a running agent, e.g. when a newer commit's review has started."""
    kill_agent()


def reauthenticate():
    """Mint the token the job publishes with, after the agent has run. Also
    needed when the review fails: the runner posts the commit status and the
    CI report with `gh` after the job command, from the same token store.
    Returns whether it worked; never raises (it runs in a `finally`)."""
    try:
        return bool(GHAuth.auth(force=True, no_strict=True))
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: could not mint a GitHub token: {type(e).__name__}: {e}")
        return False


class Workspace:
    """One attempt's workspace: a copy of the PR head plus the review context,
    owned by the agent's user while the agent runs."""

    def __init__(self, root, context_dir, work_dir, commit="HEAD"):
        self.root = root
        self.attempt_dir = os.path.join(root, "attempt")
        os.mkdir(self.attempt_dir, 0o711)
        os.chmod(self.attempt_dir, 0o711)
        self.tree = os.path.join(self.attempt_dir, "tree")
        self.codex_home = os.path.join(self.attempt_dir, "codex")
        self.gh_config = os.path.join(self.attempt_dir, "gh")
        for path in (self.tree, self.codex_home, self.gh_config):
            os.makedirs(path)
        Shell.check(f"set -o pipefail; git archive --format=tar {shlex.quote(commit)} | tar -x -C {shlex.quote(self.tree)}",
                    strict=True, verbose=True)
        # The context and output directories keep their relative paths, so the
        # prompt's `./ci/tmp/ai_review/...` paths hold inside the copy. The PR
        # could ship `ci/tmp` itself, even as a link out of the tree: replace it.
        Shell.check(f"rm -rf {shlex.quote(os.path.join(self.tree, 'ci', 'tmp'))}", strict=True, verbose=False)
        self.work_dir = os.path.join(self.tree, work_dir)
        target = os.path.join(self.tree, context_dir)
        shutil.copytree(context_dir, target, symlinks=True)
        tree = os.path.realpath(self.tree)
        for path in (target, self.work_dir):
            if os.path.commonpath([tree, os.path.realpath(path)]) != tree:
                raise RuntimeError(f"{path} resolves outside the agent's tree")

    def run(self, command, env, timeout, stdin_file=None):
        """Run `command` (a list) in the tree as the agent's user, with an
        environment built from `env` alone and stdin read from `stdin_file` by
        the job's own shell. The values go through a file in the attempt
        directory, so the logged command line never carries them. At
        `timeout` every process of the agent's user is killed: the timeout of
        `Shell.run` signals as the job user, which cannot reach them."""
        env_file = os.path.join(self.attempt_dir, "agent.env")
        with open(env_file, "w", encoding="utf-8") as f:
            for k, v in env.items():
                f.write(f"export {k}={shlex.quote(v)}\n")
        os.chmod(env_file, 0o600)
        timer = threading.Timer(timeout, kill_agent)
        try:
            kill_agent()
            chown(f"{AGENT_USER}:", self.attempt_dir)
            redirect = f" < {shlex.quote(stdin_file)}" if stdin_file else ""
            inner = f". {shlex.quote(env_file)} && exec \"$@\""
            timer.start()
            return Shell.run(
                f"cd {shlex.quote(self.tree)} && sudo -n -u {AGENT_USER} env -i /bin/sh -c {shlex.quote(inner)} sh "
                + " ".join(shlex.quote(c) for c in command) + redirect,
                timeout=timeout + 60,
                verbose=True,
            )
        finally:
            timer.cancel()
            kill_agent()
            chown(f"{os.getuid()}:{os.getgid()}", self.attempt_dir)
            Shell.check(f"chmod -R go-w {shlex.quote(self.attempt_dir)}", verbose=False)

    def collect(self, rel_path, dest):
        """Copy the agent's `rel_path` directory to `dest`: regular files and
        directories only, each file opened without following links and checked
        on the open descriptor. A link the agent left there could otherwise
        make the job read (and publish) or write a file outside the output."""
        src = os.path.join(self.tree, rel_path)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        if not os.path.isdir(src) or os.path.islink(src):
            return
        os.makedirs(dest)
        for name in os.listdir(src):
            path, target = os.path.join(src, name), os.path.join(dest, name)
            mode = os.lstat(path).st_mode
            if stat.S_ISDIR(mode):
                self.collect(os.path.join(rel_path, name), target)
                continue
            if not stat.S_ISREG(mode):
                print(f"WARNING: dropping {path} from the agent's output: not a regular file")
                continue
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            except OSError as e:
                print(f"WARNING: dropping {path} from the agent's output: {e}")
                continue
            with os.fdopen(fd, "rb") as f:
                if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
                    continue
                with open(target, "wb") as out:
                    shutil.copyfileobj(f, out)


def pr_head_commit(sha):
    """The commit to review: the PR head, fetched if the checkout (the PR
    merged into its base) does not contain it; the checkout's HEAD if the
    head cannot be had. Line numbers in the diff are those of the PR head."""
    if sha and not Shell.check(f"git cat-file -e {shlex.quote(sha)}^{{commit}} 2>/dev/null"):
        Shell.check(f"timeout 300 git fetch -q --no-tags --depth=1 origin {shlex.quote(sha)}", verbose=True)
    if sha and Shell.check(f"git cat-file -e {shlex.quote(sha)}^{{commit}} 2>/dev/null"):
        return sha
    print(f"WARNING: PR head {sha[:12] if sha else '?'} not available; reviewing the checkout's HEAD")
    return "HEAD"


def codex_login(codex, codex_home, openai_key):
    subprocess.run(
        [codex, "login", "--with-api-key"], input=openai_key, text=True, check=True,
        env={**os.environ, "CODEX_HOME": codex_home},
    )


__all__ = ["AGENT_USER", "Workspace", "codex_login", "kill_agent", "pr_head_commit", "prepare", "reauthenticate",
           "scratch_root", "stop_agent"]
