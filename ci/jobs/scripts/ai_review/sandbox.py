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
    with an environment built from nothing (`env -i`), and no process of that
    user survives an attempt;
  * it works in a disposable copy of the checked-out tree (`git archive`, so
    no `.git`, no git config and no hooks) under an unlistable directory in
    `/var/tmp`, outside the job user's home, together with a copy of the
    review context. Its outputs are copied back into the job's output
    directory, without following symlinks, before anything reads them.
"""

import os
import shlex
import shutil
import subprocess

from ci.jobs.revert_ci_regressions import (
    AGENT_SCRATCH_PARENT,
    AGENT_USER,
    chown,
    confine_agent_user,
    kill_agent_processes,
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


def prepare():
    """Remove the job's GitHub credential, clear what an earlier job on this
    runner may have left, and make sure the agent's user is confined. Raises
    when the confinement cannot be established.

    Runners are reused across jobs but run one job at a time, so any agent
    process or scratch directory present now belongs to an earlier job that
    was killed before its own cleanup (a timeout, a cancelled workflow)."""
    scrub_gh_credentials()
    confine_agent_user()
    kill_agent_processes()
    Shell.check(
        f"sudo -n find {shlex.quote(AGENT_SCRATCH_PARENT)} -maxdepth 1 \\( -name 'praktika-agent-*' -o -name "
        f"{shlex.quote(os.path.basename(SCRATCH_ROOT))} \\) -exec rm -rf {{}} +",
        verbose=True,
    )


def stop_agent():
    """Stop a running agent (all processes of its user), e.g. when a newer
    commit makes its review pointless."""
    kill_agent_processes()


def reauthenticate():
    """Mint the token the job publishes with, after the agent has run. Also
    needed when the review fails: the runner posts the commit status and the
    CI report with `gh` after the job command, from the same token store."""
    if not GHAuth.auth(force=True, no_strict=True):
        raise RuntimeError("could not mint a GitHub token to publish the review")


class Workspace:
    """One attempt's workspace: a copy of the tree plus the review context,
    owned by the agent's user while the agent runs."""

    def __init__(self, root, context_dir, work_dir):
        self.root = root
        self.attempt_dir = os.path.join(root, "attempt")
        os.mkdir(self.attempt_dir, 0o711)
        os.chmod(self.attempt_dir, 0o711)
        self.tree = os.path.join(self.attempt_dir, "tree")
        self.codex_home = os.path.join(self.attempt_dir, "codex")
        self.gh_config = os.path.join(self.attempt_dir, "gh")
        for path in (self.tree, self.codex_home, self.gh_config):
            os.makedirs(path)
        Shell.check(f"git archive --format=tar HEAD | tar -x -C {shlex.quote(self.tree)}", strict=True, verbose=True)
        # The context and output directories keep their relative paths, so the
        # prompt's `./ci/tmp/ai_review/...` paths hold inside the copy.
        self.work_dir = os.path.join(self.tree, work_dir)
        shutil.copytree(context_dir, os.path.join(self.tree, context_dir), symlinks=True)

    def run(self, command, env, timeout, stdin_file=None):
        """Run `command` (a list) in the tree as the agent's user, with an
        environment built from `env` alone and stdin read from `stdin_file` by
        the job's own shell. The values go through a file in the attempt
        directory, so the logged command line (and the job log with the
        agent's output) never carries them; `env` holds the Loom token, which
        the agent gets anyway."""
        env_file = os.path.join(self.attempt_dir, "agent.env")
        with open(env_file, "w", encoding="utf-8") as f:
            for k, v in env.items():
                f.write(f"export {k}={shlex.quote(v)}\n")
        os.chmod(env_file, 0o600)
        try:
            kill_agent_processes()
            chown(f"{AGENT_USER}:", self.attempt_dir)
            redirect = f" < {shlex.quote(stdin_file)}" if stdin_file else ""
            inner = f". {shlex.quote(env_file)} && exec \"$@\""
            return Shell.run(
                f"cd {shlex.quote(self.tree)} && sudo -n -u {AGENT_USER} env -i /bin/sh -c {shlex.quote(inner)} sh "
                + " ".join(shlex.quote(c) for c in command) + redirect,
                timeout=timeout,
                verbose=True,
            )
        finally:
            kill_agent_processes()
            chown(f"{os.getuid()}:{os.getgid()}", self.attempt_dir)

    def collect(self, rel_path, dest):
        """Copy the agent's `rel_path` directory to `dest`, keeping only
        regular files and directories. A symlink the agent left there could
        make the job read (and publish) or write a file outside the output
        directory, so links and special files are dropped, not followed."""
        src = os.path.join(self.tree, rel_path)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        if not os.path.isdir(src) or os.path.islink(src):
            return
        shutil.copytree(src, dest, symlinks=True)
        for root, dirs, files in os.walk(dest):
            for name in dirs + files:
                path = os.path.join(root, name)
                if os.path.islink(path) or not (os.path.isdir(path) or os.path.isfile(path)):
                    print(f"WARNING: dropping {path} from the agent's output: not a regular file")
                    os.unlink(path)

    def remove(self):
        Shell.check(f"rm -rf {shlex.quote(self.attempt_dir)}", verbose=False)


def codex_login(codex, codex_home, openai_key):
    subprocess.run(
        [codex, "login", "--with-api-key"], input=openai_key, text=True, check=True,
        env={**os.environ, "CODEX_HOME": codex_home},
    )


__all__ = ["AGENT_USER", "Workspace", "codex_login", "prepare", "reauthenticate", "scratch_root", "stop_agent"]
