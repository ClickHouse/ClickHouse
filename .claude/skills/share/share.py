#!/usr/bin/env python3
"""
Share a Claude Code session to pastila.nl and print a `.claude.jsonl` viewer link.

By default it shares the *current* session (the one running this skill, identified
by $CLAUDE_CODE_SESSION_ID). An explicit session may be passed as the first
argument, given as any of:
  - a path to a .jsonl transcript file,
  - a session id (uuid),
  - a project path or its ~/.claude/projects directory name (newest session there).

The actual upload (encryption + INSERT) is delegated to pastila.py, so none of
that logic is duplicated here -- this script only locates the transcript, runs
pastila.py on it, and turns the resulting paste URL into a session-viewer URL by
inserting the `.claude.jsonl` extension before the `#` key anchor.
"""

import os
import re
import sys
import glob
import shutil
import subprocess
from pathlib import Path

PROJECTS = Path.home() / ".claude" / "projects"


def die(msg):
    sys.stderr.write("share: " + msg + "\n")
    sys.exit(1)


def encode(path):
    # Claude Code names a project's transcript directory after its path with every
    # non-alphanumeric character replaced by '-' (e.g. /home/u/p -> -home-u-p).
    return re.sub(r"[^A-Za-z0-9]", "-", path)


def newest(paths):
    paths = [p for p in paths if Path(p).name != "history.jsonl"]
    if not paths:
        return None
    return Path(max(paths, key=lambda p: Path(p).stat().st_mtime))


def newest_in_dir(d):
    return newest(glob.glob(str(Path(d) / "*.jsonl")))


def resolve_session(arg):
    """Return the Path of the transcript to share."""
    if arg:
        p = Path(arg).expanduser()
        if p.is_file():
            return p
        # A bare session id (uuid): look it up across all projects.
        if "/" not in arg and not arg.endswith(".jsonl"):
            matches = glob.glob(str(PROJECTS / "*" / (arg + ".jsonl")))
            if matches:
                return Path(matches[0])
        # A project: either a ~/.claude/projects subdirectory name, or a project
        # path that needs encoding. Share the newest session found in it.
        for cand in (PROJECTS / arg, PROJECTS / encode(arg),
                     PROJECTS / encode(str(p if p.is_absolute() else p.resolve()))):
            if cand.is_dir():
                n = newest_in_dir(cand)
                if n:
                    return n
        die("could not resolve a session from %r" % arg)

    # No argument: the current session, identified strictly by its id. There is
    # deliberately no "newest transcript" fallback here. This skill performs an
    # external, irreversible upload of a private transcript, so when the current
    # session cannot be resolved we fail closed rather than risk publishing an
    # unrelated session that merely happens to be the most recently modified
    # (e.g. when run from a different checkout or a non-Claude shell).
    sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if not sid:
        die("CLAUDE_CODE_SESSION_ID is not set, so the current session cannot be "
            "identified. Pass an explicit transcript path, session id, or project "
            "to choose what to share.")
    matches = glob.glob(str(PROJECTS / "*" / (sid + ".jsonl")))
    if not matches:
        die("no transcript found for the current session id %s under %s. Pass an "
            "explicit transcript path, session id, or project to choose what to "
            "share." % (sid, PROJECTS))
    return Path(matches[0])


def find_pastila():
    candidates = [
        Path(__file__).resolve().parent / "pastila.py",      # bundled next to this script
        Path(__file__).resolve().parents[3] / "pastila.py",  # the pastila repo
        Path.cwd() / "pastila.py",
    ]
    on_path = shutil.which("pastila.py")
    if on_path:
        candidates.append(Path(on_path))
    for c in candidates:
        if c.is_file():
            return c
    die("cannot find pastila.py (looked next to this skill's script, in the "
        "pastila repo, the current directory, and PATH)")


def to_view_url(url):
    # Turn .../?fp/hash#key into .../?fp/hash.claude.jsonl#key
    if "#" in url:
        base, frag = url.split("#", 1)
        return base + ".claude.jsonl#" + frag
    return url + ".claude.jsonl"


def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    session = resolve_session(arg)
    pastila = find_pastila()

    data = session.read_bytes()
    proc = subprocess.run([sys.executable, str(pastila)], input=data,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        die("upload failed: " + (proc.stderr.decode(errors="replace").strip() or
                                 "pastila.py exited with %d" % proc.returncode))

    url = proc.stdout.decode().strip()
    if not url.startswith("http"):
        die("unexpected pastila.py output: " + url)

    print(to_view_url(url))
    sys.stderr.write("shared %s (%d bytes)\n" % (session, len(data)))


if __name__ == "__main__":
    main()
