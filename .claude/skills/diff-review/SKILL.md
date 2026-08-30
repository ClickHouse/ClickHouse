---
name: diff-review
description: In-browser review of pending changes before committing or opening a PR. Starts a local server on localhost:3000 that renders the working-tree diff with @pierre/diffs CodeView, opens the user's browser, and waits for their line comments — which must then be read back and addressed. Works from a browser on the same machine, or from the user's own machine through an SSH-forwarded port (the server prints the `ssh -L` command); where nobody opens the page within 120 s, it exits on its own. Use when the user asks for a browser review of the changes.
---

# diff-review — let the user review changes in the browser

The user wants to read and review code changes in a browser UI before anything is
committed or pushed. This skill serves the diff on localhost, collects the user's
line comments, and hands them back to you as JSON. You must address every comment.

## When not to use it

The server binds to loopback, so the UI is reachable from a browser on the very
machine the server runs on — or from the user's own machine once they forward
the port (when the machine looks remote, the server prints the exact `ssh -L`
command to do that). What it cannot survive is an environment where nobody will
ever open the page: an unattended run on an isolated VM. There the server exits
4 on its own after 120 s (see **Exit code 4** below). Don't reach for this skill
when there is no user around to open a browser: show the diff in the terminal
with `git diff` / `git show`, or just open the pull request and let review
happen there.

## How to run

1. Start the server **in the background** (Bash tool with `run_in_background: true`):

   ```bash
   node .claude/skills/diff-review/server.mjs \
     --repo <repo-root> \
     --base HEAD \
     --port 3000 \
     --out /tmp/diff-review-comments.json
   ```

   - `--base <ref>` — what to diff against. `HEAD` (default) reviews uncommitted
     work: staged + unstaged + untracked files.
   - `--committed` — review already-committed branch work before a PR: diffs
     `<base>..HEAD` and ignores the working tree, so uncommitted or untracked
     local edits never leak into the review. Combine it with the merge base
     against the repo's default branch, e.g.
     `--base $(git merge-base origin/master HEAD) --committed`.
   - The server prints the URL, opens the user's browser automatically, and stays
     alive until the user clicks **Finish review** in the UI, then exits 0.
   - On a remote-looking machine (an SSH session, a cloud instance, no graphical
     session) it starts all the same, but prints an `ssh -L` port-forwarding
     command for the user to review from their own machine, and exits 4 if no
     browser opens the page within 120 s.
   - The UI is fully self-contained: `@pierre/diffs` is vendored (see
     `vendor/README.md`), so no network access is needed and no third-party CDN
     ever sees the diff.

2. Tell the user the review is ready at `http://localhost:3000` (and relay the
   port-forwarding command if the server printed one), then **wait**. Do not
   fetch the URL yourself — a request from you would count as the browser the
   server is waiting for. Do not proceed with the commit/PR — you will be
   notified when the background command exits (i.e. when the user submits the
   review, or when nobody opened the page in time).

3. Read the `--out` JSON file. Shape:

   ```json
   {
     "verdict": "approve" | "request_changes",
     "overall": "free-text overall comment (may be empty)",
     "comments": [
       { "file": "src/app.ts", "side": "new", "startLine": 12, "endLine": 15,
         "comment": "the user's comment text" }
     ]
   }
   ```

   `side: "new"` means line numbers refer to the new version of the file;
   `side: "old"` refers to the base version (a comment on deleted lines).

4. **Address every comment.** They are the user's code review. For each one,
   quote it briefly, then either fix the code or answer the question. After making
   fixes, offer another review round (re-run the skill) before committing.

5. `verdict: "approve"` with no comments → proceed with the commit / PR.
   `verdict: "request_changes"` → do NOT commit until fixes are made and the user
   is satisfied.

## Troubleshooting

- **Exit code 3** — nothing to review against the chosen `--base`.
- **Exit code 4** — the machine looked remote (an SSH session
  (`SSH_CONNECTION`/`SSH_CLIENT`/`SSH_TTY`), cloud-init state or a cloud vendor
  in the DMI strings, or a Linux session with neither `DISPLAY` nor
  `WAYLAND_DISPLAY`) and no browser opened the review page within 120 s, despite
  the printed port-forwarding command. This is the expected outcome on an
  unattended, isolated VM — fall back to a terminal diff and carry on; do not
  retry on your own initiative.
  `--force` (or `DIFF_REVIEW_FORCE=1`) makes the server wait indefinitely — for
  a user who asked for it because they need more time to forward the port.
- **Port 3000 busy** — a stale server is probably running:
  `pkill -f "diff-review/server.mjs"`, or pass `--port <other>` and give the user
  the new URL.
- **User wants to abort** — stop the background task with TaskStop; don't wait forever.
