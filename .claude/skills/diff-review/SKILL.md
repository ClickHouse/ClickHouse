---
name: diff-review
description: In-browser review of pending changes before committing or opening a PR. Starts a local server on localhost:3000 that renders the working-tree diff with @pierre/diffs CodeView, opens the user's browser, and waits for their line comments — which must then be read back and addressed. Use ALWAYS before git commit or PR creation, and whenever the user asks to see or review the changes.
---

# diff-review — let the user review changes in the browser

The user wants to read and review code changes in a browser UI before anything is
committed or pushed. This skill serves the diff on localhost, collects the user's
line comments, and hands them back to you as JSON. You must address every comment.

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
   - The UI loads `@pierre/diffs` from esm.sh, so the browser needs network access.

2. Tell the user the review is ready at `http://localhost:3000`, then **wait**.
   Do not poll and do not proceed with the commit/PR — you will be notified when
   the background command exits (i.e. when the user submits the review).

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
- **Port 3000 busy** — a stale server is probably running:
  `pkill -f "diff-review/server.mjs"`, or pass `--port <other>` and give the user
  the new URL.
- **Browser didn't open** (SSH / headless) — give the user the URL to open manually.
- **User wants to abort** — stop the background task with TaskStop; don't wait forever.
