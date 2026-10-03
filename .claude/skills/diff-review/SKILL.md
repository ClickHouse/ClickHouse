---
name: diff-review
description: In-browser review of changes before committing or opening a PR — uncommitted work, staged changes, one commit or a whole branch. Starts a local server on localhost:3000 that renders whole-file diffs with @pierre/diffs CodeView, opens the user's browser, and hands back a round of line comments every time the user sends one — which must then be read back and addressed while the review stays open and they keep commenting. Works from a browser on the same machine, or from the user's own machine through an SSH-forwarded port (the server prints the `ssh -L` command); where nobody opens the page within 120 s, it exits on its own. Use ALWAYS before git commit or PR creation, and whenever the user asks to see or review changes.
---

# diff-review — let the user review changes in the browser

The user wants to read and review code changes in a browser UI before anything is
committed or pushed. This skill serves the diff on localhost and hands you a
round of line comments every time the user sends one.

**Sending a round does not end the review.** The user goes on reading and
commenting — those comments become the next round — while you work on the round
you were handed. Every comment you mark resolved turns green on their screen as
you go, so resolve them one by one rather than in a batch at the end. The review
ends when the user closes it in the UI.

The review file is durable state, not a report you read once. Every comment is
written to it the moment it is made, and stays in it — reappearing in the next
round, on the line it has moved to — until **you** mark it resolved. It defaults
to `diff-review-comments.json` in the reviewed repository's git directory: one
per repository, so every round of a piece of work lands in the same file, and
inside `.git`, so it never shows up in `git status` or as a file to review. Never
overwrite or delete it yourself, and pass `--out` only to keep two reviews of one
repository apart.

## When not to use it

The server binds to loopback, so the UI is reachable from a browser on the very
machine the server runs on — or from the user's own machine once they forward the
port (where the machine looks remote, the server prints the exact `ssh -L`
command). What it cannot survive is an environment where nobody will ever open
the page: an unattended run on an isolated VM. There the server exits 4 by itself
after 120 s. Don't reach for this skill when there is no user around to open a
browser: show the diff in the terminal with `git diff` / `git show`, or open the
pull request and let review happen there.

## How to run

1. Start the server under **Monitor** — its stdout is one line per event, and
   each line reaches you as a notification while you keep working:

   ```
   Monitor({
     command: "node <skill-dir>/server.mjs --repo <repo-root> --port 3000",
     description: "diff review rounds",
     persistent: true,
     timeout_ms: 3600000,
   })
   ```

   `<skill-dir>` is the directory this SKILL.md was loaded from — a project's
   `.claude/skills/<name>/` or the user's `~/.claude/skills/<name>/`, and the
   copy to run is the one you were given. Everything the server needs it finds
   beside itself, so the path is the only thing that has to be right.

   `persistent: true` is required: a review lasts as long as the user wants it
   to. Load the tool with `ToolSearch("select:Monitor")` first if it is not
   already available.

   The events, verbatim:

   | line | what it means |
   | --- | --- |
   | `diff-review: round 1 open at http://localhost:3000 — …` | the review is up; give the user the URL |
   | `diff-review: forward the port …: ssh -L 3000:localhost:3000 …` | only on a remote-looking machine: hand the user that command with the URL |
   | `diff-review: round N submitted (verdict: …, M open comment(s)) — <out>` | a round is yours: read `<out>` and address it |
   | `diff-review: review closed after N round(s); <out> is final` | the user is done; the watch ends |

   The watch also ends if the server dies — the exit code comes with it (see
   Troubleshooting). Everything else the server has to say goes to stderr and is
   in the monitor's output file.

   `--base` is one end of the review; the other is the working tree by default,
   the index with `--staged`, or a commit with `--head`. Pick the pair that
   matches what the user asked for:

   | to review | flags |
   | --- | --- |
   | uncommitted work (staged + unstaged + untracked) | *(none)* — the default, `--base HEAD` |
   | exactly what `git commit` would record | `--staged` |
   | the last commit | `--base HEAD~1 --committed` |
   | one commit anywhere in history | `--base <sha>^ --head <sha>` |
   | a whole branch, before a PR | `--base $(git merge-base origin/master HEAD) --committed` |

   `--committed` is `--head HEAD`. Anything other than the default ignores the
   working tree, so uncommitted or untracked local edits never leak into a review
   of work that is already recorded. The UI's header states which range is on
   screen (`HEAD~1 → HEAD`), so a mistake here is visible rather than silent.

   The page loads code from this server only, so no third-party CDN ever sees the
   diff. `@pierre/diffs` is pinned by sha256 and verified before it is served,
   but not kept in git: the first review on a machine downloads it into
   `~/.cache/diff-review` (`--prefetch` does it upfront; `vendor/README.md`
   covers offline machines).

   It shows one file at a time, picked from a directory tree on the left
   (status, `+a −d` counts, and a comment-count badge per file), with a path
   filter; the tree's **All files** entry puts the whole review on one page.
   Every file is diffed from both of its full sides, so unchanged code is folded
   but always expandable out to the whole file — click a folded region, or hit
   **Whole file** (`e`) for a mode that lays every file out entire and holds
   while you move through the review. Both the sidebar and the boundary between
   the two split columns can be dragged (double-click the split handle to even it
   out); a file with only one side is laid out in a single column and has no
   handle. Stepping to a hunk or a comment selects the line it lands on, so it is
   clear where the jump went.

   The sidebar has a second pane, **Comments**, listing every comment of the
   review — open ones first, then the addressed ones with your resolution under
   each. Clicking one goes to it. An addressed one carries two actions: **reply**
   opens a follow-up comment on the same lines (linked by `replyTo`, and the
   answered one is dismissed once the follow-up is saved), and **hide** dismisses
   it outright. The file-tree badges count only open comments, so that pane is
   how the user finds what has already been answered.

   The header is icon buttons, each naming its own key on hover, and `?` opens
   the full list — so there is rarely a need to recite them. If it helps anyway:
   `,`/`.` (or `k`/`j`) previous/next file, `[`/`]` previous/next hunk, `c` the
   Comments pane, `shift+C` next comment, `e` whole file, `b` the tint behind
   changed lines, `t` light / dark / system, `/` filter by path,
   `y` copy the file name and the selected range, `r` read the diff again,
   `\` hide the sidebar.
   **Send** hands the round over and leaves the review up; **End review**, in the
   `?` overlay, closes it and stops the server.

   Rendering a file for the first time costs about a millisecond per line and
   blocks — a 2 000-line file takes a couple of seconds behind a spinner — but a
   file that has been shown once is kept, so going back to it is instant. Files
   over 3 000 lines render without syntax colours rather than stalling, and so
   does the all-files page.

2. Tell the user the review is ready at `http://localhost:3000` — with the
   port-forwarding command if the server printed one — then **wait**. Do not poll
   the `--out` file, do not stop the monitor, and do not commit or push: the
   notification is what tells you a round is ready. Do not fetch the URL yourself
   either: a request from you is the browser the server waits for, and it cancels
   the deadline that would otherwise end an unattended run.

3. On a `round N submitted` notification, read the `--out` JSON file. Shape:

   ```json
   {
     "status": "submitted",
     "round": 2,
     "submitted": { "round": 2, "verdict": "request_changes", "at": "…" },
     "verdict": "request_changes",
     "overall": "free-text overall comment (may be empty)",
     "comments": [
       { "id": "c7", "file": "src/app.ts", "side": "new", "startLine": 12, "endLine": 15,
         "comment": "the user's comment text", "resolved": false, "round": 2,
         "anchor": "  const x = compute();" }
     ]
   }
   ```

   `submitted` is the round that was just handed to you, and is the one thing in
   the file that does not move. `round` is the round the user is writing **now**,
   and `status` flips back to `in_progress` the moment they type again — so do
   not wait for either to look a particular way. Address every comment with
   `"resolved": false`, whichever round it belongs to.

   Every open comment comes first; resolved ones follow, kept for the record,
   and the ones the user has read and dismissed come last.
   `side: "new"` means line numbers refer to the new version of the file;
   `side: "old"` refers to the base version (a comment on deleted lines). `id` is
   stable for the life of the comment — it is how you name one. `anchor` is the
   text of the line the comment was filed on, and is what lets the next round put
   it back in the right place after your fixes have moved the code; leave it
   alone. `"draft": true` marks a comment the user was still typing and never
   saved — it is not part of the review, and is dropped on submit.

   `"dismissed": true` marks one the user has read your answer to and is done
   with. It is off their screen for good and stays in the file only as a record —
   never resurface it, and never reopen it. `"replyTo": "c7"` marks a follow-up:
   the user read your answer to `c7` and asked something further on the same
   lines. Read the two together — `c7` is in the same file, resolved, with your
   `resolution` on it — and answer the follow-up in light of what you already
   said.

4. **Address every open comment.** They are the user's code review. For each one,
   quote it briefly, then either fix the code or answer the question.

5. **Mark what you dealt with, in the file, as you go.** When you have addressed
   a comment, set `"resolved": true` on it and add a one-line `"resolution"`: it
   turns green in the browser within a couple of seconds, which is how the user
   follows what you are doing. Do not delete comments, do not renumber `id`s, and
   do not touch `anchor`. Anything you leave unresolved comes back in the next
   round — which is the point: an answered question you could not act on stays
   visible until the user drops it.

   ```bash
   node -e '
     const f = process.argv[1], done = new Map(JSON.parse(process.argv[2]));
     const r = JSON.parse(require("fs").readFileSync(f, "utf8"));
     for (const c of r.comments) if (done.has(c.id)) { c.resolved = true; c.resolution = done.get(c.id); }
     require("fs").writeFileSync(f, JSON.stringify(r, null, 2) + "\n");
   ' <out-file> '[["c7","renamed to spilled_bytes"],["c8","answered: the selector is needed for …"]]'
   ```

   The read and the write have to be one step, as they are here. The server
   rewrites the same file every time the user types, so a file body you read
   earlier is already stale, and writing it back would drop whatever they have
   said since.

6. Say what you did, and go back to waiting. Do not restart the server and do
   not commit: the review is still open, and the next round may already be on its
   way. A round that arrives while you are still working on the previous one is
   not a problem — finish, then read the file again.

   The diff on screen stays as it was until the user reloads, so the lines they
   are commenting on never move under them. To show them the code as your fixes
   leave it, just say it is ready to reload: the page's reload control (`r`) makes
   the server re-read the whole diff — the file set, the statuses, both sides of
   every file — exactly as a fresh start would, so there is no need to restart the
   monitor. It turns amber on its own once a file on screen is no longer the bytes
   on disk. Unresolved comments come back in amber, tagged with the round they came
   from and relocated to wherever your fixes moved their line (or flagged if that
   line is gone); the ones you resolved come back in green. A comment on a file
   that has left the diff altogether cannot be drawn, so it moves to the
   "Not shown" line and stays open in the review file. The file tree badges count
   only the open ones, so with everything resolved the badges are empty and
   `c` / `shift+C` is how the user steps through what was addressed.

7. The review is over when `review closed` arrives (or the user tells you). Then
   `verdict: "approve"` with no open comments → proceed with the commit / PR.
   `verdict: "request_changes"` → do NOT commit until fixes are made and the user
   is satisfied.

## Troubleshooting

- **Exit code 3** — nothing to review in the chosen range (it is named in the message).
- **Exit code 4** — the machine looks remote (an SSH session, cloud-init state or
  a cloud vendor in the DMI strings, or Linux with neither `DISPLAY` nor
  `WAYLAND_DISPLAY` — the ones that decided it are on stderr) and no browser
  reached the review page within 120 s, despite the printed forwarding command.
  This is the expected outcome on an unattended, isolated VM: fall back to
  `git diff` / `git show` and carry on; do not retry on your own initiative.
  `--force` (or `DIFF_REVIEW_FORCE=1`) drops the deadline and waits indefinitely —
  for a user who asked for it because they need more time to forward the port.
- **Exit code 1 with "refusing to overwrite it"** — the `--out` file is not
  readable as a review (bad JSON, or a review of another repository). It may hold
  comments nobody has acted on, so the server will not clobber it: read it, deal
  with whatever is in it, then move it aside or pass a different `--out`.
- **Exit code 1 with "needs node 18 or newer"** — the node on `PATH` is too old
  (the version it found is in the message). Nothing to work around: point the
  command at a newer node, or review with `git diff` / `git show`.
- **Exit code 1 with "cannot obtain"** — a pinned UI asset is neither cached nor
  downloadable (no network, or the URL no longer serves the pinned sha256 — the
  hashes are on stderr). Nothing renders without it, so review with `git diff` /
  `git show` this time and tell the user; `vendor/README.md` says how to fix the
  pin or supply the file by hand.
- **Port 3000 busy** — a stale server is probably running:
  `pkill -f "[s]erver.mjs.*--port 3000"`, or pass `--port <other>` and give the
  user the new URL. Match on the port, never on the skill's path: another copy of
  this skill may be holding a live review someone is writing into. The `[s]` is
  what keeps the pattern from matching — and killing — the shell you run it from.
- **Browser didn't open** (SSH / headless) — give the user the URL to open
  manually, together with the `ssh -L` line if the server printed one.
- **"not saved" in the UI header, or `cannot read/write` on stderr** — something
  else has made the `--out` file unreadable. The page keeps everything it holds,
  so fix the file (or move it aside and restart) and the next keystroke saves it all.
- **User wants to abort** — stop the monitor with TaskStop; don't wait forever.
- **No Monitor tool** — run the server with Bash `run_in_background: true`
  instead; that only notifies you when the review closes, so for each round poll
  the file in a second background command that exits when the round advances:
  `until [ "$(node -pe 'JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).submitted?.round ?? 0' <out>)" -gt <N> ]; do sleep 2; done`.
- **After changing the UI** — `ui.html` and the ES modules it loads from `ui/` are
  re-read per request, so the user only has to reload the tab; changing
  `server.mjs` needs a restart (and a page reloaded against an older server loses
  the round events — restart both together). Check the logic still holds with
  `node <skill-dir>/ui_test.mjs` against a running server: it imports the shipped
  modules and verifies the tree model, the filter, comment state across a
  hand-over, carry-over, and that an addressed comment survives the whole way
  in — payload, `Session`, store. `?testannotation` and `?testsend` seed the page
  for a headless run. Some checks read the live review, so a couple of them state
  properties of whatever file set is on screen (a folded directory chain, for
  one): read a failure against the diff being served before believing it.
- **After changing persistence** — `node <skill-dir>/persist_test.mjs`
  builds a throwaway repository under `<skill-dir>/tmp`, runs real servers against
  it and checks the whole round-trip: saved before submit, survives a kill and a reload,
  relocated by `anchor.mjs` after the code moves, still open after the round is
  handed over, not reopened by the page once the session resolves it, and never
  clobbered when the file cannot be read. It needs no review to be open.
