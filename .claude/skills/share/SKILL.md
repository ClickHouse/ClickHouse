---
name: share
description: Share a Claude Code session to pastila.nl and return a viewer link. Shares the current session by default, or a session specified as a transcript path, a session id, or a project. Use when the user asks to share, publish, or get a link to this conversation or to a session/transcript.
argument-hint: "[session: path | session-id | project]"
disable-model-invocation: false
allowed-tools: Bash
---

# Share a Claude session

Upload a Claude Code session transcript to pastila.nl (encrypted) and return a
`.claude.jsonl` link that renders it like the viewer at
https://github.com/ClickHouse/alexeyprompts

## Steps

1. Run the bundled helper. It lives in the same directory as this SKILL.md — use
   its absolute path (here this is `.claude/skills/share/share.py`):

   ```sh
   python3 <skill-dir>/share.py [session]
   ```

   - **No argument** → shares the **current** session (identified strictly by
     `$CLAUDE_CODE_SESSION_ID`). If that variable is unset or its transcript
     cannot be found, the script errors out and asks for an explicit argument
     rather than guessing — it never falls back to "the newest transcript", so
     it cannot publish an unrelated session by mistake.
   - **`[session]`** → shares another session, given as one of:
     - a path to a `.jsonl` transcript,
     - a session id (uuid), or
     - a project path or its `~/.claude/projects` directory name (shares the
       newest session in it).

2. Report the URL the script prints to the user.

The script delegates the upload to `pastila.py` (encryption + INSERT), so that
logic is not duplicated; it then inserts the `.claude.jsonl` extension before the
`#` key anchor to produce the viewer link.

## Notes

- The link contains the decryption key in its `#` fragment — anyone with the link
  can read the session. Share it only intentionally.
- Sessions include tool calls and results (file fragments, command output). Take a
  quick look before sharing potentially sensitive ones.
- `pastila.py` (the encryption + upload script from the pastila repo) is bundled
  next to `share.py` in this skill, so the skill is self-contained. It still needs
  the Python packages `requests` and `cryptography` installed.
