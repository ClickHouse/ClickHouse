#!/usr/bin/env python3
"""PreToolUse hook for the investigate-ci locked-down profile.

Auto-approves exactly one Bash command shape: a read-only SELECT POSTed to the
public play.clickhouse.com endpoint (the skill's flaky-vs-real history query).
Everything else falls through to the normal permission prompt — this is an
allowlist (default-deny), so unanticipated shapes prompt rather than run.

Why a hook instead of a Bash allow-rule: a `Bash(curl ... :*)` allow has an
unconstrained wildcard tail, so `curl ... --data-binary @<file>`,
`--data-binary "@<file>"`, `-T/-K/--upload-file <file>`, a second `--data-binary`,
or `$(cat <file>)`/backtick substitution would all be auto-approved and could
exfiltrate local files to the public endpoint. The regex below pins the exact
benign command: fixed `-sS` + the exact play URL + a single double-quoted body
that must start with SELECT and contain no `"`, `$`, or backtick. The `@file`
trick is blocked because the body must begin with SELECT, not `@`.

Output contract: print a PreToolUse "allow" decision only on a match; otherwise
print nothing and exit 0 so normal permission handling (the prompt) runs. Never
emits "deny" — it only ever adds a positive allow for this one shape.
"""
import json
import re
import sys

ALLOW = re.compile(
    r"""curl -sS 'https://play\.clickhouse\.com/\?user=play' --data-binary "\s*SELECT[^"$`]*\""""
)


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except Exception:
        return  # malformed input -> no decision -> prompt

    if data.get("tool_name") != "Bash":
        return

    command = data.get("tool_input", {}).get("command", "").strip()
    if ALLOW.fullmatch(command):
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "allow",
                        "permissionDecisionReason": "read-only play.clickhouse.com SELECT (investigate-ci)",
                    }
                }
            )
        )


if __name__ == "__main__":
    main()
