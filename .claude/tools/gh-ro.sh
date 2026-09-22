#!/usr/bin/env bash
# Read-only `gh` wrapper for the investigate-ci skill.
#
# Two jobs:
#   1. Drop GH_CONFIG_DIR before invoking gh. Some agent/CI runners set it to a
#      poisoned config dir (no/expired auth) that makes every `gh` call fail,
#      while the default config (~/.config/gh) authenticates fine. This mirrors
#      how patch-release-check reaches the CLI (env -u GH_CONFIG_DIR gh).
#   2. Enforce read-only. Only a fixed allowlist of non-mutating subcommands is
#      permitted, so a single `Bash(.claude/tools/gh-ro.sh:*)` allow in
#      settings.investigate.json cannot be used to create/close/edit/merge/
#      comment, or to reach the write-capable `gh api`. Anything else exits 2.
set -euo pipefail

case "${1:-} ${2:-}" in
  "pr view"|"pr list"|"pr diff"|"pr checks"|"issue view"|"issue list") ;;
  *)
    echo "gh-ro.sh: refusing non-read-only invocation: gh ${*:-}" >&2
    echo "gh-ro.sh: allowed: pr {view,list,diff,checks}, issue {view,list}" >&2
    exit 2
    ;;
esac

exec env -u GH_CONFIG_DIR gh "$@"
