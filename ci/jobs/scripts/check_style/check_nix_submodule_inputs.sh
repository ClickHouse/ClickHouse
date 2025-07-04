#!/usr/bin/env bash
set -euo pipefail

FLAKE_FILE="flake.nix"

# Make sure the file is tracked
if ! git ls-files --error-unmatch "$FLAKE_FILE" > /dev/null 2>&1; then
  echo "❌ $FLAKE_FILE is not tracked by git"
  exit 2
fi

# Save original file hash
ORIG_HASH=$(git hash-object "$FLAKE_FILE")

# Run update script
./utils/nix/update_submodule_inputs.sh > /dev/null

# Compare hashes
NEW_HASH=$(git hash-object "$FLAKE_FILE")

if [[ "$ORIG_HASH" != "$NEW_HASH" ]]; then
  echo "contrib inputs in $FLAKE_FILE are outdated, please run ./utils/nix/update_submodule_inputs.sh"
  echo ""
  git --no-pager diff --color=never "$FLAKE_FILE"
  exit 1
else
  exit 0
fi
