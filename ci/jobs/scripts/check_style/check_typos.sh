#!/usr/bin/env bash

# Check for typos in code.

ROOT_PATH="."

#FIXME: check all (or almost all) repo
codespell \
    --skip "*generated*,*gperf*,*.bin,*.mrk*,*.idx,checksums.txt,*.dat,*.pyc,*.kate-swp,*obfuscateQueries.cpp,d3-*.js,*.min.js,*.sum,${ROOT_PATH}/utils/check-style/aspell-ignore" \
    --ignore-words "${ROOT_PATH}/ci/jobs/scripts/check_style/codespell-ignore-words.list" \
    --exclude-file "${ROOT_PATH}/ci/jobs/scripts/check_style/codespell-ignore-lines.list" \
    --quiet-level 2 \
    "$ROOT_PATH"/{src,base,programs,utils} \
    $@ | grep -P '.' \
    && echo -e "\nFound some typos in the code.\nSee ci/jobs/scripts/check_style/codespell-ignore-*.list if you want to add a line or word exception."
