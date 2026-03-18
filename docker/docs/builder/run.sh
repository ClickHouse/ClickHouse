#!/bin/bash

set -ex

GIT_BRANCH=$(git branch --show-current)

if [ "$GIT_DOCS_BRANCH" ] && ! [ "$GIT_DOCS_BRANCH" == "$GIT_BRANCH" ]; then
  git fetch origin --depth=1 -- "$GIT_DOCS_BRANCH:$GIT_DOCS_BRANCH"
  git checkout -f "$GIT_DOCS_BRANCH"
else
  # Update docs repo
  git pull
fi

# install latest packages
yarn install
yarn prep-from-local /ClickHouse

# Force build error on wrong symlinks
sed -i '/onBrokenMarkdownLinks:/ s/ignore/error/g' docusaurus.config.js

if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
  export CI=true
  exec yarn build "$@"
fi

exec "$@"
