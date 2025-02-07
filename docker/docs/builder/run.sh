#!/bin/bash

set -ex

GIT_BRANCH=$(git branch --show-current)

if [ "$GIT_DOCS_BRANCH" ] && ! [ "$GIT_DOCS_BRANCH" == "$GIT_BRANCH" ]; then
  git fetch origin --depth=1 -- "$GIT_DOCS_BRANCH:$GIT_DOCS_BRANCH"
  git checkout "$GIT_DOCS_BRANCH"
else
  # Update docs repo
  git pull
fi

# The repo is usually mounted to /ClickHouse
LANGUAGES=$(grep -o "'[/][a-z][a-z]'" /opt/clickhouse-docs/docusaurus.config.js | sort -u | sed "s/'\/\([a-z][a-z]\)'/\1/")

# install latest packages
yarn install
yarn prep-from-local /ClickHouse

for lang in $LANGUAGES
do
  if [ -d "/ClickHouse/docs/${lang}" ]; then
    cp -rf "/ClickHouse/docs/${lang}" "/opt/clickhouse-docs/docs/"
  fi
done

# Force build error on wrong symlinks
sed -i '/onBrokenMarkdownLinks:/ s/ignore/error/g' docusaurus.config.js

if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
  export CI=true
  exec yarn build "$@"
fi

exec "$@"
