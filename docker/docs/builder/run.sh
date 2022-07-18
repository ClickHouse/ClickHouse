#!/bin/bash

set -ex

if [ "$GIT_DOCS_BRANCH" ]; then
  git fetch origin --depth=1 "$GIT_DOCS_BRANCH:$GIT_DOCS_BRANCH"
  git checkout "$GIT_DOCS_BRANCH"
else
  # Update docs repo
  git pull
fi

# The repo is usually mounted to /ClickHouse

for lang in en ru zh
do
  if [ -d "/ClickHouse/docs/${lang}" ]; then
    cp -rf "/ClickHouse/docs/${lang}" "/opt/clickhouse-docs/docs/"
  fi
done

# Force build error on wrong symlinks
sed -i '/onBrokenMarkdownLinks:/ s/ignore/error/g' docusaurus.config.js

if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
  exec yarn build "$@"
fi

exec "$@"
