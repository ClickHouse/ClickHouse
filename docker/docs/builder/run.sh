#!/bin/bash

set -ex

# Update docs repo
git pull

# The repo is usually mounted to /ClickHouse

for lang in en ru zh
do
  if [ ! -e "/opt/clickhouse-docs/docs/${lang}" ] && [ -d "/ClickHouse/docs/${lang}" ]; then
    cp -r "/ClickHouse/docs/${lang}" "/opt/clickhouse-docs/docs/${lang}"
  fi
done

# Force build error on wrong symlinks
sed -i '/onBrokenMarkdownLinks:/ s/ignore/error/g' docusaurus.config.js

if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
  exec yarn build "$@"
fi

exec "$@"
