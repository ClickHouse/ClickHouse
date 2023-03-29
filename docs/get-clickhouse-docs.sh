#!/usr/bin/env bash
set -e
# The script to clone or update the user-guides documentation repo
# https://github.com/ClickHouse/clickhouse-docs

WORKDIR=$(dirname "$0")
WORKDIR=$(readlink -f "${WORKDIR}")
cd "$WORKDIR"

if [ -d "clickhouse-docs" ]; then
  git -C clickhouse-docs pull
else
  if [ -n "$1" ]; then
    url_type="$1"
  else
    read -rp "Enter the URL type (ssh | https): " url_type
  fi
  case "$url_type" in
  ssh)
    git_url=git@github.com:ClickHouse/clickhouse-docs.git
    ;;
  https)
    git_url=https://github.com/ClickHouse/clickhouse-docs.git
    ;;
  *)
    echo "Url type must be 'ssh' or 'https'"
    exit 1
    ;;
  esac
  git clone "$git_url" "clickhouse-docs"
fi
