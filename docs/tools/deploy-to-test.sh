#!/usr/bin/env bash
#
# README:
# This script deploys ClickHouse website to your personal test subdomain.
#
# Before first use of this script:
# 1) Set up building documentation according to https://github.com/ClickHouse/ClickHouse/tree/master/docs/tools
# 2) Create https://github.com/GIT_USER/clickhouse.github.io repo (replace GIT_USER with your GitHub login)
# 3) Send email on address from https://clickhouse.tech/#contacts asking to create GIT_USER-test.clickhouse.tech domain
#
set -ex

BASE_DIR=$(dirname $(readlink -f $0))
GIT_USER=${GIT_USER:-$USER}

GIT_TEST_URI=git@github.com:${GIT_USER}/clickhouse.github.io.git \
 BASE_DOMAIN=${GIT_USER}-test.clickhouse.tech \
 EXTRA_BUILD_ARGS="${@}" \
 CLOUDFLARE_TOKEN="" \
 HISTORY_SIZE=3 \
 ${BASE_DIR}/release.sh
