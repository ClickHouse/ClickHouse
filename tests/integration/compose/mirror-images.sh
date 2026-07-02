#!/usr/bin/env bash
# Mirror third-party integration-test images into the clickhouse/ Docker Hub namespace.
#
# Why: CI runners pull Docker Hub images through the dockerhub-proxy cache
# (registry:2 + nginx, backed by S3 — see tests/ci/terraform/dockerhub-proxy.md).
# That proxy only fronts Docker Hub. Images hosted on other registries
# (mcr.microsoft.com, ghcr.io) bypass the proxy and are pulled directly, so CI is
# exposed to those registries' anonymous rate limits (e.g. mcr.microsoft.com
# returns HTTP 429 "toomanyrequests" under load). Re-hosting them under
# clickhouse/ routes the pulls back through the proxy + S3 cache.
#
# Usage: log in to Docker Hub with an account that can push to the clickhouse org,
# then run this script. It is idempotent — re-run it to add images or bump versions.
# Keep the mapping below in sync with the image references in compose/*.yml.

set -xeuo pipefail

# upstream-reference  clickhouse-reference (slash flattened to dash; Docker Hub repos are org/name only)
IMAGES=(
    "mcr.microsoft.com/azure-storage/azurite:3.35.0           clickhouse/azure-storage-azurite:3.35.0"
    "ghcr.io/projectnessie/nessie:0.107.2                     clickhouse/projectnessie-nessie:0.107.2"
    "ghcr.io/ytsaurus/local:stable-24.2                       clickhouse/ytsaurus-local:stable-24.2"
    "ghcr.io/letsencrypt/pebble:2.9.0                         clickhouse/letsencrypt-pebble:2.9.0"
    "ghcr.io/letsencrypt/pebble-challtestsrv:2.9.0            clickhouse/letsencrypt-pebble-challtestsrv:2.9.0"
)

for entry in "${IMAGES[@]}"; do
    read -r src dst <<< "$entry"
    # Copy the full manifest list (all architectures) registry-to-registry. This
    # preserves multi-arch so both amd64 and arm64 runners are served; a plain
    # `docker pull && docker push` would only re-push the host's single arch.
    docker buildx imagetools create --tag "$dst" "$src"
done
