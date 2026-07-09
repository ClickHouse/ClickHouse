#!/bin/bash
# Seeds the DinD daemon's image store from clickhouse/clickhouse-integration-test
# so the stable external service images (tests/integration/compose/*.yml) do
# not have to be pulled from the registry by every integration job.
#
# Usage:
#   seed_dind_images_cache.sh clickhouse/clickhouse-integration-test:TAG
#
# The cache image ships the service images as zstd-compressed docker-archive
# tarballs under /preseed (see ci/docker/integration-images-cache/; docker load
# auto-detects the compression). They are `docker load`ed
# into whatever data-root the nested daemon runs on - normally the
# clickhouse_integration_tests_volume, which persists per runner host, so the
# whole seeding runs once per host per cache tag (a marker image records
# completion; the post-job hook prunes containers and volumes but not images).
#
# Fail-open by design: any failure here only means the job falls back to
# pulling images individually (ci/jobs/scripts/prefetch-integration-test-images
# runs after this either way), so this script always exits 0.
set -uo pipefail

image_ref="${1:?usage: $0 IMAGE:TAG}"
tag="${image_ref##*:}"
marker="clickhouse-dind-preseed-marker:$tag"

if docker image inspect "$marker" > /dev/null 2>&1; then
    echo "DinD image store already seeded from $image_ref"
    exit 0
fi

total_start=$SECONDS
if docker image inspect "$image_ref" > /dev/null 2>&1; then
    echo "$image_ref already present, skipping pull"
else
    echo "Pulling $image_ref"
    if ! timeout 900 docker pull "$image_ref"; then
        echo "WARNING: failed to pull $image_ref - skipping seeding, images will be pulled individually"
        exit 0
    fi
    echo "Pulled $image_ref in $((SECONDS - total_start))s"
fi

if ! ctr=$(docker create "$image_ref" placeholder); then
    echo "WARNING: failed to create a container from $image_ref - skipping seeding"
    exit 0
fi
trap 'docker rm "$ctr" > /dev/null 2>&1' EXIT

# `docker cp CONTAINER:path -` emits a tar stream wrapping the file; tar -xO
# unwraps it. This streams the archives straight into the daemon without
# writing anything outside its data-root.
if ! index=$(docker cp "$ctr:/preseed/index.txt" - | tar -xO); then
    echo "WARNING: failed to read /preseed/index.txt from $image_ref - skipping seeding"
    exit 0
fi

load_start=$SECONDS
loaded=0
failures=0
for name in $index; do
    if docker cp "$ctr:/preseed/$name" - | tar -xO | docker load; then
        loaded=$((loaded + 1))
    else
        echo "WARNING: failed to load $name"
        failures=$((failures + 1))
    fi
done
echo "Loaded $loaded image(s) ($failures failure(s)) in $((SECONDS - load_start))s"

docker rm "$ctr" > /dev/null
trap - EXIT
# The archives have served their purpose; drop the cache image to halve the
# disk footprint in the data-root. The marker (an empty image - images survive
# the post-job prune) makes the next job on this host skip seeding entirely.
docker rmi "$image_ref" > /dev/null || true
tar -cf - --files-from /dev/null | docker import - "$marker" > /dev/null \
    || echo "WARNING: failed to create marker image $marker"

echo "DinD image store seeded from $image_ref in $((SECONDS - total_start))s"
docker images
exit 0
