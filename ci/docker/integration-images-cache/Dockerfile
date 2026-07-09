# docker build -t clickhouse/clickhouse-integration-test .
#
# Ships the stable external service images used by the integration tests
# (tests/integration/compose/*.yml, listed in images.txt) as docker-archive
# tarballs under /preseed. At job start the integration job pulls this image
# with the nested DinD daemon and `docker load`s the archives (see
# ci/jobs/scripts/seed_dind_images_cache.sh), replacing ~30 recurring per-job
# registry pulls with one image layer that is cached per runner host in the
# clickhouse_integration_tests_volume data-root.
#
# The image build runs under unprivileged buildkit, so no docker daemon can
# run here; skopeo fetches the images daemon-less. The final stage is FROM
# scratch so consumers only ever transfer the archives themselves.
#
# Rebuilt only when this directory changes; bump images.txt (via
# generate_images_list.py --update) to refresh the cache.
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG AS builder

ARG TARGETARCH

RUN apt-get update \
    && apt-get install --yes --no-install-recommends skopeo zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY images.txt pull.sh /build/
RUN bash /build/pull.sh /build/images.txt /preseed

FROM scratch
COPY --from=builder /preseed /preseed
