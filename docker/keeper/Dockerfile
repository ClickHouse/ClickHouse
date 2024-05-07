# The Dockerfile.ubuntu exists for the tests/ci/docker_server.py script
# If the image is built from Dockerfile.alpine, then the `-alpine` suffix is added automatically,
# so the only purpose of Dockerfile.ubuntu is to push `latest`, `head` and so on w/o suffixes
FROM ubuntu:20.04 AS glibc-donor
ARG TARGETARCH

RUN arch=${TARGETARCH:-amd64} \
    && case $arch in \
        amd64) rarch=x86_64 ;; \
        arm64) rarch=aarch64 ;; \
    esac \
    && ln -s "${rarch}-linux-gnu" /lib/linux-gnu


FROM alpine

ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8 \
    TZ=UTC \
    CLICKHOUSE_CONFIG=/etc/clickhouse-server/config.xml

COPY --from=glibc-donor /lib/linux-gnu/libc.so.6 /lib/linux-gnu/libdl.so.2 /lib/linux-gnu/libm.so.6 /lib/linux-gnu/libpthread.so.0 /lib/linux-gnu/librt.so.1 /lib/linux-gnu/libnss_dns.so.2 /lib/linux-gnu/libnss_files.so.2 /lib/linux-gnu/libresolv.so.2 /lib/linux-gnu/ld-2.31.so /lib/
COPY --from=glibc-donor /etc/nsswitch.conf /etc/
COPY entrypoint.sh /entrypoint.sh

ARG TARGETARCH
RUN arch=${TARGETARCH:-amd64} \
    && case $arch in \
        amd64) mkdir -p /lib64 && ln -sf /lib/ld-2.31.so /lib64/ld-linux-x86-64.so.2 ;; \
        arm64) ln -sf /lib/ld-2.31.so /lib/ld-linux-aarch64.so.1 ;; \
    esac

# lts / testing / prestable / etc
ARG REPO_CHANNEL="stable"
ARG REPOSITORY="https://packages.clickhouse.com/tgz/${REPO_CHANNEL}"
ARG REPOSITORY="https://s3.amazonaws.com/clickhouse-builds/22.4/31c367d3cd3aefd316778601ff6565119fe36682/package_release"
ARG VERSION="23.7.5.30"
ARG PACKAGES="clickhouse-keeper"

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.
ARG DEFAULT_UID="101"
ARG DEFAULT_GID="101"
RUN addgroup -S -g "${DEFAULT_GID}" clickhouse && \
    adduser -S -h "/var/lib/clickhouse" -s /bin/bash -G clickhouse -g "ClickHouse keeper" -u "${DEFAULT_UID}" clickhouse

ARG TARGETARCH
RUN arch=${TARGETARCH:-amd64} \
    && for package in ${PACKAGES}; do \
    ( \
        cd /tmp \
        && echo "Get ${REPOSITORY}/${package}-${VERSION}-${arch}.tgz" \
            && wget -c -q "${REPOSITORY}/${package}-${VERSION}-${arch}.tgz" \
            && wget -c -q "${REPOSITORY}/${package}-${VERSION}-${arch}.tgz.sha512" \
            && sed 's:/output/:/tmp/:' < "${package}-${VERSION}-${arch}.tgz.sha512" | sha512sum -c \
            && tar xvzf "${package}-${VERSION}-${arch}.tgz" --strip-components=1 -C / \
    ) \
    ; done \
    && rm /tmp/*.tgz /install -r \
    && chmod +x /entrypoint.sh \
    && apk add --no-cache su-exec bash tzdata \
    && cp /usr/share/zoneinfo/UTC /etc/localtime \
    && echo "UTC" > /etc/timezone

ARG DEFAULT_CONFIG_DIR="/etc/clickhouse-keeper"
ARG DEFAULT_DATA_DIR="/var/lib/clickhouse-keeper"
ARG DEFAULT_LOG_DIR="/var/log/clickhouse-keeper"
RUN mkdir -p "${DEFAULT_DATA_DIR}" "${DEFAULT_LOG_DIR}" "${DEFAULT_CONFIG_DIR}" \
    && chown clickhouse:clickhouse "${DEFAULT_DATA_DIR}" \
    && chown root:clickhouse "${DEFAULT_LOG_DIR}" \
    && chmod ugo+Xrw -R "${DEFAULT_DATA_DIR}" "${DEFAULT_LOG_DIR}" "${DEFAULT_CONFIG_DIR}"

# /var/lib/clickhouse is necessary due to the current default configuration for Keeper
VOLUME "${DEFAULT_DATA_DIR}" /var/lib/clickhouse
EXPOSE 2181 10181 44444 9181

ENTRYPOINT ["/entrypoint.sh"]
