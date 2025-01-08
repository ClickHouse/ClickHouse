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
ARG VERSION="24.7.3.42"
ARG PACKAGES="clickhouse-keeper"
ARG DIRECT_DOWNLOAD_URLS=""

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
    && cd /tmp && rm -f /tmp/*tgz && rm -f /tmp/*tgz.sha512 |: \
    && if [ -n "${DIRECT_DOWNLOAD_URLS}" ]; then \
        echo "installing from provided urls with tgz packages: ${DIRECT_DOWNLOAD_URLS}" \
        && for url in $DIRECT_DOWNLOAD_URLS; do \
            echo "Get ${url}" \
            && wget -c -q "$url" \
        ; done \
    else \
        for package in ${PACKAGES}; do \
            cd /tmp \
            && echo "Get ${REPOSITORY}/${package}-${VERSION}-${arch}.tgz" \
            && wget -c -q "${REPOSITORY}/${package}-${VERSION}-${arch}.tgz" \
            && wget -c -q "${REPOSITORY}/${package}-${VERSION}-${arch}.tgz.sha512" \
        ; done \
    fi \
    && cat *.tgz.sha512 | sha512sum -c \
    && for file in *.tgz; do \
        if [ -f "$file" ]; then \
            echo "Unpacking $file"; \
            tar xvzf "$file" --strip-components=1 -C /; \
        fi \
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
