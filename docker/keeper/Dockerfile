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

ARG REPOSITORY="https://s3.amazonaws.com/clickhouse-builds/22.4/31c367d3cd3aefd316778601ff6565119fe36682/package_release"
ARG VERSION="22.4.1.917"
ARG PACKAGES="clickhouse-keeper"

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.


ARG TARGETARCH
RUN arch=${TARGETARCH:-amd64} \
    && for package in ${PACKAGES}; do \
        { \
            { echo "Get ${REPOSITORY}/${package}-${VERSION}-${arch}.tgz" \
                && wget -c -q "${REPOSITORY}/${package}-${VERSION}-${arch}.tgz" -O "/tmp/${package}-${VERSION}-${arch}.tgz" \
                && tar xvzf "/tmp/${package}-${VERSION}-${arch}.tgz" --strip-components=1 -C / ; \
            } || \
            { echo "Fallback to ${REPOSITORY}/${package}-${VERSION}.tgz" \
                && wget -c -q "${REPOSITORY}/${package}-${VERSION}.tgz" -O "/tmp/${package}-${VERSION}.tgz" \
                && tar xvzf "/tmp/${package}-${VERSION}.tgz" --strip-components=2 -C / ; \
            } ; \
        } || exit 1 \
    ; done \
    && rm /tmp/*.tgz /install -r \
    && addgroup -S -g 101 clickhouse \
    && adduser -S -h /var/lib/clickhouse -s /bin/bash -G clickhouse -g "ClickHouse keeper" -u 101 clickhouse \
    && mkdir -p /var/lib/clickhouse /var/log/clickhouse-keeper /etc/clickhouse-keeper \
    && chown clickhouse:clickhouse /var/lib/clickhouse \
    && chown root:clickhouse /var/log/clickhouse-keeper \
    && chmod +x /entrypoint.sh \
    && apk add --no-cache su-exec bash tzdata \
    && cp /usr/share/zoneinfo/UTC /etc/localtime \
    && echo "UTC" > /etc/timezone \
    && chmod ugo+Xrw -R /var/lib/clickhouse /var/log/clickhouse-keeper /etc/clickhouse-keeper


EXPOSE 2181 10181 44444 9181

VOLUME /var/lib/clickhouse /var/log/clickhouse-keeper /etc/clickhouse-keeper

ENTRYPOINT ["/entrypoint.sh"]
