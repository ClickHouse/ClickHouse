# rebuild in #33610
# docker build -t clickhouse/stress-test .
ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            parallel \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            sudo \
            openssl \
            netcat-openbsd \
            brotli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# sudo clamps RLIMIT_CORE to 0/0 for the commands it runs, so the server started
# via `clickhouse start` (sudo hop) can never raise its core-file soft limit to
# core_dump.size_limit ("Cannot set max size of core file" warning) and crashes
# leave no core. Give the sudo child soft=0/hard=unlimited: only the server's own
# startup raise takes effect; other sudo helpers keep soft=0 and still dump nothing.
RUN echo 'Defaults rlimit_core="0,infinity"' > /etc/sudoers.d/clickhouse-core-dump \
    && chmod 0440 /etc/sudoers.d/clickhouse-core-dump \
    && visudo -c

ENV EXPORT_S3_STORAGE_POLICIES=1
