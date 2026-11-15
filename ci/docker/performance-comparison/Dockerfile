# docker build -t clickhouse/performance-comparison .

ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

# Remove problematic Kitware repository
RUN rm -f /etc/apt/sources.list.d/*kitware* || true

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            bash \
            curl \
            dmidecode \
            g++ \
            git \
            gnuplot \
            imagemagick \
            lsof \
            moreutils \
            ncdu \
            numactl \
            p7zip-full \
            parallel \
            psmisc \
            rsync \
            tree \
            tzdata \
            vim \
            wget \
            rustc \
            cargo \
            ripgrep \
            zstd \
            unzip \
    && apt-get purge --yes python3-dev g++ \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN python3.13 -m pip install --no-cache-dir -r requirements.txt


COPY --from=clickhouse/cctools:859fb360308eb8ac47ad /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

# aws cli to acquire secrets and params from ssm
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip && ./aws/install \
    && rm -rf /awscliv2.zip /aws

# Keep SEAWEEDFS_VERSION in sync with the default in setup_seaweedfs.sh
ARG SEAWEEDFS_VERSION=4.42
ARG TARGETARCH

# SeaweedFS - the job-local S3 server for the S3 performance tests (ci/jobs/scripts/perf/s3_service.py).
# Baked into the image (mirroring ci/docker/stateless-test) so CI jobs fetch nothing external at runtime.
RUN arch=${TARGETARCH:-amd64} \
    && curl -fL --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 60 --max-time 600 \
        "https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/linux_${arch}.tar.gz" -o /tmp/seaweedfs.tar.gz \
    && tar -xzf /tmp/seaweedfs.tar.gz -C / weed \
    && rm /tmp/seaweedfs.tar.gz \
    && chmod +x /weed
