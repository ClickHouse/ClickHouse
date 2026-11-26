# docker build -t clickhouse/binary-builder .
ARG CCTOOLS_TAG
ARG FROM_TAG
FROM clickhouse/cctools:${CCTOOLS_TAG} AS cctools
FROM clickhouse/fasttest:$FROM_TAG
ARG TARGETARCH
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

#non-functional change

COPY --from=cctools /cctools /cctools

# We need OpenSSL FIPS in permissive mode for build on MasterCI
COPY --from=cctools \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  \
  /etc/ssl/

# Refresh the kitware GPG key inherited from fasttest base image, it may be stale
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg

# clang-tidy and llvm come from the fasttest image. Upgrading them here (apt would also do it for the packages of
# bolt's source package) stores a second copy of LLVM in this layer whenever apt.llvm.org has rebuilt them.
RUN apt-get update \
    && apt-get -o Dpkg::Options::="--force-overwrite" -o APT::Get::Upgrade-By-Source-Package=false install --yes \
        binutils-dev \
        build-essential \
        zlib1g-dev \
        libzstd-dev \
        python3-boto3 \
        pigz \
        yasm \
        zstd \
        zip \
        bolt-${LLVM_VERSION} \
    && ln -sf /usr/bin/llvm-profdata-${LLVM_VERSION} /usr/bin/llvm-profdata \
    && ln -sf /usr/bin/llvm-cov-${LLVM_VERSION} /usr/bin/llvm-cov \
    && ln -sf /usr/bin/llvm-bolt-${LLVM_VERSION} /usr/bin/llvm-bolt \
    && ln -sf /usr/bin/merge-fdata-${LLVM_VERSION} /usr/bin/merge-fdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Optional PGO+BOLT optimized toolchain override
ARG TOOLCHAIN_COMMIT=51fdb0b1b64e10e205041802cd91d962fb1d281c
# S3 directory of that commit's artifacts. Praktika owns the prefix layout and
# has changed it, so the location is pinned next to the commit instead of being
# spelled from it; update_toolchain_dockerfile.py rewrites both lines together.
ARG TOOLCHAIN_S3_DIR=REFs/master/51fdb0b1b64e10e205041802cd91d962fb1d281c/optimizetoolchain
RUN if [ "$TOOLCHAIN_COMMIT" != "none" ]; then \
    arch=${TARGETARCH:-amd64} && \
    case $arch in \
        amd64) tarch=amd64 ;; \
        arm64) tarch=aarch64 ;; \
    esac && \
    curl -fsSL "https://clickhouse-builds.s3.amazonaws.com/${TOOLCHAIN_S3_DIR}/build_toolchain_pgo_bolt_${tarch}/clang-pgo-bolt.tar.zst" \
      | zstd -d | tar -xf - -C /usr/local/ && \
    echo "Installed PGO+BOLT toolchain from commit ${TOOLCHAIN_COMMIT}" \
    ; fi

# Optional PGO+BOLT profiles for ClickHouse binary optimization
# Set CLICKHOUSE_PROFILES_COMMIT to a master commit SHA to download pre-collected profiles.
# Set to "none" (default) to skip — profiles are opt-in.
ARG CLICKHOUSE_PROFILES_COMMIT=none
RUN if [ "$CLICKHOUSE_PROFILES_COMMIT" != "none" ]; then \
    arch=${TARGETARCH:-amd64} && \
    case $arch in \
        amd64) tarch=amd64 ;; \
        arm64) tarch=aarch64 ;; \
    esac && \
    mkdir -p /opt/clickhouse-profiles && \
    for profile in clickhouse-pgo.profdata.zst clickhouse-bolt.fdata.zst; do \
        url="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${CLICKHOUSE_PROFILES_COMMIT}/optimizeclickhouse/collect_clickhouse_profiles_pgo_bolt_${tarch}/${profile}" && \
        if curl -fsSL -o "/opt/clickhouse-profiles/${profile}" "$url"; then \
            zstd -d --rm "/opt/clickhouse-profiles/${profile}" && \
            echo "Downloaded ${profile}" ; \
        else \
            echo "Profile ${profile} not available (optional)" ; \
        fi ; \
    done \
    ; fi

# Architecture of the image when BuildKit/buildx is used
ARG NFPM_VERSION=2.20.0
RUN arch=${TARGETARCH:-amd64} \
    && curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${arch}.deb" \
    && dpkg -i /tmp/nfpm.deb \
    && rm /tmp/nfpm.deb

ARG CLANG_TIDY_SHA1=3880541496c7ce0d2e2830d2f525375078fe0b1f
RUN curl -Lo /usr/bin/clang-tidy-cache.py \
        "https://raw.githubusercontent.com/matus-chochlik/ctcache/$CLANG_TIDY_SHA1/src/ctcache/clang_tidy_cache.py" \
    && chmod +x /usr/bin/clang-tidy-cache.py

COPY clang-tidy-cache.sh /usr/bin/clang-tidy-cache
