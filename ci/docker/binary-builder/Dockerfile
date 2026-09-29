# docker build -t clickhouse/binary-builder .
ARG FROM_TAG
FROM clickhouse/fasttest:$FROM_TAG
ARG TARGETARCH
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

#non-functional change

# If the cctools is updated, then first build it in the CI, then update here in a different commit
COPY --from=clickhouse/cctools:859fb360308eb8ac47ad /cctools /cctools

# We need OpenSSL FIPS in permissive mode for build on MasterCI
COPY --from=clickhouse/cctools:859fb360308eb8ac47ad \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  \
  /etc/ssl/

# Refresh the kitware GPG key inherited from fasttest base image, it may be stale
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg \
    && apt-get update \
    && apt-get install software-properties-common --yes --no-install-recommends --verbose-versions

RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt-get update \
    && apt-get -o Dpkg::Options::="--force-overwrite" upgrade --yes \
    && apt-get -o Dpkg::Options::="--force-overwrite" install --yes \
        binutils-dev \
        build-essential \
        zlib1g-dev \
        python3-boto3 \
        yasm \
        zstd \
        zip \
        clang-tidy-${LLVM_VERSION} \
        llvm-${LLVM_VERSION} \
        bolt-${LLVM_VERSION} \
    && ln -sf /usr/bin/llvm-profdata-${LLVM_VERSION} /usr/bin/llvm-profdata \
    && ln -sf /usr/bin/llvm-cov-${LLVM_VERSION} /usr/bin/llvm-cov \
    && ln -sf /usr/bin/llvm-bolt-${LLVM_VERSION} /usr/bin/llvm-bolt \
    && ln -sf /usr/bin/merge-fdata-${LLVM_VERSION} /usr/bin/merge-fdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Optional PGO+BOLT optimized toolchain override
ARG TOOLCHAIN_COMMIT=bbbf2c95d11bed6a3dbabcc9cb2a65d02cfda55e
# S3 directory of that commit's artifacts. Praktika owns the prefix layout and
# has changed it, so the location is pinned next to the commit instead of being
# spelled from it; update_toolchain_dockerfile.py rewrites both lines together.
ARG TOOLCHAIN_S3_DIR=REFs/master/${TOOLCHAIN_COMMIT}
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

# Download and install mold for s390x build
ARG MOLD_VERSION=2.40.4
RUN arch=${TARGETARCH:-amd64} \
    && case $arch in \
        amd64) rarch=x86_64 ;; \
        arm64) rarch=aarch64 ;; \
    esac \
    && curl -Lo /tmp/mold.tar.gz "https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-${rarch}-linux.tar.gz" \
    && mkdir /tmp/mold \
    && tar -xzf /tmp/mold.tar.gz -C /tmp/mold \
    && cp -r /tmp/mold/mold*/* /usr \
    && rm -rf /tmp/mold \
    && rm /tmp/mold.tar.gz

# Architecture of the image when BuildKit/buildx is used
ARG NFPM_VERSION=2.20.0
RUN arch=${TARGETARCH:-amd64} \
    && curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${arch}.deb" \
    && dpkg -i /tmp/nfpm.deb \
    && rm /tmp/nfpm.deb

ARG GO_VERSION=1.19.10
# We needed go for clickhouse-diagnostics (it is not used anymore)
RUN arch=${TARGETARCH:-amd64} \
    && curl -Lo /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-${arch}.tar.gz" \
    && tar -xzf /tmp/go.tgz -C /usr/local/ \
    && rm /tmp/go.tgz

ENV PATH="$PATH:/usr/local/go/bin"
ENV GOPATH=/workdir/go
ENV GOCACHE=/workdir/

# toolchain and SDK for Darwin
RUN curl -sL -O https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz \
  && mkdir -p /build/cmake/toolchain/darwin-x86_64 \
  && tar xJf /MacOSX11.0.sdk.tar.xz -C /build/cmake/toolchain/darwin-x86_64 --strip-components=1 \
  && ln -sf darwin-x86_64 /build/cmake/toolchain/darwin-aarch64 \
  && rm /MacOSX11.0.sdk.tar.xz

# Emscripten SDK for the experimental WebAssembly build (see the `OS_WASM` arms in cmake/).
# The version has to match the one the wasm64 target is verified with.
ARG EMSDK_VERSION=6.0.5
RUN curl -fsSL "https://github.com/emscripten-core/emsdk/archive/refs/tags/${EMSDK_VERSION}.tar.gz" | tar -xzf - -C /opt \
  && mv "/opt/emsdk-${EMSDK_VERSION}" /opt/emsdk \
  && /opt/emsdk/emsdk install "${EMSDK_VERSION}" \
  && /opt/emsdk/emsdk activate "${EMSDK_VERSION}" \
  && rm -rf /opt/emsdk/downloads

ENV EMSDK=/opt/emsdk
ENV PATH="$PATH:/opt/emsdk/upstream/emscripten"

# Node.js for the wasm64 smoke test (see build_clickhouse.py). Emscripten 6 emits
# Memory64 code, and its generated startup script refuses engines older than Node.js 24.
ARG NODE_VERSION=24.19.0
RUN case "${TARGETARCH}" in \
      amd64) NODE_ARCH=x64 ;; \
      arm64) NODE_ARCH=arm64 ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" && exit 1 ;; \
    esac \
  && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NODE_ARCH}.tar.xz" | tar -xJf - -C /opt \
  && ln -sf "/opt/node-v${NODE_VERSION}-linux-${NODE_ARCH}/bin/node" /usr/local/bin/node

# Pre-generate the wasm64 variant of the Emscripten system libraries. `-sMEMORY64`,
# `-fwasm-exceptions` and `-pthread` are ABI flags applied to every translation unit,
# and the link settings select startup objects (`-sPROXY_TO_PTHREAD` links
# `crt1_proxy_main.o` instead of `crt1.o`). The optimization level at the link selects
# library variants too: an unoptimized link picks the `-debug` builds of libc, libc++
# and the stubs (`libstubs-debug.a`), while the `-O3 ... -g0` CI link needs the release
# ones (`libstubs.a`, `libc-mt.a`, ...). `emcc` compiles whatever the link needs
# lazily at first use into its cache, and building libc, libc++ and friends would
# cost the CI link several minutes - so the link flags here mirror the `clickhouse`
# link. Keep them in sync with the `add_link_options` in the `OS_WASM` branch of
# cmake/target.cmake plus the RelWithDebInfo optimization flags.
RUN echo 'int main() { return 0; }' > /tmp/em_prewarm.cpp \
  && em++ -O3 -sMEMORY64=1 -fwasm-exceptions -pthread \
      -sSTACK_SIZE=8388608 -sINITIAL_MEMORY=268435456 -sALLOW_MEMORY_GROWTH=1 -sMAXIMUM_MEMORY=17179869184 \
      -g0 -sPROXY_TO_PTHREAD=1 -sEXIT_RUNTIME=1 \
      /tmp/em_prewarm.cpp -o /tmp/em_prewarm.js \
  && rm -f /tmp/em_prewarm.*

# Pre-warming cannot cover everything: the link also writes `symbol_lists/<hash>.json`
# cache entries whose hash covers the resolved settings, and some settings are derived
# from the program being linked (e.g. `MAIN_READS_PARAMS`, from the signature of `main`),
# so the real `clickhouse` link misses the entry the trivial program above cached and
# generates its own. The build in CI runs as an unprivileged user, hence the cache has
# to be writable for it.
RUN chmod -R a+rwX /opt/emsdk/upstream/emscripten/cache

ARG CLANG_TIDY_SHA1=3880541496c7ce0d2e2830d2f525375078fe0b1f
RUN curl -Lo /usr/bin/clang-tidy-cache.py \
        "https://raw.githubusercontent.com/matus-chochlik/ctcache/$CLANG_TIDY_SHA1/src/ctcache/clang_tidy_cache.py" \
    && chmod +x /usr/bin/clang-tidy-cache.py

COPY clang-tidy-cache.sh /usr/bin/clang-tidy-cache
