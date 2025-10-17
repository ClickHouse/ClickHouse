# docker build -t clickhouse/binary-builder .
ARG FROM_TAG
FROM clickhouse/fasttest:$FROM_TAG
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

# If the cctools is updated, then first build it in the CI, then update here in a different commit
COPY --from=clickhouse/cctools:859fb360308eb8ac47ad /cctools /cctools

# We need OpenSSL FIPS in permissive mode for build on MasterCI
COPY --from=clickhouse/cctools:859fb360308eb8ac47ad \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  \
  /etc/ssl/

# A cross-linker for RISC-V 64 (we need it, because LLVM's LLD does not work):
RUN apt-get update \
    && apt-get install software-properties-common --yes --no-install-recommends --verbose-versions

RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt-get update \
    && apt-get install --yes \
        binutils-riscv64-linux-gnu \
        build-essential \
        python3-boto3 \
        yasm \
        zstd \
        zip \
        clang-tidy-${LLVM_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Download and install mold 2.0 for s390x build
RUN curl -Lo /tmp/mold.tar.gz "https://github.com/rui314/mold/releases/download/v2.0.0/mold-2.0.0-x86_64-linux.tar.gz" \
    && mkdir /tmp/mold \
    && tar -xzf /tmp/mold.tar.gz -C /tmp/mold \
    && cp -r /tmp/mold/mold*/* /usr \
    && rm -rf /tmp/mold \
    && rm /tmp/mold.tar.gz

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH
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

ARG CLANG_TIDY_SHA1=e33c063ca6e52b48bcefbc45d46d8c150ffa81ca
RUN curl -Lo /usr/bin/clang-tidy-cache.py \
        "https://raw.githubusercontent.com/matus-chochlik/ctcache/$CLANG_TIDY_SHA1/src/ctcache/clang_tidy_cache.py" \
    && chmod +x /usr/bin/clang-tidy-cache.py

COPY clang-tidy-cache.sh /usr/bin/clang-tidy-cache
