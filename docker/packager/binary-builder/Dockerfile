# docker build -t clickhouse/binary-builder .
ARG FROM_TAG=latest
FROM clickhouse/fasttest:$FROM_TAG
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

# If the cctools is updated, then first build it in the CI, then update here in a different commit
COPY --from=clickhouse/cctools:d9e3596e706b /cctools /cctools

# Rust toolchain and libraries
ENV RUSTUP_HOME=/rust/rustup
ENV CARGO_HOME=/rust/cargo
ENV PATH="/rust/cargo/bin:${PATH}"
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    chmod 777 -R /rust && \
    rustup toolchain install nightly-2024-04-01 && \
    rustup default nightly-2024-04-01 && \
    rustup toolchain remove stable && \
    rustup component add rust-src && \
    rustup target add x86_64-unknown-linux-gnu && \
    rustup target add aarch64-unknown-linux-gnu && \
    rustup target add x86_64-apple-darwin && \
    rustup target add x86_64-unknown-freebsd && \
    rustup target add aarch64-apple-darwin && \
    rustup target add powerpc64le-unknown-linux-gnu && \
    rustup target add x86_64-unknown-linux-musl && \
    rustup target add aarch64-unknown-linux-musl && \
    rustup target add riscv64gc-unknown-linux-gnu

# A cross-linker for RISC-V 64 (we need it, because LLVM's LLD does not work):
RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt-get update \
    && apt-get install --yes \
        binutils-riscv64-linux-gnu \
        build-essential \
        python3-boto3 \
        yasm \
        zstd \
        zip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Download toolchain and SDK for Darwin
RUN curl -sL -O https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz

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

ARG CLANG_TIDY_SHA1=c191254ea00d47ade11d7170ef82fe038c213774
RUN curl -Lo /usr/bin/clang-tidy-cache \
        "https://raw.githubusercontent.com/matus-chochlik/ctcache/$CLANG_TIDY_SHA1/clang-tidy-cache" \
    && chmod +x /usr/bin/clang-tidy-cache

RUN mkdir /workdir && chmod 777 /workdir
WORKDIR /workdir

COPY build.sh /
CMD ["bash", "-c", "/build.sh 2>&1"]
