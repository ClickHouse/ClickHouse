# docker build -t clickhouse/binary-builder .
ARG FROM_TAG=latest
FROM clickhouse/test-util:latest AS cctools
# The cctools are built always from the clickhouse/test-util:latest and cached inline
# Theoretically, it should improve rebuild speed significantly
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# DO NOT PUT ANYTHING BEFORE THREE NEXT `RUN` DIRECTIVES
# THE MOST HEAVY OPERATION MUST BE THE FIRST IN THE CACHE
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# libtapi is required to support .tbh format from recent MacOS SDKs
RUN git clone --depth 1 https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && INSTALLPREFIX=/cctools ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Build and install tools for cross-linking to Darwin (x86-64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=x86_64-apple-darwin \
    && make install -j$(nproc) \
    && cd ../.. \
    && rm -rf cctools-port

# Build and install tools for cross-linking to Darwin (aarch64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=aarch64-apple-darwin \
    && make install -j$(nproc) \
    && cd ../.. \
    && rm -rf cctools-port

# !!!!!!!!!!!
# END COMPILE
# !!!!!!!!!!!

FROM clickhouse/test-util:$FROM_TAG
ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

# Rust toolchain and libraries
ENV RUSTUP_HOME=/rust/rustup
ENV CARGO_HOME=/rust/cargo
ENV PATH="/rust/cargo/bin:${PATH}"
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    chmod 777 -R /rust && \
    rustup toolchain install nightly-2023-07-04 && \
    rustup default nightly-2023-07-04 && \
    rustup component add rust-src && \
    rustup target add aarch64-unknown-linux-gnu && \
    rustup target add x86_64-apple-darwin && \
    rustup target add x86_64-unknown-freebsd && \
    rustup target add aarch64-apple-darwin && \
    rustup target add powerpc64le-unknown-linux-gnu

# NOTE: Seems like gcc-11 is too new for ubuntu20 repository
# A cross-linker for RISC-V 64 (we need it, because LLVM's LLD does not work):
RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt-get update \
    && apt-get install --yes \
        binutils-riscv64-linux-gnu \
        build-essential \
        g++-11 \
        gcc-11 \
        gcc-aarch64-linux-gnu \
        libc6 \
        libc6-dev \
        libc6-dev-arm64-cross \
        python3-boto3 \
        yasm \
        zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists

# Download toolchain and SDK for Darwin
RUN curl -sL -O https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH
ARG NFPM_VERSION=2.20.0

RUN arch=${TARGETARCH:-amd64} \
    && curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${arch}.deb" \
    && dpkg -i /tmp/nfpm.deb \
    && rm /tmp/nfpm.deb

ARG GO_VERSION=1.19.10
# We need go for clickhouse-diagnostics
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

COPY --from=cctools /cctools /cctools

RUN mkdir /workdir && chmod 777 /workdir
WORKDIR /workdir

COPY build.sh /
CMD ["bash", "-c", "/build.sh 2>&1"]
