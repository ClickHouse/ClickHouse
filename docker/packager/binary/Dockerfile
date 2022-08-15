# rebuild in #33610
# docker build -t clickhouse/binary-builder .
ARG FROM_TAG=latest
FROM clickhouse/test-util:$FROM_TAG

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

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
    && make install \
    && cd ../.. \
    && rm -rf cctools-port

# Build and install tools for cross-linking to Darwin (aarch64)
RUN git clone --depth 1 https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=aarch64-apple-darwin \
    && make install \
    && cd ../.. \
    && rm -rf cctools-port

# Download toolchain and SDK for Darwin
RUN wget -nv https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.0.sdk.tar.xz

# NOTE: Seems like gcc-11 is too new for ubuntu20 repository
RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt-get update \
    && apt-get install gcc-11 g++-11 --yes \
    && apt-get clean

# A cross-linker for RISC-V 64 (we need it, because LLVM's LLD does not work):
RUN apt-get install binutils-riscv64-linux-gnu

# Build LLVM from trunk, because only that version can build the RISC-V binaries successfully.
# It can be also used to check it for regular builds.
RUN git clone https://github.com/llvm/llvm-project.git \
    && mkdir llvm-build \
    && cd llvm-build \
    && cmake -GNinja -DCMAKE_BUILD_TYPE:STRING=Release -DLLVM_ENABLE_PROJECTS=all -DLLVM_TARGETS_TO_BUILD=all ../llvm-project/llvm \
    && ninja \
    && ninja install

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH
ARG NFPM_VERSION=2.16.0

RUN arch=${TARGETARCH:-amd64} \
  && curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${arch}.deb" \
  && dpkg -i /tmp/nfpm.deb \
  && rm /tmp/nfpm.deb

ARG GO_VERSION=1.18.3
# We need go for clickhouse-diagnostics
RUN arch=${TARGETARCH:-amd64} \
  && curl -Lo /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-${arch}.tar.gz" \
  && tar -xzf /tmp/go.tgz -C /usr/local/ \
  && rm /tmp/go.tgz

ENV PATH="$PATH:/usr/local/go/bin"
ENV GOPATH=/workdir/go
ENV GOCACHE=/workdir/

RUN mkdir /workdir && chmod 777 /workdir
WORKDIR /workdir

# NOTE: thread sanitizer is broken in clang-14, we have to build it with clang-15
# https://github.com/ClickHouse/ClickHouse/pull/39450
# https://github.com/google/sanitizers/issues/1540
# https://github.com/google/sanitizers/issues/1552

RUN export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-15 main" >> \
        /etc/apt/sources.list.d/clang.list \
    && apt-get update \
    && apt-get install \
        clang-15 \
        clang-tidy-15 \
        --yes --no-install-recommends \
    && apt-get clean

COPY build.sh /
CMD ["bash", "-c", "/build.sh 2>&1"]
