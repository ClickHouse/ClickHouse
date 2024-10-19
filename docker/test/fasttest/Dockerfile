# docker build -t clickhouse/fasttest .
ARG FROM_TAG=latest
FROM clickhouse/test-util:$FROM_TAG

RUN apt-get update \
    && apt-get install \
        brotli \
        clang-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        cmake \
        expect \
        file \
        libclang-${LLVM_VERSION}-dev \
        libclang-rt-${LLVM_VERSION}-dev \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION}-dev \
        lsof \
        ninja-build \
        odbcinst \
        psmisc \
        python3 \
        python3-pip \
        unixodbc \
        pv \
        jq \
        zstd \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/* \
    && groupadd --system --gid 1000 clickhouse \
    && useradd --system --gid 1000 --uid 1000 -m clickhouse
# ^ For some reason, groupadd and useradd are needed for tests with 'expect', but I don't know, why.

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

# This symlink is required by gcc to find the lld linker
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld
# FIXME: workaround for "The imported target "merge-fdata" references the file" error
# https://salsa.debian.org/pkg-llvm-team/llvm-toolchain/-/commit/992e52c0b156a5ba9c6a8a54f8c4857ddd3d371d
RUN sed -i '/_IMPORT_CHECK_FILES_FOR_\(mlir-\|llvm-bolt\|merge-fdata\|MLIR\)/ {s|^|#|}' /usr/lib/llvm-${LLVM_VERSION}/lib/cmake/llvm/LLVMExports-*.cmake

# LLVM changes paths for compiler-rt libraries. For some reason clang-18.1.8 cannot catch up libraries from default install path.
# It's very dirty workaround, better to build compiler and LLVM ourself and use it. Details: https://github.com/llvm/llvm-project/issues/95792
RUN test ! -d /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu || ln -s /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu /usr/lib/llvm-18/lib/clang/18/lib/x86_64-unknown-linux-gnu

# Rust toolchain and libraries
ENV RUSTUP_HOME=/rust/rustup
ENV CARGO_HOME=/rust/cargo
ENV PATH="/rust/cargo/bin:${PATH}"
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && \
    chmod 777 -R /rust && \
    rustup toolchain install nightly-2024-11-01 && \
    rustup default nightly-2024-11-01 && \
    rustup toolchain remove stable && \
    rustup component add rust-src && \
    rustup target add x86_64-unknown-linux-gnu && \
    rustup target add aarch64-unknown-linux-gnu

# Give suid to gdb to grant it attach permissions
# chmod 777 to make the container user independent
RUN chmod u+s /opt/gdb/bin/gdb \
  && mkdir -p /var/lib/clickhouse \
  && chmod 777 /var/lib/clickhouse

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV COMMIT_SHA=''
ENV PULL_REQUEST_NUMBER=''
ENV COPY_CLICKHOUSE_BINARY_TO_OUTPUT=0
