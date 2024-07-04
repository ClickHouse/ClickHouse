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
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

# This symlink is required by gcc to find the lld linker
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld
# FIXME: workaround for "The imported target "merge-fdata" references the file" error
# https://salsa.debian.org/pkg-llvm-team/llvm-toolchain/-/commit/992e52c0b156a5ba9c6a8a54f8c4857ddd3d371d
RUN sed -i '/_IMPORT_CHECK_FILES_FOR_\(mlir-\|llvm-bolt\|merge-fdata\|MLIR\)/ {s|^|#|}' /usr/lib/llvm-${LLVM_VERSION}/lib/cmake/llvm/LLVMExports-*.cmake

# LLVM changes paths for compiler-rt libraries. For some reason clang-18.1.8 cannot catch up libraries from default install path.
# It's very dirty workaround, better to build compiler and LLVM ourself and use it.
RUN ls /usr/lib/llvm-18/lib/clang/18/lib/aarch64-unknown-linux-gnu 2>/dev/null 1>/dev/null \
    && ln -s /usr/lib/llvm-18/lib/clang/18/lib/aarch64-unknown-linux-gnu /usr/lib/llvm-18/lib/clang/18/lib/linux 2>/dev/null \
    && cd /usr/lib/llvm-18/lib/clang/18/lib/linux && rename 's/.a.syms$/-aarch64.a.syms/' *.a.syms && rename 's/.a$/-aarch64.a/' *.a && rename 's/.so$/-aarch64.so/' *.so  && rename 's/.o$/-aarch64.o/' *.o ||:

RUN ls /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu 2>/dev/null 1>/dev/null \
    && ln -s /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu /usr/lib/llvm-18/lib/clang/18/lib/linux 2>/dev/null \
    && cd /usr/lib/llvm-18/lib/clang/18/lib/linux && rename 's/.a.syms$/-x86_64.a.syms/' *.a.syms && rename 's/.a$/-x86_64.a/' *.a && rename 's/.so$/-x86_64.so/' *.so  && rename 's/.o$/-x86_64.o/' *.o ||:

ARG CCACHE_VERSION=4.6.1
RUN mkdir /tmp/ccache \
    && cd /tmp/ccache \
    && curl -L \
        -O https://github.com/ccache/ccache/releases/download/v$CCACHE_VERSION/ccache-$CCACHE_VERSION.tar.xz \
        -O https://github.com/ccache/ccache/releases/download/v$CCACHE_VERSION/ccache-$CCACHE_VERSION.tar.xz.asc \
    && gpg --recv-keys --keyserver hkps://keyserver.ubuntu.com 5A939A71A46792CF57866A51996DDA075594ADB8 \
    && gpg --verify ccache-4.6.1.tar.xz.asc \
    && tar xf ccache-$CCACHE_VERSION.tar.xz \
    && cd /tmp/ccache/ccache-$CCACHE_VERSION \
    && cmake -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=None \
        -DZSTD_FROM_INTERNET=ON \
        -DREDIS_STORAGE_BACKEND=OFF \
        -Wno-dev \
        -B build \
        -S . \
    && make VERBOSE=1 -C build \
    && make install -C build \
    && cd / \
    && rm -rf /tmp/ccache

ARG TARGETARCH
ARG SCCACHE_VERSION=v0.7.7
ENV SCCACHE_IGNORE_SERVER_IO_ERROR=1
# sccache requires a value for the region. So by default we use The Default Region
ENV SCCACHE_REGION=us-east-1
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
    amd64) rarch=x86_64 ;; \
    arm64) rarch=aarch64 ;; \
  esac \
  && curl -Ls "https://github.com/mozilla/sccache/releases/download/$SCCACHE_VERSION/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl.tar.gz" | \
    tar xz -C /tmp \
  && mv "/tmp/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl/sccache" /usr/bin \
  && rm "/tmp/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl" -r

# Give suid to gdb to grant it attach permissions
# chmod 777 to make the container user independent
RUN chmod u+s /usr/bin/gdb \
  && mkdir -p /var/lib/clickhouse \
  && chmod 777 /var/lib/clickhouse

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV COMMIT_SHA=''
ENV PULL_REQUEST_NUMBER=''
ENV COPY_CLICKHOUSE_BINARY_TO_OUTPUT=0

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
