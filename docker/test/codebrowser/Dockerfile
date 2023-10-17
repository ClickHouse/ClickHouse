# rebuild in #33610
# docker build --network=host -t clickhouse/codebrowser .
# docker run --volume=path_to_repo:/repo_folder --volume=path_to_result:/test_output clickhouse/codebrowser
ARG FROM_TAG=latest
FROM clickhouse/binary-builder:$FROM_TAG

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update && apt-get --yes --allow-unauthenticated install libclang-${LLVM_VERSION}-dev libmlir-${LLVM_VERSION}-dev

ARG TARGETARCH
RUN arch=${TARGETARCH:-amd64} \
    && case $arch in \
        amd64) rarch=x86_64 ;; \
        arm64) rarch=aarch64 ;; \
        *) exit 1 ;; \
    esac

# repo versions doesn't work correctly with C++17
# also we push reports to s3, so we add index.html to subfolder urls
# https://github.com/ClickHouse/woboq_codebrowser/commit/37e15eaf377b920acb0b48dbe82471be9203f76b
RUN git clone --branch=master --depth=1 https://github.com/ClickHouse/woboq_codebrowser /woboq_codebrowser \
  && cd /woboq_codebrowser \
  && cmake . -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang\+\+-${LLVM_VERSION} -DCMAKE_C_COMPILER=clang-${LLVM_VERSION} -DCLANG_BUILTIN_HEADERS_DIR=/usr/lib/llvm-${LLVM_VERSION}/lib/clang/${LLVM_VERSION}/include \
  && ninja

COPY build.sh /
CMD ["bash", "-c", "/build.sh 2>&1"]
