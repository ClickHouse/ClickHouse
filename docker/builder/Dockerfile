FROM ubuntu:19.10

RUN apt-get --allow-unauthenticated update -y && apt-get install --yes wget gnupg
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb [trusted=yes] http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main" >> /etc/apt/sources.list

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            ccache \
            cmake \
            curl \
            expect \
            g++ \
            gcc \
            ninja-build \
            perl \
            pkg-config \
            python \
            python-lxml \
            python-requests \
            python-termcolor \
            sudo \
            tzdata \
            llvm-10 \
            clang-10 \
            clang-tidy-10 \
            lld-10 \
            lldb-10

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
