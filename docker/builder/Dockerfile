FROM ubuntu:18.04

RUN apt-get update -y \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:ubuntu-toolchain-r/test \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            ccache \
            cmake \
            curl \
            expect \
            g++-9 \
            gcc-9 \
            libreadline-dev \
            ninja-build \
            perl \
            pkg-config \
            python \
            python-lxml \
            python-requests \
            python-termcolor \
            sudo \
            tzdata \
            gperf

RUN apt install -y wget
RUN printf "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main\ndeb-src http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list \
    && wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && apt update && apt-get install -y clang-8 lldb-8 lld-8

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
