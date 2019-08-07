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
            libclang-6.0-dev \
            libicu-dev \
            liblld-6.0-dev \
            libreadline-dev \
            gperf \
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

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
