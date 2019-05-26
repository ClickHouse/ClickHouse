FROM ubuntu:18.04

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
            tzdata

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
