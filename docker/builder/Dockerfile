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
            libmysqlclient-dev \
            libreadline-dev \
            libssl-dev \
            ninja-build \
            perl \
            pkg-config \
            python \
            python-lxml \
            python-requests \
            python-termcolor \
            sudo \
            tzdata \
            unixodbc-dev

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
