FROM ubuntu:18.04

RUN apt-get update -y && \
    env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        cmake ninja-build ccache pkg-config gcc g++ \
        liblld-6.0-dev libclang-6.0-dev \
        libssl-dev libicu-dev libreadline-dev libmysqlclient-dev unixodbc-dev \
        bash expect python python-lxml python-termcolor python-requests curl perl sudo tzdata

ADD build.sh /
RUN chmod +x /build.sh

CMD ["/build.sh"]
