FROM ubuntu:17.10

RUN apt-get update -y && \
    apt-get install -y \
        cmake pkg-config gcc-7 g++-7 \
        liblld-5.0-dev libclang-5.0-dev \
        libssl-dev libicu-dev libreadline-dev libmysqlclient-dev unixodbc-dev
        # For tests: bash expect python python-lxml python-termcolor curl perl sudo tzdata

ADD build.sh /
RUN chmod +x /build.sh

CMD ["/build.sh"]
