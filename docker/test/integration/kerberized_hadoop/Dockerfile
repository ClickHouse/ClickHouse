# docker build -t clickhouse/kerberized-hadoop .

FROM prasanthj/docker-hadoop:2.6.0

RUN apt update && \
    apt install -y gcc make

ADD https://archive.apache.org/dist/commons/daemon/source/commons-daemon-1.0.15-src.tar.gz /tmp/commons-daemon-1.0.15-src.tar.gz

RUN cd /tmp && \
    tar xzf commons-daemon-1.0.15-src.tar.gz && \
    cd commons-daemon-1.0.15-src/src/native/unix && \
    ./configure && \
    make && \
    cp ./jsvc /usr/local/hadoop-2.6.0/sbin && \
    cd /tmp && \
    rm -rf commons-daemon-1.0.15-src*
