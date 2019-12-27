# docker build -t yandex/clickhouse-performance-comparison .
FROM alpine

RUN apk update && apk add --no-cache bash wget python3 python3-dev g++
RUN pip3 --no-cache-dir install clickhouse_driver
RUN apk del g++ python3-dev

COPY * /

