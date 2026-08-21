# docker build -t clickhouse/mysql-golang-client .
# MySQL golang client docker container

FROM golang:1.24-alpine

RUN apk --no-cache upgrade

WORKDIR /opt

COPY ./main.go main.go

# Pin to commit 217d05049 (pre-v1.6.0): v1.7.0+ changed MySQLError.Error()
# format to include SQL state ("Error 81 (HY000): ..."), breaking test assertions
# that expect the old format ("Error 81: ..."). Using the exact same commit as
# the original golang:1.17 Dockerfile to guarantee identical driver behavior.
RUN go mod init none \
    && go get github.com/go-sql-driver/mysql@217d05049 \
    && go build main.go
