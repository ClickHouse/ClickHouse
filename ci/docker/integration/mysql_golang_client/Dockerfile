# docker build -t clickhouse/mysql-golang-client .
# MySQL golang client docker container

FROM golang:1.17

WORKDIR /opt

COPY ./main.go main.go

RUN go mod init none \
    && go get github.com/go-sql-driver/mysql@217d05049 \
    && go build main.go
