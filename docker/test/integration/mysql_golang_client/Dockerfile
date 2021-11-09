# docker build -t clickhouse/mysql-golang-client .
# MySQL golang client docker container

FROM golang:1.13

RUN go get "github.com/go-sql-driver/mysql"

COPY ./main.go main.go

RUN go build main.go
