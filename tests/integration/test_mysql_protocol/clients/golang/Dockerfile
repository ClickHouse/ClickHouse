FROM golang:1.12.2

RUN go get "github.com/go-sql-driver/mysql"

COPY ./main.go main.go

RUN go build main.go
