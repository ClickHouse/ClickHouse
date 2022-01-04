#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Not sure why fail even in sequential mode. Disabled for now to make some progress.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

GO_BUILD="${CURDIR}/go_build"
DOCKER_NAME="url_basic_auth_go_webserver"
mkdir -p ${GO_BUILD}
cd ${GO_BUILD}
cp ../02157_url_basic_auth.go app.go
cat <<EOF >> Dockerfile
FROM golang:alpine as builder

RUN apk --no-cache add git

WORKDIR /go/src/webserver

COPY app.go .

RUN go mod init

RUN go mod tidy

RUN CGO_ENABLED=0 go build -a -installsuffix cgo -o app .

FROM alpine:latest as prod

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=0 /go/src/webserver/app .

CMD ["./app"]
EOF

docker build -t go/webserver:1 . >/dev/null
docker run --rm -d --name ${DOCKER_NAME} -p 33339:33339 go/webserver:1 >/dev/null
clickhouse-client --query "select * from url('http://admin1:password@127.0.0.1:33339/example', 'RawBLOB', 'a String')" 2>&1 | grep Exception
clickhouse-client --query "select * from url('http://admin2:password%2F@127.0.0.1:33339/example', 'RawBLOB', 'a String')" 2>&1 | grep Exception
docker stop ${DOCKER_NAME}
rm -rf ${GO_BUILD}
