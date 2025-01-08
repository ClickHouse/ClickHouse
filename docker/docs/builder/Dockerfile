# docker build -t clickhouse/docs-builder .
FROM golang:alpine AS htmltest-builder

ARG HTMLTEST_VERSION=0.17.0

RUN CGO_ENABLED=0 go install github.com/wjdp/htmltest@v${HTMLTEST_VERSION} \
    && mv "${GOPATH}/bin/htmltest" /usr/bin/htmltest

# nodejs 17 prefers ipv6 and is broken in our environment
FROM node:16-alpine

RUN apk add --no-cache git openssh bash

# At this point we want to really update /opt/clickhouse-docs directory
# So we reset the cache
ARG CACHE_INVALIDATOR=0

RUN git clone https://github.com/ClickHouse/clickhouse-docs.git \
    --depth=1 --branch=main /opt/clickhouse-docs

WORKDIR /opt/clickhouse-docs

RUN yarn config set registry https://registry.npmjs.org \
    && yarn install \
    && yarn cache clean

ENV HOME /opt/clickhouse-docs

RUN mkdir /output_path \
  && chmod -R a+w . /output_path \
  && git config --global --add safe.directory /opt/clickhouse-docs

COPY run.sh /run.sh
COPY --from=htmltest-builder /usr/bin/htmltest /usr/bin/htmltest

ENTRYPOINT ["/run.sh"]
