# docker build -t clickhouse/docs-builder .
FROM golang:alpine AS htmltest-builder

ARG HTMLTEST_VERSION=0.17.0

RUN CGO_ENABLED=0 go install github.com/wjdp/htmltest@v${HTMLTEST_VERSION} \
    && mv "${GOPATH}/bin/htmltest" /usr/bin/htmltest

# we need clickhouse for running of the settings autogeneration script
FROM clickhouse/clickhouse-server AS clickhouse-provider

# nodejs 17 prefers ipv6 and is broken in our environment
FROM node:20-alpine

RUN apk add --no-cache git openssh bash curl libc6-compat

# Copy ClickHouse binary from the clickhouse-provider stage
COPY --from=clickhouse-provider /usr/bin/clickhouse /usr/bin/clickhouse
ENV PATH=$PATH:/usr/bin

# Despite using libc6-compat we are still missing some files
# See https://github.com/ClickHouse/ClickHouse/issues/26350
RUN git clone https://github.com/ClickHouse/libc-blobs.git -b master --depth=1
RUN mv /lib/ld-linux-aarch64.so.1 /lib/ld-linux-aarch64.so.1.bak
RUN cp -r libc-blobs/aarch64/lib/* /lib/

# At this point we want to really update /opt/clickhouse-docs directory
# So we reset the cache
ARG CACHE_INVALIDATOR=0

RUN git clone https://github.com/ClickHouse/clickhouse-docs.git \
    --depth=1 --branch=main /opt/clickhouse-docs

WORKDIR /opt/clickhouse-docs

RUN yarn config set registry https://registry.npmjs.org \
    && yarn install \
    && yarn cache clean

ENV HOME=/opt/clickhouse-docs

RUN mkdir /output_path \
  && chmod -R a+w . /output_path \
  && git config --global --add safe.directory /opt/clickhouse-docs

COPY run.sh /run.sh
COPY --from=htmltest-builder /usr/bin/htmltest /usr/bin/htmltest

ENTRYPOINT ["/run.sh"]
