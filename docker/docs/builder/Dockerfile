# rebuild in #36968
# docker build -t clickhouse/docs-builder .
# nodejs 17 prefers ipv6 and is broken in our environment
FROM node:16.14.2-alpine3.15

RUN apk add --no-cache git openssh bash

# TODO: clean before merge!
ARG DOCS_BRANCH=main

RUN git clone https://github.com/ClickHouse/clickhouse-docs.git \
    --depth=1 --branch=${DOCS_BRANCH} /opt/clickhouse-docs

WORKDIR /opt/clickhouse-docs

RUN yarn config set registry https://registry.npmjs.org \
    && yarn install \
    && yarn cache clean

COPY run.sh /run.sh

ENTRYPOINT ["/run.sh"]

CMD ["yarn", "build"]
