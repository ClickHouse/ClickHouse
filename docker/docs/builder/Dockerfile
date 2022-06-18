# rebuild in #36968
# docker build -t clickhouse/docs-builder .
# nodejs 17 prefers ipv6 and is broken in our environment
FROM node:16-alpine

RUN apk add --no-cache git openssh bash

# At this point we want to really update /opt/clickhouse-docs
# despite the cached images
ARG CACHE_INVALIDATOR=0

RUN git clone https://github.com/ClickHouse/clickhouse-docs.git \
    --depth=1 --branch=main /opt/clickhouse-docs

WORKDIR /opt/clickhouse-docs

RUN yarn config set registry https://registry.npmjs.org \
    && yarn install \
    && yarn cache clean

COPY run.sh /run.sh

ENTRYPOINT ["/run.sh"]

CMD ["yarn", "build"]
