# docker build -t clickhouse/integration-helper .
# Helper docker container to run iptables without sudo

FROM alpine:3.18
RUN apk add --no-cache -U iproute2 \
  && for bin in \
  iptables iptables-restore iptables-save \
  ip6tables ip6tables-restore ip6tables-save; \
    do ln -sf xtables-nft-multi "/sbin/$bin"; \
  done
