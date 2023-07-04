# docker build -t clickhouse/integration-helper .
# Helper docker container to run iptables without sudo

FROM alpine
RUN apk add --no-cache -U iproute2 \
  && for bin in iptables iptables-restore iptables-save; \
    do ln -sf xtables-nft-multi "/sbin/$bin"; \
  done
