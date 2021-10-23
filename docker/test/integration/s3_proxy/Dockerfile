# docker build -t yandex/clickhouse-s3-proxy .
FROM nginx:alpine

COPY run.sh /run.sh
COPY server.crt /etc/ssl/certs/server.crt
COPY server.key /etc/ssl/certs/server.key
COPY nginx.conf /etc/nginx/nginx.conf

RUN chmod +x /run.sh

CMD ["/run.sh"]
