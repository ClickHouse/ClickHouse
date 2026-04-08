# docker build -t clickhouse/mysql-php-client .
# MySQL PHP client docker container
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM php:8.4-cli-alpine

COPY ./client.crt client.crt
COPY ./client.key client.key
COPY ./test.php test.php
COPY ./test_ssl.php test_ssl.php

RUN docker-php-ext-install pdo pdo_mysql
