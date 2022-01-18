# docker build -t yandex/clickhouse-mysql-php-client .
# MySQL PHP client docker container

FROM php:7.3-cli

COPY ./client.crt client.crt
COPY ./client.key client.key
COPY ./test.php test.php
COPY ./test_ssl.php test_ssl.php

RUN docker-php-ext-install pdo pdo_mysql
