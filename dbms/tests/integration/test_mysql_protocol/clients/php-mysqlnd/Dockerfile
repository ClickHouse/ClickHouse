FROM php:7.3-cli

COPY ./client.crt client.crt
COPY ./client.key client.key
COPY ./test.php test.php

RUN docker-php-ext-install pdo pdo_mysql
