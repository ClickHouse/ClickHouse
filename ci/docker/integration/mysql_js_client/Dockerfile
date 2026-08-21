# docker build -t clickhouse/mysql-js-client .
# MySQL JavaScript client docker container

FROM node:22-alpine

WORKDIR /usr/app

RUN npm install -g npm@11 && npm install mysql

COPY ./test.js ./test.js
