# docker build -t clickhouse/postgresql-java-client .
# PostgreSQL Java client docker container

FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ARG ver=42.2.12
RUN dotnet add package Npgsql
RUN dotnet add package CommandLineParser

WORKDIR /jdbc
