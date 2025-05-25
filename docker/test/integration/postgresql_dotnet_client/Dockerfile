# docker build -t clickhouse/postgresql-dotnet-client .
# PostgreSQL C# client docker container

FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ARG ver=42.2.12
COPY Program.cs Program.cs
COPY testapp.csproj testapp.csproj
RUN dotnet add package Npgsql
RUN dotnet add package CommandLineParser

WORKDIR /testapp
