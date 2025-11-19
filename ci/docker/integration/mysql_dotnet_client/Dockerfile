# docker build -t clickhouse/mysql_dotnet_client .

FROM ubuntu:22.04

WORKDIR /testapp

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl

ARG ver=42.2.12
COPY Program.cs Program.cs
COPY testapp.csproj testapp.csproj
