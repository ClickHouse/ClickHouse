# docker build -t clickhouse/mysql_dotnet_client .
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM ubuntu:22.04

WORKDIR /testapp

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl

ARG ver=42.2.12
COPY testapp.csproj testapp.csproj
RUN dotnet restore

COPY Program.cs Program.cs
