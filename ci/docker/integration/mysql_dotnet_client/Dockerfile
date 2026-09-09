# docker build -t clickhouse/mysql_dotnet_client .
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential dotnet-sdk-8.0 curl

# MySQL client app
WORKDIR /testapp
COPY testapp.csproj testapp.csproj
RUN dotnet restore
COPY Program.cs Program.cs

# PostgreSQL client app
WORKDIR /pg_testapp
COPY pg/testapp.csproj testapp.csproj
RUN dotnet restore
COPY pg/Program.cs Program.cs
