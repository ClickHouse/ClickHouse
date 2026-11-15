# docker build .
# docker run -it --rm --network=host 14f23e59669c dotnet /client/out/clickhouse.test.dll --host localhost --port 8123 --user default --database default
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM mcr.microsoft.com/dotnet/sdk:8.0

WORKDIR /client
COPY *.cs *.csproj /client/

ARG VERSION=4.1.0
RUN dotnet add package ClickHouse.Client -v ${VERSION}

# Build here, not in the test. `dotnet run` restores, builds and runs, which starts an MSBuild node
# and a Roslyn compiler server alongside the application: three .NET processes where the test needs
# one, and the two extra ones are not what this test covers. They are already a known source of
# noise in this suite - the analyzer crash that
# https://github.com/ClickHouse/ClickHouse/pull/107375 had to work around came from the in-test
# build. Publishing here leaves the test with a single `dotnet <dll>` process.
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 MSBUILDDISABLENODEREUSE=1
RUN dotnet publish -c Release -o /client/out && rm -rf /client/obj /client/bin
